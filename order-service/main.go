package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	_ "github.com/lib/pq"
)

type Order struct {
	ID         string    `json:"id"`
	CustomerID string    `json:"customer_id"`
	ItemName   string    `json:"item_name"`
	Amount     int64     `json:"amount"`
	Status     string    `json:"status"`
	CreatedAt  time.Time `json:"created_at"`
}

var db *sql.DB

func main() {
	dbHost := getEnv("DB_HOST", "localhost")
	dbUser := getEnv("DB_USER", "postgres")
	dbPassword := getEnv("DB_PASSWORD", "secret123")
	dbName := getEnv("DB_NAME", "orders")
	dbPort := getEnv("DB_PORT", "5432")

	connStr := fmt.Sprintf("host=%s user=%s password=%s dbname=%s port=%s sslmode=disable",
		dbHost, dbUser, dbPassword, dbName, dbPort)

	var err error
	db, err = sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal("Failed to connect to database:", err)
	}
	defer db.Close()

	for i := 0; i < 10; i++ {
		if err = db.Ping(); err == nil {
			log.Println("Connected to database successfully")
			break
		}
		log.Println("Waiting for database...")
		time.Sleep(2 * time.Second)
	}
	if err != nil {
		log.Fatal("Database unreachable:", err)
	}

	http.HandleFunc("/orders", ordersHandler)
	http.HandleFunc("/orders/", orderHandler)

	port := getEnv("PORT", "8080")
	log.Printf("Order Service running on port %s", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

func ordersHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		createOrder(w, r)
	case http.MethodGet:
		listOrders(w, r)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func orderHandler(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Path[len("/orders/"):]

	switch r.Method {
	case http.MethodGet:
		getOrder(w, r, id)
	case http.MethodPatch:
		cancelOrder(w, r, id)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func createOrder(w http.ResponseWriter, r *http.Request) {
	var req struct {
		CustomerID string `json:"customer_id"`
		ItemName   string `json:"item_name"`
		Amount     int64  `json:"amount"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if req.Amount <= 0 {
		http.Error(w, "amount must be greater than 0", http.StatusBadRequest)
		return
	}

	order := Order{
		ID:         fmt.Sprintf("%d", time.Now().UnixNano()),
		CustomerID: req.CustomerID,
		ItemName:   req.ItemName,
		Amount:     req.Amount,
		Status:     "Pending",
		CreatedAt:  time.Now(),
	}

	query := `INSERT INTO orders (id, customer_id, item_name, amount, status, created_at) 
	          VALUES ($1, $2, $3, $4, $5, $6)`
	_, err := db.Exec(query, order.ID, order.CustomerID, order.ItemName, order.Amount, order.Status, order.CreatedAt)
	if err != nil {
		http.Error(w, "Failed to create order", http.StatusInternalServerError)
		return
	}

	paymentURL := getEnv("PAYMENT_SERVICE_URL", "http://localhost:8081")
	paymentReq := map[string]interface{}{
		"order_id": order.ID,
		"amount":   order.Amount,
	}
	jsonData, _ := json.Marshal(paymentReq)

	resp, err := http.Post(paymentURL+"/payments", "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		order.Status = "Failed"
		db.Exec("UPDATE orders SET status = $1 WHERE id = $2", order.Status, order.ID)
		http.Error(w, "Payment service unavailable", http.StatusServiceUnavailable)
		return
	}
	defer resp.Body.Close()

	var paymentResp struct {
		Status string `json:"status"`
	}
	json.NewDecoder(resp.Body).Decode(&paymentResp)

	if paymentResp.Status == "Authorized" {
		order.Status = "Paid"
	} else {
		order.Status = "Failed"
	}

	db.Exec("UPDATE orders SET status = $1 WHERE id = $2", order.Status, order.ID)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(order)
}

func getOrder(w http.ResponseWriter, _ *http.Request, id string) {
	var order Order
	query := `SELECT id, customer_id, item_name, amount, status, created_at FROM orders WHERE id = $1`
	err := db.QueryRow(query, id).Scan(&order.ID, &order.CustomerID, &order.ItemName, &order.Amount, &order.Status, &order.CreatedAt)

	if err == sql.ErrNoRows {
		http.Error(w, "Order not found", http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(order)
}

func listOrders(w http.ResponseWriter, _ *http.Request) {
	query := `SELECT id, customer_id, item_name, amount, status, created_at FROM orders ORDER BY created_at DESC`
	rows, err := db.Query(query)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var orders []Order
	for rows.Next() {
		var order Order
		rows.Scan(&order.ID, &order.CustomerID, &order.ItemName, &order.Amount, &order.Status, &order.CreatedAt)
		orders = append(orders, order)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(orders)
}

func cancelOrder(w http.ResponseWriter, _ *http.Request, id string) {
	var status string
	query := `SELECT status FROM orders WHERE id = $1`
	err := db.QueryRow(query, id).Scan(&status)
	if err == sql.ErrNoRows {
		http.Error(w, "Order not found", http.StatusNotFound)
		return
	}

	if status == "Paid" {
		http.Error(w, "Cannot cancel paid order", http.StatusBadRequest)
		return
	}

	if status != "Pending" {
		http.Error(w, "Only pending orders can be cancelled", http.StatusBadRequest)
		return
	}

	db.Exec("UPDATE orders SET status = 'Cancelled' WHERE id = $1", id)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "cancelled"})
}

func getEnv(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}
