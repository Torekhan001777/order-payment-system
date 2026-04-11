package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	_ "github.com/lib/pq"
)

type Payment struct {
	ID            string    `json:"id"`
	OrderID       string    `json:"order_id"`
	TransactionID string    `json:"transaction_id"`
	Amount        int64     `json:"amount"`
	Status        string    `json:"status"`
	CreatedAt     time.Time `json:"created_at"`
}

var db *sql.DB

func main() {
	dbHost := getEnv("DB_HOST", "localhost")
	dbUser := getEnv("DB_USER", "postgres")
	dbPassword := getEnv("DB_PASSWORD", "secret123")
	dbName := getEnv("DB_NAME", "payments")
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

	http.HandleFunc("/payments", paymentsHandler)
	http.HandleFunc("/payments/", paymentHandler)

	port := getEnv("PORT", "8081")
	log.Printf("Payment Service running on port %s", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

func paymentsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		processPayment(w, r)
	} else {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func paymentHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		orderID := r.URL.Path[len("/payments/"):]
		getPayment(w, r, orderID)
	} else {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func processPayment(w http.ResponseWriter, r *http.Request) {
	var req struct {
		OrderID string `json:"order_id"`
		Amount  int64  `json:"amount"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var existingPayment Payment
	query := `SELECT id, order_id, transaction_id, amount, status, created_at FROM payments WHERE order_id = $1`
	err := db.QueryRow(query, req.OrderID).Scan(
		&existingPayment.ID, &existingPayment.OrderID, &existingPayment.TransactionID,
		&existingPayment.Amount, &existingPayment.Status, &existingPayment.CreatedAt,
	)

	if err == nil {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(existingPayment)
		return
	}

	status := "Authorized"
	if req.Amount > 100000 {
		status = "Declined"
	}

	payment := Payment{
		ID:            fmt.Sprintf("%d", time.Now().UnixNano()),
		OrderID:       req.OrderID,
		TransactionID: fmt.Sprintf("txn_%d", time.Now().UnixNano()),
		Amount:        req.Amount,
		Status:        status,
		CreatedAt:     time.Now(),
	}

	insertQuery := `INSERT INTO payments (id, order_id, transaction_id, amount, status, created_at) 
	                VALUES ($1, $2, $3, $4, $5, $6)`
	_, err = db.Exec(insertQuery, payment.ID, payment.OrderID, payment.TransactionID,
		payment.Amount, payment.Status, payment.CreatedAt)
	if err != nil {
		http.Error(w, "Failed to process payment", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(payment)
}

func getPayment(w http.ResponseWriter, _ *http.Request, orderID string) {
	var payment Payment
	query := `SELECT id, order_id, transaction_id, amount, status, created_at FROM payments WHERE order_id = $1`
	err := db.QueryRow(query, orderID).Scan(
		&payment.ID, &payment.OrderID, &payment.TransactionID,
		&payment.Amount, &payment.Status, &payment.CreatedAt,
	)

	if err == sql.ErrNoRows {
		http.Error(w, "Payment not found", http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(payment)
}

func getEnv(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}
