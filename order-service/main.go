package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"time"

	orderv1 "order-service/api/order/v1"
	paymentv1 "order-service/api/payment/v1"

	"github.com/gin-gonic/gin"
	_ "github.com/lib/pq"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Order struct {
	ID         string    `json:"id"`
	CustomerID string    `json:"customer_id"`
	ItemName   string    `json:"item_name"`
	Amount     int64     `json:"amount"`
	Status     string    `json:"status"`
	CreatedAt  time.Time `json:"created_at"`
}

type orderGrpcServer struct {
	orderv1.UnimplementedOrderServiceServer
	db *sql.DB
}

var (
	db            *sql.DB
	paymentClient paymentv1.PaymentServiceClient
)

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
		log.Fatal("failed to connect to database:", err)
	}
	defer db.Close()

	for i := 0; i < 15; i++ {
		if err = db.Ping(); err == nil {
			log.Println("connected to database successfully")
			break
		}
		log.Println("waiting for database...")
		time.Sleep(2 * time.Second)
	}
	if err != nil {
		log.Fatal("database unreachable:", err)
	}

	paymentAddr := getEnv("PAYMENT_GRPC_ADDR", "payment-service:9091")
	paymentConn, err := grpc.Dial(paymentAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal("failed to connect payment grpc:", err)
	}
	defer paymentConn.Close()
	paymentClient = paymentv1.NewPaymentServiceClient(paymentConn)

	go startOrderGrpcServer(db)

	r := gin.Default()
	r.POST("/orders", createOrder)
	r.GET("/orders", listOrders)
	r.GET("/orders/:id", getOrder)
	r.PATCH("/orders/:id", cancelOrder)

	port := getEnv("PORT", "8080")
	log.Printf("Order Service REST running on port %s", port)
	log.Fatal(r.Run(":" + port))
}

func startOrderGrpcServer(db *sql.DB) {
	grpcPort := getEnv("ORDER_GRPC_PORT", "9090")
	lis, err := net.Listen("tcp", ":"+grpcPort)
	if err != nil {
		log.Fatalf("failed to listen order grpc: %v", err)
	}

	s := grpc.NewServer()
	orderv1.RegisterOrderServiceServer(s, &orderGrpcServer{db: db})
	log.Printf("Order Service gRPC running on port %s", grpcPort)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve order grpc: %v", err)
	}
}

func (s *orderGrpcServer) SubscribeToOrderUpdates(req *orderv1.OrderRequest, stream orderv1.OrderService_SubscribeToOrderUpdatesServer) error {
	if req.GetOrderId() == "" {
		return status.Error(codes.InvalidArgument, "order_id is required")
	}

	var lastStatus string
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-stream.Context().Done():
			return nil
		case <-ticker.C:
			var currentStatus string
			query := `SELECT status FROM orders WHERE id = $1`
			err := s.db.QueryRow(query, req.GetOrderId()).Scan(&currentStatus)
			if err == sql.ErrNoRows {
				return status.Error(codes.NotFound, "order not found")
			}
			if err != nil {
				return status.Error(codes.Internal, err.Error())
			}

			if currentStatus != lastStatus {
				lastStatus = currentStatus
				msg := &orderv1.OrderStatusUpdate{
					OrderId:   req.GetOrderId(),
					Status:    currentStatus,
					UpdatedAt: timestamppb.Now(),
				}
				if err := stream.Send(msg); err != nil {
					return err
				}
			}
		}
	}
}

func createOrder(c *gin.Context) {
	var req struct {
		CustomerID string `json:"customer_id"`
		ItemName   string `json:"item_name"`
		Amount     int64  `json:"amount"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if req.Amount <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "amount must be greater than 0"})
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
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to create order"})
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 4*time.Second)
	defer cancel()

	paymentResp, err := paymentClient.ProcessPayment(ctx, &paymentv1.PaymentRequest{
		OrderId: order.ID,
		Amount:  order.Amount,
	})
	if err != nil {
		order.Status = "Failed"
		_, _ = db.Exec("UPDATE orders SET status = $1 WHERE id = $2", order.Status, order.ID)
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "payment service unavailable"})
		return
	}

	if paymentResp.GetStatus() == "Authorized" {
		order.Status = "Paid"
	} else {
		order.Status = "Failed"
	}

	_, _ = db.Exec("UPDATE orders SET status = $1 WHERE id = $2", order.Status, order.ID)
	c.JSON(http.StatusCreated, order)
}

func getOrder(c *gin.Context) {
	id := c.Param("id")

	var order Order
	query := `SELECT id, customer_id, item_name, amount, status, created_at FROM orders WHERE id = $1`
	err := db.QueryRow(query, id).Scan(&order.ID, &order.CustomerID, &order.ItemName, &order.Amount, &order.Status, &order.CreatedAt)

	if err == sql.ErrNoRows {
		c.JSON(http.StatusNotFound, gin.H{"error": "order not found"})
		return
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, order)
}

func listOrders(c *gin.Context) {
	query := `SELECT id, customer_id, item_name, amount, status, created_at FROM orders ORDER BY created_at DESC`
	rows, err := db.Query(query)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	defer rows.Close()

	var orders []Order
	for rows.Next() {
		var order Order
		if err := rows.Scan(&order.ID, &order.CustomerID, &order.ItemName, &order.Amount, &order.Status, &order.CreatedAt); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		orders = append(orders, order)
	}

	c.JSON(http.StatusOK, orders)
}

func cancelOrder(c *gin.Context) {
	id := c.Param("id")

	var statusValue string
	query := `SELECT status FROM orders WHERE id = $1`
	err := db.QueryRow(query, id).Scan(&statusValue)
	if err == sql.ErrNoRows {
		c.JSON(http.StatusNotFound, gin.H{"error": "order not found"})
		return
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	if statusValue == "Paid" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "cannot cancel paid order"})
		return
	}

	if statusValue != "Pending" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "only pending orders can be cancelled"})
		return
	}

	_, err = db.Exec("UPDATE orders SET status = 'Cancelled' WHERE id = $1", id)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "cancelled"})
}

func getEnv(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}
