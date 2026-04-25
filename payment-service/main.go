package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"time"

	paymentv1 "payment-service/api/payment/v1"

	_ "github.com/lib/pq"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Payment struct {
	ID            string    `json:"id"`
	OrderID       string    `json:"order_id"`
	TransactionID string    `json:"transaction_id"`
	Amount        int64     `json:"amount"`
	Status        string    `json:"status"`
	CreatedAt     time.Time `json:"created_at"`
}

type paymentGrpcServer struct {
	paymentv1.UnimplementedPaymentServiceServer
	db *sql.DB
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

	go startPaymentGrpcServer(db)

	http.HandleFunc("/payments", paymentsHandler)
	http.HandleFunc("/payments/", paymentHandler)

	port := getEnv("PORT", "8081")
	log.Printf("Payment Service REST running on port %s", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

func startPaymentGrpcServer(db *sql.DB) {
	grpcPort := getEnv("PAYMENT_GRPC_PORT", "9091")
	lis, err := net.Listen("tcp", ":"+grpcPort)
	if err != nil {
		log.Fatalf("failed to listen payment grpc: %v", err)
	}

	grpcServer := grpc.NewServer(
		grpc.UnaryInterceptor(loggingInterceptor),
	)
	paymentv1.RegisterPaymentServiceServer(grpcServer, &paymentGrpcServer{db: db})

	log.Printf("Payment Service gRPC running on port %s", grpcPort)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve payment grpc: %v", err)
	}
}

func loggingInterceptor(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	started := time.Now()
	resp, err := handler(ctx, req)
	log.Printf("grpc method=%s duration=%s err=%v", info.FullMethod, time.Since(started), err)
	return resp, err
}

func (s *paymentGrpcServer) ProcessPayment(ctx context.Context, req *paymentv1.PaymentRequest) (*paymentv1.PaymentResponse, error) {
	if req.GetOrderId() == "" {
		return nil, status.Error(codes.InvalidArgument, "order_id is required")
	}
	if req.GetAmount() <= 0 {
		return nil, status.Error(codes.InvalidArgument, "amount must be greater than 0")
	}

	var existing Payment
	query := `SELECT id, order_id, transaction_id, amount, status, created_at FROM payments WHERE order_id = $1`
	err := s.db.QueryRowContext(ctx, query, req.GetOrderId()).Scan(
		&existing.ID, &existing.OrderID, &existing.TransactionID, &existing.Amount, &existing.Status, &existing.CreatedAt,
	)
	if err == nil {
		return &paymentv1.PaymentResponse{
			TransactionId: existing.TransactionID,
			Status:        existing.Status,
			ProcessedAt:   timestamppb.New(existing.CreatedAt),
		}, nil
	}
	if err != nil && err != sql.ErrNoRows {
		return nil, status.Error(codes.Internal, "failed to read payment")
	}

	paymentStatus := "Authorized"
	if req.GetAmount() > 100000 {
		paymentStatus = "Declined"
	}

	payment := Payment{
		ID:            fmt.Sprintf("%d", time.Now().UnixNano()),
		OrderID:       req.GetOrderId(),
		TransactionID: fmt.Sprintf("txn_%d", time.Now().UnixNano()),
		Amount:        req.GetAmount(),
		Status:        paymentStatus,
		CreatedAt:     time.Now(),
	}

	insertQuery := `INSERT INTO payments (id, order_id, transaction_id, amount, status, created_at)
                    VALUES ($1, $2, $3, $4, $5, $6)`
	_, err = s.db.ExecContext(ctx, insertQuery, payment.ID, payment.OrderID, payment.TransactionID,
		payment.Amount, payment.Status, payment.CreatedAt)
	if err != nil {
		return nil, status.Error(codes.Internal, "failed to process payment")
	}

	return &paymentv1.PaymentResponse{
		TransactionId: payment.TransactionID,
		Status:        payment.Status,
		ProcessedAt:   timestamppb.New(payment.CreatedAt),
	}, nil
}

func paymentsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		processPaymentREST(w, r)
	} else {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func paymentHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		orderID := r.URL.Path[len("/payments/"):]
		getPaymentREST(w, r, orderID)
	} else {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func processPaymentREST(w http.ResponseWriter, r *http.Request) {
	var req struct {
		OrderID string `json:"order_id"`
		Amount  int64  `json:"amount"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	resp, err := (&paymentGrpcServer{db: db}).ProcessPayment(r.Context(), &paymentv1.PaymentRequest{
		OrderId: req.OrderID,
		Amount:  req.Amount,
	})
	if err != nil {
		st, _ := status.FromError(err)
		http.Error(w, st.Message(), http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"transaction_id": resp.GetTransactionId(),
		"status":         resp.GetStatus(),
		"processed_at":   resp.GetProcessedAt().AsTime(),
	})
}

func getPaymentREST(w http.ResponseWriter, _ *http.Request, orderID string) {
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
	_ = json.NewEncoder(w).Encode(payment)
}

func getEnv(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}
