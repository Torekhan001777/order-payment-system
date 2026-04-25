package main

import (
	"context"
	"fmt"
	"log"
	"os"

	orderv1 "order-service/api/order/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	if len(os.Args) < 2 {
		log.Fatal("usage: go run ./cmd/stream-client <order_id> [grpc_addr]")
	}

	orderID := os.Args[1]
	addr := "localhost:9090"
	if len(os.Args) >= 3 {
		addr = os.Args[2]
	}

	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	client := orderv1.NewOrderServiceClient(conn)
	stream, err := client.SubscribeToOrderUpdates(context.Background(), &orderv1.OrderRequest{OrderId: orderID})
	if err != nil {
		log.Fatalf("subscribe failed: %v", err)
	}

	fmt.Printf("subscribed to order_id=%s\n", orderID)
	for {
		msg, err := stream.Recv()
		if err != nil {
			log.Fatalf("stream ended: %v", err)
		}
		fmt.Printf("update: order_id=%s status=%s at=%s\n", msg.GetOrderId(), msg.GetStatus(), msg.GetUpdatedAt().AsTime().Format("2006-01-02 15:04:05"))
	}
}
