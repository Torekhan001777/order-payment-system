//go:build ignore
// +build ignore

package grpc

import (
	"context"
	"payment-service/internal/usecase"

	pb "github.com/Torekhan001777/order-payment-generated/payment/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type PaymentServer struct {
	pb.UnimplementedPaymentServiceServer
	useCase *usecase.PaymentUseCase
}

func NewPaymentServer(uc *usecase.PaymentUseCase) *PaymentServer {
	return &PaymentServer{useCase: uc}
}

func (s *PaymentServer) ProcessPayment(ctx context.Context, req *pb.PaymentRequest) (*pb.PaymentResponse, error) {
	payment, err := s.useCase.ProcessPayment(req.OrderId, req.Amount)
	if err != nil {
		return nil, err
	}
	return &pb.PaymentResponse{
		TransactionId: payment.TransactionID,
		Status:        payment.Status,
		ProcessedAt:   timestamppb.New(payment.CreatedAt),
	}, nil
}
