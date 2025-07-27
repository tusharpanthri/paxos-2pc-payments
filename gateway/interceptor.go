package main

import (
	"context"
	"fmt"
	"log"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

const (
	ColorReset   = "\033[0m"
	ColorMagenta = "\033[35m"
	ColorYellow  = "\033[33m"
)

func AuthInterceptor(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	if !isGatewayActive() {
		return nil, fmt.Errorf("Gateway is offline")
	}
	log.Printf(ColorMagenta+"----- Start Request: %s -----"+ColorReset, info.FullMethod)
	if info.FullMethod == "/payment.PaymentGateway/Register" ||
		info.FullMethod == "/payment.PaymentGateway/Authenticate" ||
		info.FullMethod == "/payment.PaymentGateway/BankRegister" {
		log.Printf(ColorYellow+"Interceptor: Skipping auth for method: %s"+ColorReset, info.FullMethod)
		resp, err := handler(ctx, req)
		log.Printf(ColorMagenta+"----- End Request: %s -----"+ColorReset, info.FullMethod)
		return resp, err
	}
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, fmt.Errorf("Missing metadata")
	}
	tokens := md["authorization"]
	if len(tokens) == 0 {
		return nil, fmt.Errorf("Missing authorization token")
	}
	if !ValidateToken(tokens[0]) {
		return nil, fmt.Errorf("Invalid token")
	}
	log.Printf(ColorYellow+"Interceptor: Token validated for method: %s"+ColorReset, info.FullMethod)
	resp, err := handler(ctx, req)
	log.Printf(ColorMagenta+"----- End Request: %s -----"+ColorReset, info.FullMethod)
	return resp, err
}
