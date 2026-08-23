package main

import (
	"context"
	"log"
	"time"

	"paxos-2pc-kvstore/internal/logx"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// callerKey is the context key under which an authenticated caller's identity
// is stored. Using an unexported type prevents collisions with other packages
// that also write to the context.
type callerKey struct{}

// callerFrom returns the identity established by authUnaryInterceptor.
func callerFrom(ctx context.Context) (identity, bool) {
	id, ok := ctx.Value(callerKey{}).(identity)
	return id, ok
}

// publicMethods are reachable without a token: two of them are how a caller
// obtains one, and the third is how a bank announces itself at start-up.
var publicMethods = map[string]bool{
	"/payment.PaymentGateway/Register":     true,
	"/payment.PaymentGateway/Authenticate": true,
	"/payment.PaymentGateway/BankRegister": true,
}

// loggingUnaryInterceptor records the method, outcome and duration of every
// call. It runs outermost so that rejected calls are logged too.
func loggingUnaryInterceptor(
	ctx context.Context,
	req any,
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (any, error) {
	start := time.Now()
	resp, err := handler(ctx, req)

	code := status.Code(err)
	color := logx.Green
	if err != nil {
		color = logx.Red
	}
	log.Printf(color+"%-46s %-16s %s"+logx.Reset, info.FullMethod, code, time.Since(start).Round(time.Millisecond))
	return resp, err
}

// authUnaryInterceptor enforces that non-public methods carry a valid session
// token, and attaches the caller's identity to the context so handlers can
// perform ownership checks without re-reading metadata.
func (s *gatewayService) authUnaryInterceptor(
	ctx context.Context,
	req any,
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (any, error) {
	if err := s.health.check(); err != nil {
		return nil, err
	}
	if publicMethods[info.FullMethod] {
		return handler(ctx, req)
	}

	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "request carries no metadata")
	}
	tokens := md.Get("authorization")
	if len(tokens) == 0 {
		return nil, status.Error(codes.Unauthenticated, "missing authorization token")
	}
	caller, ok := s.auth.lookupSession(tokens[0])
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "token is invalid or has expired")
	}

	return handler(context.WithValue(ctx, callerKey{}, caller), req)
}

// requireOwnership checks that the authenticated caller owns accountID. This
// is what stops an authenticated user from moving money out of, or reading
// the balance of, somebody else's account.
func requireOwnership(ctx context.Context, accountID string) (identity, error) {
	caller, ok := callerFrom(ctx)
	if !ok {
		return identity{}, status.Error(codes.Unauthenticated, "no authenticated caller")
	}
	if caller.AccountID == "" {
		return identity{}, status.Error(codes.FailedPrecondition,
			"this account was registered by an older client; register again to link an account id")
	}
	if caller.AccountID != accountID {
		return identity{}, status.Errorf(codes.PermissionDenied,
			"user %s is not the owner of account %s", caller.Username, accountID)
	}
	return caller, nil
}
