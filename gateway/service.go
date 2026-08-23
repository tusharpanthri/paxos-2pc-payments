package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"

	"paxos-2pc-kvstore/internal/logx"
	pb "paxos-2pc-kvstore/proto"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// gatewayService implements the PaymentGateway RPCs. It owns authentication
// and the bank registry, and delegates the actual movement of money to the
// coordinator.
type gatewayService struct {
	pb.UnimplementedPaymentGatewayServer

	auth        *authStore
	registry    *bankRegistry
	coordinator *coordinator
	health      *gatewayHealth
}

func newGatewayService(auth *authStore, registry *bankRegistry, h *gatewayHealth) *gatewayService {
	return &gatewayService{
		auth:        auth,
		registry:    registry,
		coordinator: &coordinator{registry: registry},
		health:      h,
	}
}

// BankRegister records a bank's address so the gateway can route to it.
func (s *gatewayService) BankRegister(ctx context.Context, req *pb.BankRegisterRequest) (*pb.BankRegisterResponse, error) {
	if req.BankName == "" || req.BankAddress == "" {
		return nil, status.Error(codes.InvalidArgument, "bank name and address are both required")
	}
	if err := s.registry.register(req.BankName, req.BankAddress); err != nil {
		return nil, status.Errorf(codes.Internal, "register bank %s: %v", req.BankName, err)
	}
	log.Printf(logx.Green+"[registry] bank %s is reachable at %s"+logx.Reset, req.BankName, req.BankAddress)
	return &pb.BankRegisterResponse{Success: true, Message: "bank registered"}, nil
}

// Register creates or updates a gateway login and links it to a bank account.
func (s *gatewayService) Register(ctx context.Context, req *pb.RegisterRequest) (*pb.RegisterResponse, error) {
	if req.Username == "" || req.Password == "" || req.AccountId == "" {
		return nil, status.Error(codes.InvalidArgument, "username, password and account id are all required")
	}
	if err := s.auth.register(req.Username, req.Password, req.AccountId, req.BankName); err != nil {
		if errors.Is(err, errAccountClaimed) {
			log.Printf(logx.Yellow+"[auth] %s tried to claim account %s, which belongs to someone else"+logx.Reset,
				req.Username, req.AccountId)
			return nil, status.Errorf(codes.PermissionDenied, "account %s is already registered to another user", req.AccountId)
		}
		return nil, status.Errorf(codes.Internal, "register %s: %v", req.Username, err)
	}
	log.Printf(logx.Green+"[auth] registered %s for account %s at %s"+logx.Reset, req.Username, req.AccountId, req.BankName)
	return &pb.RegisterResponse{Success: true, Message: "registration successful"}, nil
}

// Authenticate exchanges a username and password for a session token.
func (s *gatewayService) Authenticate(ctx context.Context, req *pb.AuthRequest) (*pb.AuthResponse, error) {
	token, err := s.auth.authenticate(req.Username, req.Password)
	if errors.Is(err, errBadCredentials) {
		// The message is deliberately vague: telling a caller which half was
		// wrong hands them a way to enumerate valid usernames.
		log.Printf(logx.Yellow+"[auth] failed login for %q"+logx.Reset, req.Username)
		return nil, status.Error(codes.Unauthenticated, "invalid username or password")
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "authenticate %s: %v", req.Username, err)
	}
	log.Printf(logx.Green+"[auth] issued a token to %s"+logx.Reset, req.Username)
	return &pb.AuthResponse{Token: token, Message: "authentication successful"}, nil
}

// TransferMoney runs a two-phase commit across the sender's and receiver's
// banks. The caller must own the account being debited.
func (s *gatewayService) TransferMoney(ctx context.Context, req *pb.TransferRequest) (*pb.TransferResponse, error) {
	if _, err := requireOwnership(ctx, req.FromAccount); err != nil {
		return nil, err
	}
	if req.TransactionId == "" {
		return nil, status.Error(codes.InvalidArgument, "a transaction id is required for idempotent retries")
	}
	if req.Amount <= 0 {
		return nil, status.Errorf(codes.InvalidArgument, "amount must be positive, got %.2f", req.Amount)
	}

	log.Printf(logx.Cyan+"[txn %s] %s@%s -> %s@%s %.2f"+logx.Reset,
		req.TransactionId, req.FromAccount, req.FromBank, req.ToAccount, req.ToBank, req.Amount)

	if err := s.coordinator.transfer(ctx, req); err != nil {
		return nil, err
	}
	return &pb.TransferResponse{Success: true, Message: "transfer completed"}, nil
}

// CheckBalance reads a balance from the owning bank.
func (s *gatewayService) CheckBalance(ctx context.Context, req *pb.BalanceRequest) (*pb.BalanceResponse, error) {
	if _, err := requireOwnership(ctx, req.AccountId); err != nil {
		return nil, err
	}
	bank, err := s.registry.client(req.BankName)
	if err != nil {
		return nil, err
	}
	return bank.GetBalance(ctx, req)
}

// gatewayHealth is the operator-controlled up/down switch, mirroring the one
// on the bank servers so an outage can be demonstrated at either tier.
type gatewayHealth struct {
	mu     sync.RWMutex
	active bool
}

func newGatewayHealth() *gatewayHealth { return &gatewayHealth{active: true} }

func (h *gatewayHealth) check() error {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if !h.active {
		return status.Error(codes.Unavailable, "gateway is offline")
	}
	return nil
}

func (h *gatewayHealth) set(active bool) {
	h.mu.Lock()
	h.active = active
	h.mu.Unlock()
}

// watchConsole toggles the simulated outage from stdin.
func (h *gatewayHealth) watchConsole() {
	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print(logx.Blue + "gateway> type 'down' or 'up': " + logx.Reset)
		if !scanner.Scan() {
			return
		}
		switch strings.ToLower(strings.TrimSpace(scanner.Text())) {
		case "down":
			h.set(false)
			log.Printf(logx.Yellow + "[health] gateway is now DOWN (simulated)" + logx.Reset)
		case "up":
			h.set(true)
			log.Printf(logx.Green + "[health] gateway is now UP" + logx.Reset)
		case "":
			// ignore stray newlines
		default:
			log.Printf(logx.Red + "[health] unknown command; use 'down' or 'up'" + logx.Reset)
		}
	}
}
