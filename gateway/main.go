package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	pb "payment_gateway/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

var (
	port = flag.Int("port", 50051, "The gateway server port")
)

// Global gateway status.
var gatewayActive = true
var gatewayMutex sync.RWMutex

func isGatewayActive() bool {
	gatewayMutex.RLock()
	defer gatewayMutex.RUnlock()
	return gatewayActive
}

func setGatewayActive(status bool) {
	gatewayMutex.Lock()
	defer gatewayMutex.Unlock()
	gatewayActive = status
}

func monitorGatewayStatus() {
	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("Gateway: Enter command (down/up): ")
		if scanner.Scan() {
			cmd := strings.ToLower(strings.TrimSpace(scanner.Text()))
			if cmd == "down" {
				setGatewayActive(false)
				log.Printf("Gateway: Now offline.")
			} else if cmd == "up" {
				setGatewayActive(true)
				log.Printf("Gateway: Now online.")
			} else {
				log.Printf("Gateway: Unknown command. Use 'down' or 'up'.")
			}
		}
	}
}

// Global bank registry.
var bankRegistry = make(map[string]string)
var bankRegistryMutex sync.RWMutex

func getBankAddress(bankName string) (string, bool) {
	bankRegistryMutex.RLock()
	addr, ok := bankRegistry[bankName]
	bankRegistryMutex.RUnlock()
	return addr, ok
}

func (s *server) BankRegister(ctx context.Context, req *pb.BankRegisterRequest) (*pb.BankRegisterResponse, error) {
	bankRegistryMutex.Lock()
	bankRegistry[req.BankName] = req.BankAddress
	bankRegistryMutex.Unlock()
	log.Printf("Gateway: Bank '%s' registered at address %s", req.BankName, req.BankAddress)
	return &pb.BankRegisterResponse{Success: true, Message: "Bank registration successful"}, nil
}

type server struct {
	pb.UnimplementedPaymentGatewayServer
}

func PrintRegisteredUsers() {
	userStore.RLock()
	defer userStore.RUnlock()
	log.Println("Gateway: Currently registered users:")
	for username, user := range userStore.users {
		log.Printf("   Username: %s, Password: %s", username, user.Password)
	}
}

func (s *server) Register(ctx context.Context, req *pb.RegisterRequest) (*pb.RegisterResponse, error) {
	log.Printf("Gateway: Register RPC called with username: %s", req.Username)
	RegisterUser(req.Username, req.Password)
	err := AppendOrUpdateGatewayUser(req.Username, req.Password)
	if err != nil {
		msg := fmt.Sprintf("Failed to update gateway users file: %v", err)
		log.Printf("Gateway: Registration failed: %s", msg)
		return &pb.RegisterResponse{Success: false, Message: msg}, nil
	}
	log.Printf("Gateway: User %s registered successfully.", req.Username)
	return &pb.RegisterResponse{Success: true, Message: "Registration successful"}, nil
}

func (s *server) Authenticate(ctx context.Context, req *pb.AuthRequest) (*pb.AuthResponse, error) {
	log.Printf("Gateway: Authenticate RPC called for username: %s", req.Username)
	valid := ValidateUser(req.Username, req.Password)
	if !valid {
		msg := "Invalid credentials or user not registered"
		log.Printf("Gateway: Authentication failed for username: %s, message: %s", req.Username, msg)
		PrintRegisteredUsers()
		return &pb.AuthResponse{Token: "", Message: msg}, nil
	}
	token := fmt.Sprintf("token-%d", time.Now().UnixNano())
	AddToken(token, req.Username)
	log.Printf("Gateway: User %s authenticated successfully, token issued: %s", req.Username, token)
	return &pb.AuthResponse{Token: token, Message: "Authentication successful"}, nil
}

func (s *server) TransferMoney(ctx context.Context, req *pb.TransferRequest) (*pb.TransferResponse, error) {
	log.Printf("Gateway: TransferMoney RPC called for txn: %s", req.TransactionId)
	if !isGatewayActive() {
		return &pb.TransferResponse{Success: false, Message: "Gateway is offline"}, nil
	}
	senderAddr, ok := getBankAddress(req.FromBank)
	if !ok {
		msg := fmt.Sprintf("Sender bank '%s' is not registered.", req.FromBank)
		log.Printf("Gateway: %s", msg)
		return &pb.TransferResponse{Success: false, Message: msg}, nil
	}
	receiverAddr, ok := getBankAddress(req.ToBank)
	if !ok {
		msg := fmt.Sprintf("Receiver bank '%s' is not registered.", req.ToBank)
		log.Printf("Gateway: %s", msg)
		return &pb.TransferResponse{Success: false, Message: msg}, nil
	}
	senderConn, err := grpc.Dial(senderAddr, grpc.WithInsecure())
	if err != nil {
		log.Printf("Gateway: Failed to connect to sender bank at %s: %v", senderAddr, err)
		return &pb.TransferResponse{Success: false, Message: "Failed to connect to sender bank."}, nil
	}
	defer senderConn.Close()
	senderClient := pb.NewBankClient(senderConn)
	receiverConn, err := grpc.Dial(receiverAddr, grpc.WithInsecure())
	if err != nil {
		log.Printf("Gateway: Failed to connect to receiver bank at %s: %v", receiverAddr, err)
		return &pb.TransferResponse{Success: false, Message: "Failed to connect to receiver bank."}, nil
	}
	defer receiverConn.Close()
	receiverClient := pb.NewBankClient(receiverConn)

	// Create composite keys.
	debitKey := req.TransactionId + "-debit"
	creditKey := req.TransactionId + "-credit"

	prepDebitReq := &pb.DebitCreditRequest{
		AccountId:           req.FromAccount,
		Amount:              req.Amount,
		TransactionId:       debitKey,
		CounterpartyAccount: req.ToAccount,
	}
	debitPrepResp, err := senderClient.PrepareDebit(ctx, prepDebitReq)
	if err != nil || !debitPrepResp.Success {
		msg := fmt.Sprintf("Debit preparation failed: %v, %s", err, debitPrepResp.Message)
		log.Printf("Gateway: %s", msg)
		return &pb.TransferResponse{Success: false, Message: msg}, nil
	}
	log.Printf("Gateway: Debit preparation succeeded for txn %s", debitKey)

	prepCreditReq := &pb.DebitCreditRequest{
		AccountId:           req.ToAccount,
		Amount:              req.Amount,
		TransactionId:       creditKey,
		CounterpartyAccount: req.FromAccount,
	}
	creditPrepResp, err := receiverClient.PrepareCredit(ctx, prepCreditReq)
	if err != nil || !creditPrepResp.Success {
		msg := fmt.Sprintf("Credit preparation failed: %v, %s", err, creditPrepResp.Message)
		log.Printf("Gateway: %s", msg)
		_, _ = senderClient.AbortDebit(ctx, prepDebitReq)
		return &pb.TransferResponse{Success: false, Message: msg}, nil
	}
	log.Printf("Gateway: Credit preparation succeeded for txn %s", creditKey)

	// COMMIT PHASE:
	commitDebitResp, err := senderClient.CommitDebit(ctx, prepDebitReq)
	if err != nil || !commitDebitResp.Success {
		lowerMsg := strings.ToLower(commitDebitResp.Message)
		if err != nil || strings.Contains(lowerMsg, "offline") || strings.Contains(lowerMsg, "down") {
			msg := fmt.Sprintf("Debit commit failed due to sender bank down: %v, %s", err, commitDebitResp.Message)
			log.Printf("Gateway: %s", msg)
			return &pb.TransferResponse{Success: false, Message: msg}, nil
		} else if strings.Contains(lowerMsg, "not found") || strings.Contains(lowerMsg, "invalid credentials") {
			msg := fmt.Sprintf("Debit commit failed due to authentication error: %v, %s", err, commitDebitResp.Message)
			log.Printf("Gateway: %s", msg)
			return &pb.TransferResponse{Success: false, Message: msg}, nil
		} else {
			msg := fmt.Sprintf("Debit commit failed: %v, %s", err, commitDebitResp.Message)
			log.Printf("Gateway: %s", msg)
			_, _ = receiverClient.AbortCredit(ctx, prepCreditReq)
			return &pb.TransferResponse{Success: false, Message: msg}, nil
		}
	}
	log.Printf("Gateway: Debit commit succeeded for txn %s", debitKey)

	commitCreditResp, err := receiverClient.CommitCredit(ctx, prepCreditReq)
	if err != nil || !commitCreditResp.Success {
		lowerMsg := strings.ToLower(commitCreditResp.Message)
		if err != nil || strings.Contains(lowerMsg, "offline") || strings.Contains(lowerMsg, "down") {
			msg := fmt.Sprintf("Credit commit failed due to receiver bank down: %v, %s", err, commitCreditResp.Message)
			log.Printf("Gateway: %s", msg)
			// Attempt to revert debit.
			abortResp, abortErr := senderClient.AbortDebit(ctx, prepDebitReq)
			if abortErr != nil || !abortResp.Success {
				log.Printf("Gateway: Critical: Unable to revert debit after credit failure: %v, %s", abortErr, abortResp.Message)
			}
			return &pb.TransferResponse{Success: false, Message: msg}, nil
		} else if strings.Contains(lowerMsg, "not found") || strings.Contains(lowerMsg, "invalid credentials") {
			msg := fmt.Sprintf("Credit commit failed due to authentication error: %v, %s", err, commitCreditResp.Message)
			log.Printf("Gateway: %s", msg)
			_, _ = senderClient.AbortDebit(ctx, prepDebitReq)
			return &pb.TransferResponse{Success: false, Message: msg}, nil
		} else {
			msg := fmt.Sprintf("Credit commit failed: %v, %s", err, commitCreditResp.Message)
			log.Printf("Gateway: %s", msg)
			_, _ = senderClient.AbortDebit(ctx, prepDebitReq)
			return &pb.TransferResponse{Success: false, Message: msg}, nil
		}
	}
	log.Printf("Gateway: Credit commit succeeded for txn %s", creditKey)
	log.Printf("Gateway: Transaction %s processed successfully", req.TransactionId)
	return &pb.TransferResponse{Success: true, Message: "Transaction processed successfully"}, nil
}

func (s *server) CheckBalance(ctx context.Context, req *pb.BalanceRequest) (*pb.BalanceResponse, error) {
	log.Printf("Gateway: CheckBalance called for account: %s, bank: %s", req.AccountId, req.BankName)
	bankAddr, ok := getBankAddress(req.BankName)
	if !ok {
		msg := fmt.Sprintf("Bank '%s' is not registered.", req.BankName)
		log.Printf("Gateway: %s", msg)
		return &pb.BalanceResponse{Balance: 0, Message: msg}, nil
	}
	bankConn, err := grpc.Dial(bankAddr, grpc.WithInsecure())
	if err != nil {
		log.Printf("Gateway: Failed to connect to bank at %s: %v", bankAddr, err)
		return &pb.BalanceResponse{Balance: 0, Message: "Failed to connect to bank."}, nil
	}
	defer bankConn.Close()
	bankClient := pb.NewBankClient(bankConn)
	balResp, err := bankClient.GetBalance(ctx, req)
	if err != nil {
		log.Printf("Gateway: Bank GetBalance error: %v", err)
		return &pb.BalanceResponse{Balance: 0, Message: "Bank error"}, nil
	}
	log.Printf("Gateway: Balance for account %s: %.2f", req.AccountId, balResp.Balance)
	return balResp, nil
}

func main() {
	flag.Parse()
	LoadGatewayUsers()

	cert, err := tls.LoadX509KeyPair("../certs/gateway.pem", "../certs/gateway.key")
	if err != nil {
		log.Fatalf("Gateway: failed to load key pair: %s", err)
	}
	caCert, err := ioutil.ReadFile("../certs/ca.pem")
	if err != nil {
		log.Fatalf("Gateway: failed to read CA certificate: %s", err)
	}
	caCertPool := x509.NewCertPool()
	if ok := caCertPool.AppendCertsFromPEM(caCert); !ok {
		log.Fatalf("Gateway: failed to append CA certs")
	}
	creds := credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientCAs:    caCertPool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
	})

	go monitorGatewayStatus()

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("Gateway: failed to listen: %v", err)
	}
	grpcServer := grpc.NewServer(
		grpc.Creds(creds),
		grpc.UnaryInterceptor(AuthInterceptor),
	)
	pb.RegisterPaymentGatewayServer(grpcServer, &server{})
	log.Printf("Gateway: Server listening on port %d", *port)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Gateway: failed to serve: %s", err)
	}
}
