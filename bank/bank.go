package main

import (
	"bufio"
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"crypto/tls"
	"crypto/x509"

	pb "payment_gateway/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

const (
	ColorReset  = "\033[0m"
	ColorRed    = "\033[0m\033[31m"
	ColorGreen  = "\033[0m\033[32m"
	ColorYellow = "\033[0m\033[33m"
	ColorBlue   = "\033[0m\033[34m"
	ColorCyan   = "\033[0m\033[36m"
)

var serverActive = true
var serverStatusMutex sync.RWMutex

func checkServerActive() error {
	serverStatusMutex.RLock()
	defer serverStatusMutex.RUnlock()
	if !serverActive {
		return fmt.Errorf("Bank is offline")
	}
	return nil
}

var fileLock sync.Mutex

var transactionsLog = struct {
	sync.RWMutex
	log map[string]bool
}{log: make(map[string]bool)}

func loadTransactionsLog(bankName string) {
	filename := fmt.Sprintf("../%s_transactions.txt", bankName)
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		f, err := os.Create(filename)
		if err != nil {
			log.Printf(ColorRed+"Bank: Could not create transactions file: %v"+ColorReset, err)
			return
		}
		f.Close()
		log.Printf(ColorYellow+"Bank: Created new empty transactions file for bank '%s'."+ColorReset, bankName)
		return
	}
	file, err := os.Open(filename)
	if err != nil {
		log.Printf(ColorRed+"Bank: Could not open transactions file: %v"+ColorReset, err)
		return
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	transactionsLog.Lock()
	defer transactionsLog.Unlock()
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line != "" {
			parts := strings.Split(line, ",")
			if len(parts) > 0 {
				transactionsLog.log[parts[0]] = true
			}
		}
	}
}

func appendTransaction(bankName, txnID, senderAccount, receiverAccount, operation string, amount float64) error {
	filename := fmt.Sprintf("../%s_transactions.txt", bankName)
	f, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	line := fmt.Sprintf("%s,%s,%s,%.2f,%s\n", txnID, senderAccount, receiverAccount, amount, operation)
	_, err = f.WriteString(line)
	if err != nil {
		return err
	}
	transactionsLog.Lock()
	transactionsLog.log[txnID] = true
	transactionsLog.Unlock()
	return nil
}

func isTransactionProcessed(txnID string) bool {
	transactionsLog.RLock()
	defer transactionsLog.RUnlock()
	return transactionsLog.log[txnID]
}

func ReadBankUsers(bankName string) ([][]string, error) {
	filename := fmt.Sprintf("../%s_users.txt", bankName)
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	reader := csv.NewReader(file)
	return reader.ReadAll()
}

func WriteBankUsers(bankName string, records [][]string) error {
	filename := fmt.Sprintf("../%s_users.txt", bankName)
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()
	writer := csv.NewWriter(file)
	err = writer.WriteAll(records)
	if err != nil {
		return err
	}
	writer.Flush()
	return nil
}

// preparedTransaction holds details of a prepared transaction.
type preparedTransaction struct {
	operation    string // "debit" or "credit"
	accountId    string
	counterparty string
	amount       float64
}

var preparedMutex sync.RWMutex
var preparedTransactions = make(map[string]preparedTransaction)

type bankServer struct {
	pb.UnimplementedBankServer
}

func newBankServer() *bankServer {
	return &bankServer{}
}

// ProcessTransaction remains unchanged.
func (b *bankServer) ProcessTransaction(ctx context.Context, req *pb.TransactionRequest) (*pb.TransactionResponse, error) {
	msg := "Use DebitAccount/CreditAccount or 2PC RPCs for transactions."
	log.Printf(ColorYellow + msg + ColorReset)
	return &pb.TransactionResponse{Success: false, Message: msg}, nil
}

// -------- 2PC Methods for Debit --------

// PrepareDebit: Reserve funds with composite key.
func (b *bankServer) PrepareDebit(ctx context.Context, req *pb.DebitCreditRequest) (*pb.DebitCreditResponse, error) {
	key := req.TransactionId + "-debit"
	log.Printf(ColorBlue+"[PrepareDebit] Received for key %s (Account: %s, Counterparty: %s, Amount: %.2f)"+ColorReset, key, req.AccountId, req.CounterpartyAccount, req.Amount)
	if err := checkServerActive(); err != nil {
		return &pb.DebitCreditResponse{Success: false, Message: err.Error()}, nil
	}
	if req.AccountId == req.CounterpartyAccount {
		msg := "Self-transfer not allowed."
		log.Printf(ColorRed+"[PrepareDebit] %s"+ColorReset, msg)
		return &pb.DebitCreditResponse{Success: false, Message: msg}, nil
	}
	if isTransactionProcessed(key) {
		log.Printf(ColorCyan+"[PrepareDebit] Duplicate txn %s detected; already processed."+ColorReset, key)
		return &pb.DebitCreditResponse{Success: true, Message: "Transaction already processed."}, nil
	}
	fileLock.Lock()
	defer fileLock.Unlock()
	bName := os.Getenv("BANK_NAME")
	records, err := ReadBankUsers(bName)
	if err != nil {
		msg := fmt.Sprintf("Failed to read bank file: %v", err)
		log.Printf(ColorRed+"[PrepareDebit] %s"+ColorReset, msg)
		return &pb.DebitCreditResponse{Success: false, Message: msg}, nil
	}
	found := false
	var currentBalance float64
	for i, record := range records {
		if i == 0 {
			continue
		}
		if record[0] == req.AccountId {
			found = true
			currentBalance, _ = strconv.ParseFloat(record[4], 64)
			break
		}
	}
	if !found {
		msg := "Sender account not found in prepare phase."
		log.Printf(ColorRed+"[PrepareDebit] %s"+ColorReset, msg)
		return &pb.DebitCreditResponse{Success: false, Message: msg}, nil
	}
	if currentBalance < req.Amount {
		msg := "Insufficient funds during prepare."
		log.Printf(ColorRed+"[PrepareDebit] %s"+ColorReset, msg)
		return &pb.DebitCreditResponse{Success: false, Message: msg}, nil
	}
	preparedMutex.Lock()
	preparedTransactions[key] = preparedTransaction{
		operation:    "debit",
		accountId:    req.AccountId,
		counterparty: req.CounterpartyAccount,
		amount:       req.Amount,
	}
	preparedMutex.Unlock()
	log.Printf(ColorGreen+"[PrepareDebit] Prepared txn %s successfully."+ColorReset, key)
	return &pb.DebitCreditResponse{Success: true, Message: "Debit prepared successfully"}, nil
}

// CommitDebit: Deduct funds.
func (b *bankServer) CommitDebit(ctx context.Context, req *pb.DebitCreditRequest) (*pb.DebitCreditResponse, error) {
	key := req.TransactionId + "-debit"
	log.Printf(ColorBlue+"[CommitDebit] Received for key %s"+ColorReset, key)
	preparedMutex.Lock()
	prep, ok := preparedTransactions[key]
	if !ok || prep.operation != "debit" ||
		prep.accountId != req.AccountId ||
		prep.counterparty != req.CounterpartyAccount ||
		prep.amount != req.Amount {
		preparedMutex.Unlock()
		msg := "No matching prepared debit found."
		log.Printf(ColorRed+"[CommitDebit] %s"+ColorReset, msg)
		return &pb.DebitCreditResponse{Success: false, Message: msg}, nil
	}
	delete(preparedTransactions, key)
	preparedMutex.Unlock()

	fileLock.Lock()
	defer fileLock.Unlock()
	bName := os.Getenv("BANK_NAME")
	records, err := ReadBankUsers(bName)
	if err != nil {
		msg := fmt.Sprintf("Failed to read bank file: %v", err)
		log.Printf(ColorRed+"[CommitDebit] %s"+ColorReset, msg)
		return &pb.DebitCreditResponse{Success: false, Message: msg}, nil
	}
	updated := false
	for i, record := range records {
		if i == 0 {
			continue
		}
		if record[0] == req.AccountId {
			balance, _ := strconv.ParseFloat(record[4], 64)
			balance -= req.Amount
			records[i][4] = fmt.Sprintf("%.2f", balance)
			updated = true
			break
		}
	}
	if !updated {
		msg := "Sender account not found during commit."
		log.Printf(ColorRed+"[CommitDebit] %s"+ColorReset, msg)
		return &pb.DebitCreditResponse{Success: false, Message: msg}, nil
	}
	err = WriteBankUsers(bName, records)
	if err != nil {
		msg := fmt.Sprintf("Failed to update bank file: %v", err)
		log.Printf(ColorRed+"[CommitDebit] %s"+ColorReset, msg)
		return &pb.DebitCreditResponse{Success: false, Message: msg}, nil
	}
	if err := appendTransaction(bName, key, req.AccountId, req.CounterpartyAccount, "debit", req.Amount); err != nil {
		log.Printf(ColorRed+"[CommitDebit] Warning: %v"+ColorReset, err)
	}
	log.Printf(ColorGreen+"[CommitDebit] Debit committed for txn %s"+ColorReset, key)
	return &pb.DebitCreditResponse{Success: true, Message: "Debit committed successfully"}, nil
}

// AbortDebit: Cancel prepared debit.
func (b *bankServer) AbortDebit(ctx context.Context, req *pb.DebitCreditRequest) (*pb.DebitCreditResponse, error) {
	key := req.TransactionId + "-debit"
	log.Printf(ColorBlue+"[AbortDebit] Received for key %s"+ColorReset, key)
	preparedMutex.Lock()
	_, ok := preparedTransactions[key]
	if ok {
		delete(preparedTransactions, key)
	}
	preparedMutex.Unlock()
	log.Printf(ColorYellow+"[AbortDebit] Debit aborted for txn %s"+ColorReset, key)
	return &pb.DebitCreditResponse{Success: true, Message: "Debit aborted"}, nil
}

// PrepareCredit: Reserve credit.
func (b *bankServer) PrepareCredit(ctx context.Context, req *pb.DebitCreditRequest) (*pb.DebitCreditResponse, error) {
	key := req.TransactionId + "-credit"
	log.Printf(ColorBlue+"[PrepareCredit] Received for key %s"+ColorReset, key)
	if err := checkServerActive(); err != nil {
		return &pb.DebitCreditResponse{Success: false, Message: err.Error()}, nil
	}
	if req.AccountId == req.CounterpartyAccount {
		msg := "Self-transfer not allowed."
		log.Printf(ColorRed+"[PrepareCredit] %s"+ColorReset, msg)
		return &pb.DebitCreditResponse{Success: false, Message: msg}, nil
	}
	if isTransactionProcessed(key) {
		log.Printf(ColorCyan+"[PrepareCredit] Duplicate txn %s detected; already processed."+ColorReset, key)
		return &pb.DebitCreditResponse{Success: true, Message: "Transaction already processed."}, nil
	}
	fileLock.Lock()
	defer fileLock.Unlock()
	bName := os.Getenv("BANK_NAME")
	records, err := ReadBankUsers(bName)
	if err != nil {
		msg := fmt.Sprintf("Failed to read bank file: %v", err)
		log.Printf(ColorRed+"[PrepareCredit] %s"+ColorReset, msg)
		return &pb.DebitCreditResponse{Success: false, Message: msg}, nil
	}
	found := false
	for i, record := range records {
		if i == 0 {
			continue
		}
		if record[0] == req.AccountId {
			found = true
			break
		}
	}
	if !found {
		msg := "Receiver account not found during prepare."
		log.Printf(ColorRed+"[PrepareCredit] %s"+ColorReset, msg)
		return &pb.DebitCreditResponse{Success: false, Message: msg}, nil
	}
	preparedMutex.Lock()
	preparedTransactions[key] = preparedTransaction{
		operation:    "credit",
		accountId:    req.AccountId,
		counterparty: req.CounterpartyAccount,
		amount:       req.Amount,
	}
	preparedMutex.Unlock()
	log.Printf(ColorGreen+"[PrepareCredit] Credit prepared for txn %s"+ColorReset, key)
	return &pb.DebitCreditResponse{Success: true, Message: "Credit prepared successfully"}, nil
}

// CommitCredit: Add funds.
func (b *bankServer) CommitCredit(ctx context.Context, req *pb.DebitCreditRequest) (*pb.DebitCreditResponse, error) {
	key := req.TransactionId + "-credit"
	log.Printf(ColorBlue+"[CommitCredit] Received for key %s"+ColorReset, key)
	preparedMutex.Lock()
	prep, ok := preparedTransactions[key]
	if !ok || prep.operation != "credit" ||
		prep.accountId != req.AccountId ||
		prep.counterparty != req.CounterpartyAccount ||
		prep.amount != req.Amount {
		preparedMutex.Unlock()
		msg := "No matching prepared credit found."
		log.Printf(ColorRed+"[CommitCredit] %s"+ColorReset, msg)
		return &pb.DebitCreditResponse{Success: false, Message: msg}, nil
	}
	delete(preparedTransactions, key)
	preparedMutex.Unlock()

	fileLock.Lock()
	defer fileLock.Unlock()
	bName := os.Getenv("BANK_NAME")
	records, err := ReadBankUsers(bName)
	if err != nil {
		msg := fmt.Sprintf("Failed to read bank file: %v", err)
		log.Printf(ColorRed+"[CommitCredit] %s"+ColorReset, msg)
		return &pb.DebitCreditResponse{Success: false, Message: msg}, nil
	}
	updated := false
	for i, record := range records {
		if i == 0 {
			continue
		}
		if record[0] == req.AccountId {
			balance, _ := strconv.ParseFloat(record[4], 64)
			balance += req.Amount
			records[i][4] = fmt.Sprintf("%.2f", balance)
			updated = true
			break
		}
	}
	if !updated {
		msg := "Receiver account not found during commit."
		log.Printf(ColorRed+"[CommitCredit] %s"+ColorReset, msg)
		return &pb.DebitCreditResponse{Success: false, Message: msg}, nil
	}
	err = WriteBankUsers(bName, records)
	if err != nil {
		msg := fmt.Sprintf("Failed to update bank file: %v", err)
		log.Printf(ColorRed+"[CommitCredit] %s"+ColorReset, msg)
		return &pb.DebitCreditResponse{Success: false, Message: msg}, nil
	}
	if err := appendTransaction(bName, key, req.CounterpartyAccount, req.AccountId, "credit", req.Amount); err != nil {
		log.Printf(ColorRed+"[CommitCredit] Warning: %v"+ColorReset, err)
	}
	log.Printf(ColorGreen+"[CommitCredit] Credit committed for txn %s"+ColorReset, key)
	return &pb.DebitCreditResponse{Success: true, Message: "Credit committed successfully"}, nil
}

// AbortCredit: Cancel prepared credit.
func (b *bankServer) AbortCredit(ctx context.Context, req *pb.DebitCreditRequest) (*pb.DebitCreditResponse, error) {
	key := req.TransactionId + "-credit"
	log.Printf(ColorBlue+"[AbortCredit] Received for key %s"+ColorReset, key)
	preparedMutex.Lock()
	_, ok := preparedTransactions[key]
	if ok {
		delete(preparedTransactions, key)
	}
	preparedMutex.Unlock()
	log.Printf(ColorYellow+"[AbortCredit] Credit aborted for txn %s"+ColorReset, key)
	return &pb.DebitCreditResponse{Success: true, Message: "Credit aborted"}, nil
}

func (b *bankServer) GetBalance(ctx context.Context, req *pb.BalanceRequest) (*pb.BalanceResponse, error) {
	if err := checkServerActive(); err != nil {
		return &pb.BalanceResponse{Balance: 0, Message: err.Error()}, nil
	}
	log.Printf(ColorBlue+"[GetBalance] Called for account: %s in bank: %s"+ColorReset, req.AccountId, req.BankName)
	records, err := ReadBankUsers(req.BankName)
	if err != nil {
		msg := fmt.Sprintf("Could not read bank file: %v", err)
		log.Printf(ColorRed+"[GetBalance] %s"+ColorReset, msg)
		return &pb.BalanceResponse{Balance: 0, Message: msg}, nil
	}
	for _, record := range records {
		if record[0] == req.AccountId {
			balance, _ := strconv.ParseFloat(record[4], 64)
			msg := fmt.Sprintf("Balance for account %s: %.2f", req.AccountId, balance)
			log.Printf(ColorGreen+"[GetBalance] %s"+ColorReset, msg)
			return &pb.BalanceResponse{Balance: balance, Message: msg}, nil
		}
	}
	msg := "Account not found"
	log.Printf(ColorRed+"[GetBalance] %s"+ColorReset, msg)
	return &pb.BalanceResponse{Balance: 0, Message: msg}, nil
}

func monitorServerStatus() {
	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print(ColorBlue + "Enter command (down/up): " + ColorReset)
		if scanner.Scan() {
			cmd := strings.ToLower(strings.TrimSpace(scanner.Text()))
			if cmd == "down" {
				serverStatusMutex.Lock()
				serverActive = false
				serverStatusMutex.Unlock()
				log.Printf(ColorYellow + "[Monitor] Bank server now DOWN (simulated)" + ColorReset)
			} else if cmd == "up" {
				serverStatusMutex.Lock()
				serverActive = true
				serverStatusMutex.Unlock()
				log.Printf(ColorGreen + "[Monitor] Bank server now UP (simulated)" + ColorReset)
			} else {
				log.Printf(ColorRed + "[Monitor] Unknown command. Use 'down' or 'up'." + ColorReset)
			}
		}
	}
}

func promptInput(prompt string) string {
	fmt.Print(prompt)
	reader := bufio.NewReader(os.Stdin)
	text, _ := reader.ReadString('\n')
	return strings.TrimSpace(text)
}

func registerBank(bankName string, port int) {
	cert, err := tls.LoadX509KeyPair("../certs/client.pem", "../certs/client.key")
	if err != nil {
		log.Printf("Bank: failed to load TLS credentials: %v", err)
		return
	}
	caCert, err := os.ReadFile("../certs/ca.pem")
	if err != nil {
		log.Printf("Bank: failed to read CA certificate: %v", err)
		return
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)
	creds := credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      caCertPool,
	})
	conn, err := grpc.Dial("localhost:50051", grpc.WithTransportCredentials(creds))
	if err != nil {
		log.Printf("Bank: failed to dial Payment Gateway: %v", err)
		return
	}
	defer conn.Close()
	pgClient := pb.NewPaymentGatewayClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req := &pb.BankRegisterRequest{
		BankName:    bankName,
		BankAddress: fmt.Sprintf("localhost:%d", port),
	}
	resp, err := pgClient.BankRegister(ctx, req)
	if err != nil {
		log.Printf("Bank: Registration with Payment Gateway failed: %v", err)
		return
	}
	log.Printf("Bank: Registration successful: %s", resp.Message)
}

func main() {
	bankNameFlag := flag.String("bank", "DefaultBank", "Name of the bank")
	port := flag.Int("port", 50052, "Bank server port")
	flag.Parse()

	bankName := *bankNameFlag
	log.Printf(ColorCyan+"[Startup] Bank Server '%s' starting..."+ColorReset, bankName)
	os.Setenv("BANK_NAME", bankName)

	loadTransactionsLog(bankName)

	filename := fmt.Sprintf("../%s_users.txt", bankName)
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		f, err := os.Create(filename)
		if err != nil {
			log.Fatalf(ColorRed+"Bank: Failed to create users file for bank '%s': %v"+ColorReset, bankName, err)
		}
		defer f.Close()
		f.WriteString("AccountId,username,password,bank_name,balance\n")
		log.Printf(ColorYellow+"Bank: Created new users file for bank '%s'."+ColorReset, bankName)
	} else {
		records, err := ReadBankUsers(bankName)
		if err != nil {
			log.Printf(ColorRed+"Bank: Error reading users for bank '%s': %v"+ColorReset, bankName, err)
		} else if len(records) <= 1 {
			log.Printf(ColorYellow+"Bank: No registered users found for bank '%s'."+ColorReset, bankName)
		} else {
			log.Printf(ColorGreen+"Bank: Registered users for bank '%s':"+ColorReset, bankName)
			for i, record := range records {
				if i == 0 {
					log.Printf(ColorBlue+"%s"+ColorReset, strings.Join(record, " | "))
				} else {
					log.Printf(ColorCyan+"%s"+ColorReset, strings.Join(record, " | "))
				}
			}
		}
	}

	go monitorServerStatus()
	registerBank(bankName, *port)

	log.Printf(ColorGreen+"Bank: Server for '%s' listening on port %d"+ColorReset, bankName, *port)
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf(ColorRed+"Bank: Failed to listen: %v"+ColorReset, err)
	}
	grpcServer := grpc.NewServer()
	bankSrv := newBankServer()
	pb.RegisterBankServer(grpcServer, bankSrv)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf(ColorRed+"Bank: Failed to serve: %s"+ColorReset, err)
	}
}
