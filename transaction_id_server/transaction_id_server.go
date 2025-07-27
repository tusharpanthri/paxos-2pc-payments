package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"

	pb "payment_gateway/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"
)

const (
	port        = ":50055"                     // Transaction ID Server runs on port 50055
	counterFile = "../transaction_counter.txt" // File to persist counter
)

var (
	transactionCounter int64 = 1
	counterMutex       sync.Mutex
)

// loadCounter reads the last used transaction counter from file
func loadCounter() {
	counterMutex.Lock()
	defer counterMutex.Unlock()

	// Check if counter file exists
	if _, err := os.Stat(counterFile); os.IsNotExist(err) {
		// File doesn't exist, create it with initial value
		err := ioutil.WriteFile(counterFile, []byte("1"), 0644)
		if err != nil {
			log.Printf("[TransactionID Server] WARNING: Failed to create counter file: %v", err)
		}
		log.Printf("[TransactionID Server] Created new counter file with initial value 1")
		transactionCounter = 1
		return
	}

	// Read counter from file
	data, err := ioutil.ReadFile(counterFile)
	if err != nil {
		log.Printf("[TransactionID Server] WARNING: Failed to read counter file: %v", err)
		return
	}

	// Parse the counter value
	value, err := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
	if err != nil {
		log.Printf("[TransactionID Server] WARNING: Invalid counter value in file: %v", err)
		return
	}

	transactionCounter = value
	log.Printf("[TransactionID Server] Loaded counter value: %d", transactionCounter)
}

// saveCounter writes the current transaction counter to file
func saveCounter() error {
	counterMutex.Lock()
	defer counterMutex.Unlock()

	// Write counter to file
	err := ioutil.WriteFile(counterFile, []byte(fmt.Sprintf("%d", transactionCounter)), 0644)
	if err != nil {
		log.Printf("[TransactionID Server] ERROR: Failed to save counter: %v", err)
		return err
	}
	return nil
}

// Embed the generated unimplemented server to satisfy the interface.
type server struct {
	pb.UnimplementedTransactionIDServiceServer
}

// GetNewTransactionID generates and returns a unique transaction ID.
func (s *server) GetNewTransactionID(ctx context.Context, req *pb.TransactionIDRequest) (*pb.TransactionIDResponse, error) {
	counterMutex.Lock()
	transactionID := fmt.Sprintf("TXN-%06d", transactionCounter)
	transactionCounter++
	counterMutex.Unlock()

	// Save the updated counter value
	if err := saveCounter(); err != nil {
		log.Printf("[TransactionID Server] WARNING: Failed to persist counter after generating ID %s", transactionID)
	}

	p, ok := peer.FromContext(ctx)
	clientAddress := "UNKNOWN"
	if ok {
		clientAddress = p.Addr.String()
	}

	log.Printf("[TransactionID Server] Assigned ID: %s to client: %s", transactionID, clientAddress)
	return &pb.TransactionIDResponse{TransactionId: transactionID}, nil
}

func main() {
	// Load the transaction counter from file
	loadCounter()

	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("[TransactionID Server] Failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterTransactionIDServiceServer(grpcServer, &server{})

	log.Printf("[TransactionID Server] Listening on %s (starting with transaction counter: %d)", port, transactionCounter)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("[TransactionID Server] Failed to serve: %v", err)
	}
}
