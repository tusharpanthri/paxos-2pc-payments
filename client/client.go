package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	pb "payment_gateway/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
)

const (
	ColorReset  = "\033[0m"
	ColorRed    = "\033[31m"
	ColorGreen  = "\033[32m"
	ColorYellow = "\033[33m"
	ColorBlue   = "\033[0m\033[34m"
	ColorCyan   = "\033[0m\033[36m"
)

var offlineQueue []*pb.TransferRequest
var offlineQueueMutex sync.Mutex

func addToOfflineQueue(req *pb.TransferRequest) {
	offlineQueueMutex.Lock()
	defer offlineQueueMutex.Unlock()
	offlineQueue = append(offlineQueue, req)
	log.Printf(ColorYellow+"Client: Txn %s queued for offline processing."+ColorReset, req.TransactionId)
}

func processOfflineQueue(client pb.PaymentGatewayClient, token string) {
	for {
		time.Sleep(10 * time.Second)
		offlineQueueMutex.Lock()
		if len(offlineQueue) == 0 {
			offlineQueueMutex.Unlock()
			continue
		}
		pending := offlineQueue
		offlineQueue = nil
		offlineQueueMutex.Unlock()

		log.Printf(ColorBlue+"Client: Retrying %d offline txn(s)..."+ColorReset, len(pending))
		for _, req := range pending {
			md := metadata.New(map[string]string{"authorization": token})
			authCtx := metadata.NewOutgoingContext(context.Background(), md)
			ctx, cancel := context.WithTimeout(authCtx, 5*time.Second)
			resp, err := client.TransferMoney(ctx, req)
			cancel()
			if err != nil || !resp.Success {
				log.Printf(ColorRed+"Client: Offline txn %s failed: %v"+ColorReset, req.TransactionId, err)
				addToOfflineQueue(req)
			} else {
				log.Printf(ColorGreen+"Client: Offline txn %s processed successfully."+ColorReset, req.TransactionId)
			}
		}
	}
}

// WriteBankUsers writes CSV records to the bank's user file.
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

// ReadBankUsers reads CSV records from the bank's user file.
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

// registerAtBank manually registers a user at the bank.
// File format: AccountId,username,password,bank_name,balance
func registerAtBank(accountID, username, password, bankName string) error {
	filename := fmt.Sprintf("../%s_users.txt", bankName)
	var records [][]string

	if _, err := os.Stat(filename); os.IsNotExist(err) {
		f, err := os.Create(filename)
		if err != nil {
			return fmt.Errorf("failed to create bank users file: %v", err)
		}
		defer f.Close()
		header := "AccountId,username,password,bank_name,balance\n"
		if _, err := f.WriteString(header); err != nil {
			return fmt.Errorf("failed to write header: %v", err)
		}
		records = [][]string{{"AccountId", "username", "password", "bank_name", "balance"}}
	} else {
		var err error
		records, err = ReadBankUsers(bankName)
		if err != nil && err != io.EOF {
			return fmt.Errorf("failed to read bank users file: %v", err)
		}
	}

	for i, record := range records {
		if i == 0 {
			continue
		}
		if len(record) < 1 {
			continue
		}
		if record[0] == accountID {
			if record[1] == username && record[2] == password {
				log.Printf(ColorCyan+"Client: Account %s already registered in bank %s."+ColorReset, accountID, bankName)
				return nil
			}
			return fmt.Errorf("account number %s already registered with different credentials", accountID)
		}
	}

	newRecord := []string{accountID, username, password, bankName, "1000.00"}
	records = append(records, newRecord)
	if err := WriteBankUsers(bankName, records); err != nil {
		return fmt.Errorf("failed to update bank users file: %v", err)
	}
	log.Printf(ColorGreen+"Client: Successfully registered at bank %s with account %s"+ColorReset, bankName, accountID)
	return nil
}

func main() {
	username := flag.String("username", "kevin", "Username")
	password := flag.String("password", "kevin", "Password")
	accountID := flag.String("account", "ACC1", "Account number")
	bankName := flag.String("bank", "ICICI", "Bank name")
	registerFlag := flag.Bool("register", false, "Set to true to register user at bank")
	flag.Parse()

	log.Printf(ColorCyan+"[Startup] Client starting with Username: %s, Account: %s, Bank: %s"+ColorReset, *username, *accountID, *bankName)

	// Load TLS credentials for secure connection to Payment Gateway
	cert, err := tls.LoadX509KeyPair("../certs/client.pem", "../certs/client.key")
	if err != nil {
		log.Fatalf(ColorRed+"Client: failed to load key pair: %s"+ColorReset, err)
	}
	caCert, err := ioutil.ReadFile("../certs/ca.pem")
	if err != nil {
		log.Fatalf(ColorRed+"Client: failed to read CA certificate: %s"+ColorReset, err)
	}
	caCertPool := x509.NewCertPool()
	if !caCertPool.AppendCertsFromPEM(caCert) {
		log.Fatalf(ColorRed + "Client: failed to append CA certs" + ColorReset)
	}
	creds := credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      caCertPool,
	})

	// Connect to Payment Gateway using TLS
	conn, err := grpc.Dial("localhost:50051", grpc.WithTransportCredentials(creds))
	if err != nil {
		log.Fatalf(ColorRed+"Client: did not connect to Gateway: %v"+ColorReset, err)
	}
	defer conn.Close()
	client := pb.NewPaymentGatewayClient(conn)

	// Connect to Transaction ID Server (using insecure connection)
	tidConn, err := grpc.Dial("localhost:50055", grpc.WithInsecure())
	if err != nil {
		log.Fatalf(ColorRed+"Client: could not connect to Transaction ID Server: %v"+ColorReset, err)
	}
	defer tidConn.Close()
	// Use pb.TransactionIDRequest and pb.TransactionIDResponse since they are in the same package.
	// If you set a distinct go_package for transaction_id.proto, update the import accordingly.
	tidClient := pb.NewTransactionIDServiceClient(tidConn)

	var token string
	var txnCounter int

	// Register with the Payment Gateway
	{
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		log.Printf(ColorYellow+"Client: Registering user %s at Gateway"+ColorReset, *username)
		regResp, err := client.Register(ctx, &pb.RegisterRequest{
			Username:  *username,
			Password:  *password,
			AccountId: *accountID,
			BankName:  *bankName,
		})
		if err != nil {
			log.Fatalf(ColorRed+"Client: Gateway registration failed: %v"+ColorReset, err)
		}
		if regResp.Success {
			log.Printf(ColorGreen+"Client: Gateway registration successful: %s"+ColorReset, regResp.Message)
		} else {
			log.Printf(ColorYellow+"Client: Gateway registration update: %s"+ColorReset, regResp.Message)
		}
	}

	// Optionally register at the bank if the flag is set
	if *registerFlag {
		err := registerAtBank(*accountID, *username, *password, *bankName)
		if err != nil {
			log.Fatalf(ColorRed+"Client: Bank registration failed: %v"+ColorReset, err)
		}
	}

	// Authenticate with the Payment Gateway
	{
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		log.Printf(ColorYellow+"Client: Authenticating user %s"+ColorReset, *username)
		authResp, err := client.Authenticate(ctx, &pb.AuthRequest{
			Username:  *username,
			Password:  *password,
			AccountId: *accountID,
		})
		if err != nil {
			log.Fatalf(ColorRed+"Client: Authentication failed: %v"+ColorReset, err)
		}
		if authResp.Token == "" {
			log.Fatalf(ColorRed+"Client: Empty token received: %s"+ColorReset, authResp.Message)
		}
		token = authResp.Token
		log.Printf(ColorGreen+"Client: Authentication successful, token: %s"+ColorReset, token)
	}

	go processOfflineQueue(client, token)

	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Println(ColorBlue + "\nSelect Operation:" + ColorReset)
		fmt.Println("1: Transact")
		fmt.Println("2: Check Balance")
		fmt.Println("0: Exit")
		fmt.Print(ColorBlue + "Your choice: " + ColorReset)
		choice, _ := reader.ReadString('\n')
		choice = strings.TrimSpace(choice)
		switch choice {
		case "1":
			fmt.Print(ColorBlue + "Enter Receiver's Account Number: " + ColorReset)
			toAccount, _ := reader.ReadString('\n')
			toAccount = strings.TrimSpace(toAccount)
			fmt.Print(ColorBlue + "Enter Receiver's Bank Name: " + ColorReset)
			toBank, _ := reader.ReadString('\n')
			toBank = strings.TrimSpace(toBank)
			fmt.Print(ColorBlue + "Enter Amount to Transfer: " + ColorReset)
			amtStr, _ := reader.ReadString('\n')
			amtStr = strings.TrimSpace(amtStr)
			amount, err := strconv.ParseFloat(amtStr, 64)
			if err != nil {
				log.Printf(ColorRed + "Client: Invalid amount entered" + ColorReset)
				continue
			}

			// Request a new transaction ID from the Transaction ID Server
			tidCtx, tidCancel := context.WithTimeout(context.Background(), 3*time.Second)
			tidResp, err := tidClient.GetNewTransactionID(tidCtx, &pb.TransactionIDRequest{})
			tidCancel()
			if err != nil {
				log.Printf(ColorRed+"Client: Failed to get transaction ID: %v"+ColorReset, err)
				continue
			}
			txnID := tidResp.TransactionId
			log.Printf(ColorYellow+"Client: Received Transaction ID: %s"+ColorReset, txnID)

			md := metadata.New(map[string]string{"authorization": token})
			authCtx := metadata.NewOutgoingContext(context.Background(), md)
			ctx, cancel := context.WithTimeout(authCtx, 5*time.Second)
			transReq := &pb.TransferRequest{
				TransactionId: txnID,
				FromAccount:   *accountID,
				ToAccount:     toAccount,
				Amount:        amount,
				FromBank:      *bankName,
				ToBank:        toBank,
			}
			resp, err := client.TransferMoney(ctx, transReq)
			cancel()
			if err != nil {
				log.Printf(ColorRed+"Client: TransferMoney RPC failed: %v"+ColorReset, err)
				addToOfflineQueue(transReq)
			} else {
				if resp.Success {
					log.Printf(ColorGreen+"Client: Transfer successful: %s"+ColorReset, resp.Message)
					txnCounter++
					if txnCounter%3 == 0 {
						log.Printf(ColorBlue+"Client: Testing idempotency for txn %s"+ColorReset, txnID)
						ctxDup, cancelDup := context.WithTimeout(authCtx, 5*time.Second)
						dupResp, err := client.TransferMoney(ctxDup, transReq)
						cancelDup()
						if err != nil {
							log.Printf(ColorRed+"Client: Duplicate txn failed: %v"+ColorReset, err)
						} else {
							log.Printf(ColorGreen+"Client: Duplicate txn response: %s"+ColorReset, dupResp.Message)
						}
					}
				} else {
					lowerMsg := strings.ToLower(resp.Message)
					permanentFailures := []string{
						"not registered", "does not exist", "self-transfer not allowed",
						"insufficient", "invalid credentials", "already processed",
						"duplicate transaction", "already registered", "invalid transaction", "malformed",
						"account not found",
					}
					isPermanent := false
					for _, keyword := range permanentFailures {
						if strings.Contains(lowerMsg, keyword) {
							isPermanent = true
							break
						}
					}
					if isPermanent {
						log.Printf(ColorRed+"Client: Permanent failure: %s"+ColorReset, resp.Message)
					} else {
						log.Printf(ColorRed+"Client: Transfer failed: %s"+ColorReset, resp.Message)
						addToOfflineQueue(transReq)
					}
				}
			}
		case "2":
			md := metadata.New(map[string]string{"authorization": token})
			authCtx := metadata.NewOutgoingContext(context.Background(), md)
			ctx, cancel := context.WithTimeout(authCtx, 5*time.Second)
			balResp, err := client.CheckBalance(ctx, &pb.BalanceRequest{
				AccountId: *accountID,
				BankName:  *bankName,
			})
			cancel()
			if err != nil {
				log.Printf(ColorRed+"Client: CheckBalance failed: %v"+ColorReset, err)
			} else {
				log.Printf(ColorGreen+"Client: Current balance: %.2f"+ColorReset, balResp.Balance)
			}
		case "0":
			log.Printf(ColorBlue + "Client: Exiting..." + ColorReset)
			return
		default:
			log.Printf(ColorRed + "Client: Invalid choice, try again." + ColorReset)
		}
	}
}
