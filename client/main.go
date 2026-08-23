// Command client is the interactive front end: it registers with the
// gateway, authenticates, and offers a small menu for transferring money and
// checking balances.
//
// Transfers that cannot be delivered are held in a durable queue and retried
// in the background, which is why the menu stays responsive while the gateway
// or a bank is down.
//
// Usage:
//
//	go run ./client -username=varun -password=varun -account=ACC1 -bank=ICICI -register
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"paxos-2pc-kvstore/internal/logx"
	"paxos-2pc-kvstore/internal/tlsconfig"
	pb "paxos-2pc-kvstore/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

// rpcTimeout bounds every call this client makes.
const rpcTimeout = 10 * time.Second

type config struct {
	username  string
	password  string
	accountID string
	bankName  string
	gateway   string
	txnIDAddr string
	certsDir  string
	dataDir   string
	register  bool
}

func parseFlags() config {
	var c config
	flag.StringVar(&c.username, "username", "varun", "username to register and log in with")
	flag.StringVar(&c.password, "password", "varun", "password")
	flag.StringVar(&c.accountID, "account", "ACC1", "account id at the bank")
	flag.StringVar(&c.bankName, "bank", "ICICI", "bank holding the account")
	flag.StringVar(&c.gateway, "gateway", "localhost:50051", "address of the payment gateway")
	flag.StringVar(&c.txnIDAddr, "txnid", "localhost:50055", "address of the transaction id service")
	flag.StringVar(&c.certsDir, "certs", "certs", "directory holding TLS certificates")
	flag.StringVar(&c.dataDir, "data", ".", "directory holding data files")
	flag.BoolVar(&c.register, "register", false, "seed this account at the bank before starting")
	flag.Parse()
	return c
}

func main() {
	log.SetFlags(log.Ltime)
	if err := run(parseFlags()); err != nil {
		log.Fatalf(logx.Red+"client: %v"+logx.Reset, err)
	}
}

func run(cfg config) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log.Printf(logx.Cyan+"[startup] %s, account %s at %s"+logx.Reset, cfg.username, cfg.accountID, cfg.bankName)

	if cfg.register {
		if err := seedBankAccount(cfg.dataDir, cfg.accountID, cfg.username, cfg.password, cfg.bankName); err != nil {
			return fmt.Errorf("seed bank account: %w", err)
		}
	}

	gateway, closeGateway, err := dialGateway(cfg)
	if err != nil {
		return err
	}
	defer closeGateway()

	txnIDs, closeTxnIDs, err := dialTransactionIDs(cfg)
	if err != nil {
		return err
	}
	defer closeTxnIDs()

	token, err := registerAndAuthenticate(ctx, gateway, cfg)
	if err != nil {
		return err
	}

	queue, err := newOfflineQueue(filepath.Join(cfg.dataDir, fmt.Sprintf("%s_pending.json", cfg.username)))
	if err != nil {
		return fmt.Errorf("open offline queue: %w", err)
	}
	go queue.drain(ctx, gateway, token)

	menu(ctx, session{cfg: cfg, gateway: gateway, txnIDs: txnIDs, token: token, queue: queue})
	return nil
}

// dialGateway opens the mutually authenticated connection to the gateway.
func dialGateway(cfg config) (pb.PaymentGatewayClient, func(), error) {
	creds, err := tlsconfig.ClientCreds(
		tlsconfig.Files{Dir: cfg.certsDir, Cert: "client.pem", Key: "client.key"},
		"localhost",
	)
	if err != nil {
		return nil, nil, err
	}
	conn, err := grpc.NewClient(cfg.gateway, grpc.WithTransportCredentials(creds))
	if err != nil {
		return nil, nil, fmt.Errorf("connect to gateway at %s: %w", cfg.gateway, err)
	}
	return pb.NewPaymentGatewayClient(conn), func() { conn.Close() }, nil
}

// dialTransactionIDs opens the connection to the transaction id service.
func dialTransactionIDs(cfg config) (pb.TransactionIDServiceClient, func(), error) {
	creds, err := tlsconfig.ClientCreds(
		tlsconfig.Files{Dir: cfg.certsDir, Cert: "client.pem", Key: "client.key"},
		"localhost",
	)
	if err != nil {
		return nil, nil, err
	}
	conn, err := grpc.NewClient(cfg.txnIDAddr, grpc.WithTransportCredentials(creds))
	if err != nil {
		return nil, nil, fmt.Errorf("connect to transaction id service at %s: %w", cfg.txnIDAddr, err)
	}
	return pb.NewTransactionIDServiceClient(conn), func() { conn.Close() }, nil
}

// registerAndAuthenticate makes sure the user exists at the gateway and then
// exchanges the password for a session token.
func registerAndAuthenticate(ctx context.Context, gateway pb.PaymentGatewayClient, cfg config) (string, error) {
	regCtx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()
	if _, err := gateway.Register(regCtx, &pb.RegisterRequest{
		Username:  cfg.username,
		Password:  cfg.password,
		AccountId: cfg.accountID,
		BankName:  cfg.bankName,
	}); err != nil {
		return "", fmt.Errorf("register with gateway: %w", err)
	}

	authCtx, cancelAuth := context.WithTimeout(ctx, rpcTimeout)
	defer cancelAuth()
	resp, err := gateway.Authenticate(authCtx, &pb.AuthRequest{
		Username:  cfg.username,
		Password:  cfg.password,
		AccountId: cfg.accountID,
	})
	if err != nil {
		return "", fmt.Errorf("authenticate: %w", err)
	}
	log.Printf(logx.Green + "[startup] authenticated" + logx.Reset)
	return resp.Token, nil
}

// session bundles everything the menu needs.
type session struct {
	cfg     config
	gateway pb.PaymentGatewayClient
	txnIDs  pb.TransactionIDServiceClient
	token   string
	queue   *offlineQueue
}

func menu(ctx context.Context, s session) {
	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Printf("\n%s1%s transfer   %s2%s balance   %s0%s quit   (%d queued)\n> ",
			logx.Blue, logx.Reset, logx.Blue, logx.Reset, logx.Blue, logx.Reset, s.queue.depth())

		line, err := reader.ReadString('\n')
		if err != nil {
			return // stdin closed
		}
		switch strings.TrimSpace(line) {
		case "1":
			s.transfer(ctx, reader)
		case "2":
			s.checkBalance(ctx)
		case "0":
			log.Printf(logx.Blue + "bye" + logx.Reset)
			return
		case "":
			// ignore stray newlines
		default:
			log.Printf(logx.Red + "unknown option" + logx.Reset)
		}
	}
}

func (s session) transfer(ctx context.Context, reader *bufio.Reader) {
	toAccount := prompt(reader, "receiver account: ")
	toBank := prompt(reader, "receiver bank: ")
	amountText := prompt(reader, "amount: ")

	amount, err := strconv.ParseFloat(amountText, 64)
	if err != nil || amount <= 0 {
		log.Printf(logx.Red+"%q is not a valid amount"+logx.Reset, amountText)
		return
	}

	// The transaction id comes from a central service so that a retry of this
	// transfer reuses the same id and the banks can recognise it as a
	// duplicate rather than moving the money twice.
	txnID, err := s.newTransactionID(ctx)
	if err != nil {
		log.Printf(logx.Red+"could not obtain a transaction id: %v"+logx.Reset, err)
		return
	}

	req := &pb.TransferRequest{
		TransactionId: txnID,
		FromAccount:   s.cfg.accountID,
		ToAccount:     toAccount,
		FromBank:      s.cfg.bankName,
		ToBank:        toBank,
		Amount:        amount,
	}

	callCtx, cancel := context.WithTimeout(withToken(ctx, s.token), rpcTimeout)
	resp, err := s.gateway.TransferMoney(callCtx, req)
	cancel()

	switch {
	case err == nil:
		log.Printf(logx.Green+"%s: %s"+logx.Reset, txnID, resp.Message)
	case retriable(err):
		log.Printf(logx.Yellow+"%s could not be delivered right now: %s"+logx.Reset,
			txnID, status.Convert(err).Message())
		s.queue.add(req)
	default:
		log.Printf(logx.Red+"%s rejected: %s"+logx.Reset, txnID, status.Convert(err).Message())
	}
}

func (s session) newTransactionID(ctx context.Context) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()
	resp, err := s.txnIDs.GetNewTransactionID(ctx, &pb.TransactionIDRequest{})
	if err != nil {
		return "", err
	}
	return resp.TransactionId, nil
}

func (s session) checkBalance(ctx context.Context) {
	ctx, cancel := context.WithTimeout(withToken(ctx, s.token), rpcTimeout)
	defer cancel()

	resp, err := s.gateway.CheckBalance(ctx, &pb.BalanceRequest{
		AccountId: s.cfg.accountID,
		BankName:  s.cfg.bankName,
	})
	if err != nil {
		log.Printf(logx.Red+"balance unavailable: %s"+logx.Reset, status.Convert(err).Message())
		return
	}
	log.Printf(logx.Green+"balance: %.2f"+logx.Reset, resp.Balance)
}

func prompt(reader *bufio.Reader, label string) string {
	fmt.Print(logx.Blue + label + logx.Reset)
	line, _ := reader.ReadString('\n')
	return strings.TrimSpace(line)
}
