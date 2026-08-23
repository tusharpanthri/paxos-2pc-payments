// Command bank runs one bank server: a two-phase-commit participant that
// owns a CSV file of accounts and applies the debit or credit half of a
// transfer on behalf of the payment gateway.
//
// Usage:
//
//	go run ./bank -bank=ICICI -port=50052
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"time"

	"paxos-2pc-kvstore/internal/accounts"
	"paxos-2pc-kvstore/internal/logx"
	"paxos-2pc-kvstore/internal/tlsconfig"
	pb "paxos-2pc-kvstore/proto"

	"google.golang.org/grpc"
)

// registrationTimeout bounds the announcement call to the gateway at start-up.
const registrationTimeout = 5 * time.Second

type config struct {
	bankName string
	port     int
	gateway  string
	certsDir string
	dataDir  string
}

func parseFlags() config {
	var c config
	flag.StringVar(&c.bankName, "bank", "ICICI", "name of this bank; also selects its data files")
	flag.IntVar(&c.port, "port", 50052, "port to serve on")
	flag.StringVar(&c.gateway, "gateway", "localhost:50051", "address of the payment gateway")
	flag.StringVar(&c.certsDir, "certs", "certs", "directory holding TLS certificates")
	flag.StringVar(&c.dataDir, "data", ".", "directory holding the account and ledger files")
	flag.Parse()
	return c
}

func main() {
	log.SetFlags(log.Ltime)
	cfg := parseFlags()

	if err := run(cfg); err != nil {
		log.Fatalf(logx.Red+"bank %s: %v"+logx.Reset, cfg.bankName, err)
	}
}

func run(cfg config) error {
	log.Printf(logx.Cyan+"[startup] bank %s"+logx.Reset, cfg.bankName)

	store, err := accounts.NewStore(cfg.dataDir, cfg.bankName)
	if err != nil {
		return fmt.Errorf("open account store: %w", err)
	}
	l, err := openLedger(cfg.dataDir, cfg.bankName)
	if err != nil {
		return fmt.Errorf("open ledger: %w", err)
	}
	logAccounts(store, l)

	h := newHealth(cfg.bankName)
	svc := newBankService(cfg.bankName, store, newParticipant(store, l), h)

	creds, err := tlsconfig.ServerCreds(tlsconfig.Files{Dir: cfg.certsDir, Cert: "bank.pem", Key: "bank.key"})
	if err != nil {
		return err
	}
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.port))
	if err != nil {
		return fmt.Errorf("listen on port %d: %w", cfg.port, err)
	}

	server := grpc.NewServer(grpc.Creds(creds))
	pb.RegisterBankServer(server, svc)

	go h.watchConsole()
	if err := announceToGateway(cfg); err != nil {
		// A bank that cannot register is useless to the gateway, but it is
		// still worth serving: the operator may start the gateway second.
		log.Printf(logx.Yellow+"[startup] could not register with gateway: %v"+logx.Reset, err)
	}

	log.Printf(logx.Green+"[startup] bank %s listening on :%d"+logx.Reset, cfg.bankName, cfg.port)
	return server.Serve(lis)
}

// logAccounts prints the opening state so a demo run is easy to follow.
func logAccounts(store *accounts.Store, l *ledger) {
	all, err := store.List()
	if err != nil {
		log.Printf(logx.Red+"[startup] could not read %s: %v"+logx.Reset, store.Path(), err)
		return
	}
	log.Printf(logx.Cyan+"[startup] %d account(s) in %s, %d settled operation(s) in the ledger"+logx.Reset,
		len(all), store.Path(), l.count())
	for _, a := range all {
		log.Printf("           %s  %-10s  %.2f", a.ID, a.Username, a.Balance)
	}
}

// announceToGateway tells the gateway where to reach this bank. The gateway
// keeps the registry in memory, so every bank re-announces at start-up.
func announceToGateway(cfg config) error {
	creds, err := tlsconfig.ClientCreds(
		tlsconfig.Files{Dir: cfg.certsDir, Cert: "client.pem", Key: "client.key"},
		"localhost",
	)
	if err != nil {
		return err
	}
	conn, err := grpc.NewClient(cfg.gateway, grpc.WithTransportCredentials(creds))
	if err != nil {
		return fmt.Errorf("dial gateway: %w", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), registrationTimeout)
	defer cancel()

	resp, err := pb.NewPaymentGatewayClient(conn).BankRegister(ctx, &pb.BankRegisterRequest{
		BankName:    cfg.bankName,
		BankAddress: fmt.Sprintf("localhost:%d", cfg.port),
	})
	if err != nil {
		return err
	}
	log.Printf(logx.Green+"[startup] registered with gateway: %s"+logx.Reset, resp.Message)
	return nil
}
