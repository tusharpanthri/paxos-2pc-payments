// Command gateway runs the payment gateway: the front door for clients and
// the two-phase-commit coordinator for transfers between banks.
//
// Usage:
//
//	go run ./gateway -port=50051
package main

import (
	"flag"
	"fmt"
	"log"
	"net"

	"payment_gateway/internal/logx"
	"payment_gateway/internal/tlsconfig"
	pb "payment_gateway/proto"

	"google.golang.org/grpc"
)

type config struct {
	port     int
	certsDir string
	dataDir  string
}

func parseFlags() config {
	var c config
	flag.IntVar(&c.port, "port", 50051, "port to serve on")
	flag.StringVar(&c.certsDir, "certs", "certs", "directory holding TLS certificates")
	flag.StringVar(&c.dataDir, "data", ".", "directory holding gateway_users.txt")
	flag.Parse()
	return c
}

func main() {
	log.SetFlags(log.Ltime)
	if err := run(parseFlags()); err != nil {
		log.Fatalf(logx.Red+"gateway: %v"+logx.Reset, err)
	}
}

func run(cfg config) error {
	auth, err := newAuthStore(cfg.dataDir)
	if err != nil {
		return fmt.Errorf("open auth store: %w", err)
	}
	registry, err := newBankRegistry(cfg.certsDir)
	if err != nil {
		return fmt.Errorf("build bank registry: %w", err)
	}
	defer registry.close()

	health := newGatewayHealth()
	svc := newGatewayService(auth, registry, health)

	creds, err := tlsconfig.ServerCreds(tlsconfig.Files{Dir: cfg.certsDir, Cert: "gateway.pem", Key: "gateway.key"})
	if err != nil {
		return err
	}
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.port))
	if err != nil {
		return fmt.Errorf("listen on port %d: %w", cfg.port, err)
	}

	// Interceptors run outermost first: every call is logged, including the
	// ones authentication rejects.
	server := grpc.NewServer(
		grpc.Creds(creds),
		grpc.ChainUnaryInterceptor(
			loggingUnaryInterceptor,
			svc.authUnaryInterceptor,
		),
	)
	pb.RegisterPaymentGatewayServer(server, svc)

	go health.watchConsole()

	log.Printf(logx.Green+"[startup] gateway listening on :%d with %d registered user(s)"+logx.Reset,
		cfg.port, auth.count())
	return server.Serve(lis)
}
