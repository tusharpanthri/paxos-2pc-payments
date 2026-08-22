// Command transaction_id_server hands out the transaction ids that make
// retries safe.
//
// Every transfer carries an id issued here. Because a retry reuses the id it
// was originally given, the banks can recognise a repeat and apply the
// transfer at most once. Generating the id on the client would not work:
// two clients could pick the same one, and a client that generated a fresh id
// per attempt would turn one payment into several.
//
// Usage:
//
//	go run ./transaction_id_server -port=50055
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"payment_gateway/internal/tlsconfig"
	pb "payment_gateway/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

type config struct {
	port     int
	certsDir string
	dataDir  string
}

func parseFlags() config {
	var c config
	flag.IntVar(&c.port, "port", 50055, "port to serve on")
	flag.StringVar(&c.certsDir, "certs", "certs", "directory holding TLS certificates")
	flag.StringVar(&c.dataDir, "data", ".", "directory holding transaction_counter.txt")
	flag.Parse()
	return c
}

// counter is a monotonic sequence that survives a restart.
//
// The current value is written to disk before an id is handed out, so a crash
// can only ever skip ids, never reissue one. Skipping is harmless; reissuing
// would let two different payments share an idempotency key.
type counter struct {
	path string

	mu   sync.Mutex
	next int64
}

func openCounter(dir string) (*counter, error) {
	c := &counter{path: filepath.Join(dir, "transaction_counter.txt"), next: 1}

	data, err := os.ReadFile(c.path)
	if os.IsNotExist(err) {
		return c, c.persistLocked()
	}
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", c.path, err)
	}

	value, err := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("%s contains %q, which is not a number: %w", c.path, string(data), err)
	}
	c.next = value
	return c, nil
}

// issue returns the next id, persisting the new high-water mark first.
func (c *counter) issue() (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	id := fmt.Sprintf("TXN-%06d", c.next)
	c.next++
	if err := c.persistLocked(); err != nil {
		c.next-- // nothing was handed out, so do not burn the id
		return "", err
	}
	return id, nil
}

func (c *counter) persistLocked() error {
	return os.WriteFile(c.path, []byte(strconv.FormatInt(c.next, 10)), 0o644)
}

type service struct {
	pb.UnimplementedTransactionIDServiceServer
	counter *counter
}

func (s *service) GetNewTransactionID(ctx context.Context, _ *pb.TransactionIDRequest) (*pb.TransactionIDResponse, error) {
	id, err := s.counter.issue()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "could not persist the counter: %v", err)
	}
	log.Printf("issued %s to %s", id, callerAddress(ctx))
	return &pb.TransactionIDResponse{TransactionId: id}, nil
}

func callerAddress(ctx context.Context) string {
	if p, ok := peer.FromContext(ctx); ok {
		return p.Addr.String()
	}
	return "unknown"
}

func main() {
	log.SetFlags(log.Ltime)
	log.SetPrefix("[txnid] ")
	if err := run(parseFlags()); err != nil {
		log.Fatal(err)
	}
}

func run(cfg config) error {
	c, err := openCounter(cfg.dataDir)
	if err != nil {
		return err
	}
	creds, err := tlsconfig.ServerCreds(tlsconfig.Files{Dir: cfg.certsDir, Cert: "txnid.pem", Key: "txnid.key"})
	if err != nil {
		return err
	}
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.port))
	if err != nil {
		return fmt.Errorf("listen on port %d: %w", cfg.port, err)
	}

	server := grpc.NewServer(grpc.Creds(creds))
	pb.RegisterTransactionIDServiceServer(server, &service{counter: c})

	log.Printf("listening on :%d, next id is TXN-%06d", cfg.port, c.next)
	return server.Serve(lis)
}
