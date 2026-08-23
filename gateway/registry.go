package main

import (
	"fmt"
	"sync"

	"paxos-2pc-kvstore/internal/tlsconfig"
	pb "paxos-2pc-kvstore/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// bankRegistry maps bank names to their addresses and holds one gRPC client
// per bank.
//
// The original implementation dialled a fresh connection on every transfer
// and closed it afterwards, which pays for a TCP and TLS handshake per
// payment. A grpc.ClientConn is safe for concurrent use and reconnects on its
// own, so it is created once and shared.
type bankRegistry struct {
	creds grpc.DialOption

	mu    sync.RWMutex
	banks map[string]*bankConn
}

type bankConn struct {
	address string
	conn    *grpc.ClientConn
	client  pb.BankClient
}

func newBankRegistry(certsDir string) (*bankRegistry, error) {
	creds, err := tlsconfig.ClientCreds(
		tlsconfig.Files{Dir: certsDir, Cert: "client.pem", Key: "client.key"},
		"localhost",
	)
	if err != nil {
		return nil, err
	}
	return &bankRegistry{
		creds: grpc.WithTransportCredentials(creds),
		banks: make(map[string]*bankConn),
	}, nil
}

// register records where a bank can be reached. Re-registering at a new
// address replaces the old connection.
func (r *bankRegistry) register(name, address string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if existing, ok := r.banks[name]; ok {
		if existing.address == address {
			return nil
		}
		existing.conn.Close()
	}

	conn, err := grpc.NewClient(address, r.creds)
	if err != nil {
		return fmt.Errorf("connect to bank %s at %s: %w", name, address, err)
	}
	r.banks[name] = &bankConn{address: address, conn: conn, client: pb.NewBankClient(conn)}
	return nil
}

// client returns the gRPC client for a bank, or a NotFound status error if no
// bank has registered under that name.
func (r *bankRegistry) client(name string) (pb.BankClient, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	b, ok := r.banks[name]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "bank %q is not registered with this gateway", name)
	}
	return b.client, nil
}

// close releases every bank connection.
func (r *bankRegistry) close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, b := range r.banks {
		b.conn.Close()
	}
	r.banks = make(map[string]*bankConn)
}
