// Command bank runs one replica of one bank: a two-phase-commit participant
// that owns a CSV file of accounts and applies the debit or credit half of a
// transfer on behalf of the payment gateway.
//
// A bank is a Paxos group. Every prepare, commit and abort is replicated
// across the group's replicas before it takes effect, so a replica can crash
// without losing a reservation or a settled transfer. Started with no -id or
// -peers, a bank is a single-member group -- always its own leader -- which
// is what keeps the plain quickstart working unchanged.
//
// Usage:
//
//	go run ./bank -bank=ICICI -port=50052
//	go run ./bank -bank=ICICI -id=ICICI-1 -port=50110 -data=data/ICICI-1 \
//	    -peers=ICICI-1=localhost:50210,ICICI-2=localhost:50211,ICICI-3=localhost:50212
package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"strings"
	"time"

	"os"

	"paxos-2pc-kvstore/internal/accounts"
	"paxos-2pc-kvstore/internal/logx"
	"paxos-2pc-kvstore/internal/paxos"
	"paxos-2pc-kvstore/internal/tlsconfig"
	pb "paxos-2pc-kvstore/proto"

	"google.golang.org/grpc"
)

// registrationTimeout bounds a single announcement call to the gateway.
const registrationTimeout = 5 * time.Second

// electionPollInterval is how often the leadership-watch loop checks whether
// this replica has just become leader.
const electionPollInterval = 150 * time.Millisecond

type config struct {
	bankName string
	id       string
	peers    string
	port     int
	gateway  string
	certsDir string
	dataDir  string
}

func parseFlags() config {
	var c config
	flag.StringVar(&c.bankName, "bank", "ICICI", "name of this bank; also selects its data files")
	flag.StringVar(&c.id, "id", "", "this replica's id within the bank's Paxos group (default: the bank name, for a single-member group)")
	flag.StringVar(&c.peers, "peers", "", "comma-separated id=host:port list of every replica's consensus address, including this one (default: a single-member group on port+1000)")
	flag.IntVar(&c.port, "port", 50052, "port to serve on")
	flag.StringVar(&c.gateway, "gateway", "localhost:50051", "address of the payment gateway")
	flag.StringVar(&c.certsDir, "certs", "certs", "directory holding TLS certificates")
	flag.StringVar(&c.dataDir, "data", ".", "directory holding the account, ledger and Paxos log files")
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

	id := cfg.id
	if id == "" {
		id = cfg.bankName
	}
	peers, err := parsePeers(cfg.peers)
	if err != nil {
		return err
	}
	if len(peers) == 0 {
		peers[paxos.NodeID(id)] = fmt.Sprintf("localhost:%d", cfg.port+1000)
	}
	consensusAddr, ok := peers[paxos.NodeID(id)]
	if !ok {
		return fmt.Errorf("this replica's id %q is not present in -peers %q", id, cfg.peers)
	}

	if err := os.MkdirAll(cfg.dataDir, 0o755); err != nil {
		return fmt.Errorf("create data directory %s: %w", cfg.dataDir, err)
	}

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
	participant := newParticipant(store, l)

	node, plog, err := startPaxosGroup(id, peers, cfg, participant)
	if err != nil {
		return fmt.Errorf("start Paxos group: %w", err)
	}
	defer plog.Close()
	defer node.Stop()

	if err := serveConsensus(node, h, consensusAddr, cfg.certsDir); err != nil {
		return fmt.Errorf("serve consensus endpoint: %w", err)
	}
	go watchLeadership(node, cfg)

	svc := newBankService(cfg.bankName, store, node, h)

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

	log.Printf(logx.Green+"[startup] bank %s (%s) listening on :%d, consensus on %s, group=%v"+logx.Reset,
		cfg.bankName, id, cfg.port, consensusAddr, peerIDs(peers))
	return server.Serve(lis)
}

// startPaxosGroup opens this replica's durable Paxos log and builds the Node
// that replicates every prepare/commit/abort across the group.
func startPaxosGroup(id string, peers map[paxos.NodeID]string, cfg config, p *participant) (*paxos.Node, *paxos.Log, error) {
	plog, err := paxos.OpenLog(cfg.dataDir, paxos.NodeID(id))
	if err != nil {
		return nil, nil, err
	}

	clientTLS, err := tlsconfig.ClientTLS(
		tlsconfig.Files{Dir: cfg.certsDir, Cert: "client.pem", Key: "client.key"},
		"localhost",
	)
	if err != nil {
		return nil, nil, err
	}
	transport := paxos.NewHTTPTransport(peers, clientTLS, 2*time.Second)

	// Tuned tighter than the library defaults: this is a local demo, not a
	// WAN deployment, and a snappy election is what makes a single-member
	// group (the default, backward-compatible case) self-elect almost
	// instantly instead of sitting idle for a second at start-up.
	pcfg := paxos.Config{
		HeartbeatInterval: 100 * time.Millisecond,
		ElectionTimeout:   400 * time.Millisecond,
		RoundTimeout:      300 * time.Millisecond,
	}

	node := paxos.NewNode(paxos.NodeID(id), peerIDs(peers), plog, newBankStateMachine(p), transport, pcfg)
	node.Start()
	return node, plog, nil
}

// serveConsensus starts the mTLS HTTP listener the rest of the group sends
// Paxos protocol messages to. It is gated by the same health switch as the
// gRPC handlers, so typing "down" in this replica's console also drops it
// out of consensus -- a real crash simulation, not just a client-visible one.
func serveConsensus(node *paxos.Node, h *health, addr, certsDir string) error {
	serverTLS, err := tlsconfig.ServerTLS(tlsconfig.Files{Dir: certsDir, Cert: "bank.pem", Key: "bank.key"})
	if err != nil {
		return err
	}

	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("parse consensus address %q: %w", addr, err)
	}
	lis, err := tls.Listen("tcp", ":"+port, serverTLS)
	if err != nil {
		return fmt.Errorf("listen for consensus on :%s: %w", port, err)
	}

	handler := healthGate(h, paxos.Handler(node))
	go func() {
		if err := http.Serve(lis, handler); err != nil {
			log.Printf(logx.Red+"[consensus] listener stopped: %v"+logx.Reset, err)
		}
	}()
	return nil
}

// watchLeadership announces this replica to the gateway every time it
// becomes the group's leader, including after a failover election. The
// gateway already replaces a bank's registered address on every call
// (bankRegistry.register), so this is what keeps its routing table pointed
// at a reachable replica without the gateway needing to know anything about
// Paxos.
func watchLeadership(node *paxos.Node, cfg config) {
	var wasLeader bool
	for {
		isLeader := node.IsLeader()
		if isLeader && !wasLeader {
			log.Printf(logx.Cyan+"[election] %s is now the leader of its group"+logx.Reset, node.ID())
			announceWithRetry(cfg)
		}
		wasLeader = isLeader
		time.Sleep(electionPollInterval)
	}
}

// announceWithRetry calls announceToGateway a few times before giving up. A
// freshly elected leader is still useful even if the gateway is not up yet
// -- the next election (or, in a real deployment, a retried announce) will
// find it.
func announceWithRetry(cfg config) {
	const attempts = 5
	for i := 1; i <= attempts; i++ {
		if err := announceToGateway(cfg); err == nil {
			return
		} else if i == attempts {
			log.Printf(logx.Yellow+"[startup] could not register with gateway after %d attempts: %v"+logx.Reset, attempts, err)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// parsePeers parses a comma-separated "id=host:port" list into a map. An
// empty string is not an error -- the caller fills in a single-member
// default.
func parsePeers(raw string) (map[paxos.NodeID]string, error) {
	peers := make(map[paxos.NodeID]string)
	for _, part := range strings.Split(raw, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		id, addr, ok := strings.Cut(part, "=")
		if !ok || id == "" || addr == "" {
			return nil, fmt.Errorf("invalid -peers entry %q; want id=host:port", part)
		}
		peers[paxos.NodeID(id)] = addr
	}
	return peers, nil
}

func peerIDs(peers map[paxos.NodeID]string) []paxos.NodeID {
	ids := make([]paxos.NodeID, 0, len(peers))
	for id := range peers {
		ids = append(ids, id)
	}
	return ids
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
// keeps the registry in memory, so this is called every time this replica
// becomes the group's leader.
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
