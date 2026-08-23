package paxos

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// MemNetwork is an in-process stand-in for the real transport: replicas talk
// to each other through direct calls instead of gRPC.
//
// It exists so the protocol can be tested against the failures that actually
// matter -- a dead leader, a partitioned minority, a replica that rejoins
// after missing a hundred commits -- without standing up nine processes and
// nine certificates. The production transport is a thin gRPC shim over the
// same Node.Handle entry point, so what is exercised here is the real
// protocol, not a simulation of it.
type MemNetwork struct {
	mu      sync.RWMutex
	nodes   map[NodeID]*Node
	down    map[NodeID]bool
	blocked map[NodeID]map[NodeID]bool
	latency time.Duration
}

// NewMemNetwork returns an empty network.
func NewMemNetwork() *MemNetwork {
	return &MemNetwork{
		nodes:   make(map[NodeID]*Node),
		down:    make(map[NodeID]bool),
		blocked: make(map[NodeID]map[NodeID]bool),
	}
}

// Register attaches a replica to the network.
func (m *MemNetwork) Register(n *Node) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nodes[n.ID()] = n
}

// Transport returns the transport a given replica should send through.
func (m *MemNetwork) Transport(from NodeID) Transport {
	return memTransport{net: m, from: from}
}

// SetLatency adds an artificial delay to every delivery, which is useful for
// widening the window in which races would show up.
func (m *MemNetwork) SetLatency(d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.latency = d
}

// Kill makes a replica unreachable in both directions, as a crashed process
// would be. The replica keeps running; it simply cannot be talked to, which
// is also how it discovers it has been isolated.
func (m *MemNetwork) Kill(id NodeID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.down[id] = true
}

// Revive undoes Kill.
func (m *MemNetwork) Revive(id NodeID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.down, id)
}

// Disconnect blocks the link from a to b, leaving b to a alone. One-way
// blocks are how the nastier partitions are built.
func (m *MemNetwork) Disconnect(a, b NodeID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.blocked[a] == nil {
		m.blocked[a] = make(map[NodeID]bool)
	}
	m.blocked[a][b] = true
}

// Connect restores a link blocked by Disconnect.
func (m *MemNetwork) Connect(a, b NodeID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.blocked[a] != nil {
		delete(m.blocked[a], b)
	}
}

// Isolate cuts a replica off from every peer, in both directions, while
// leaving it running. This is the partition case rather than the crash case:
// the isolated node still believes it may be leader until it fails to reach
// a quorum.
func (m *MemNetwork) Isolate(id NodeID) {
	m.mu.Lock()
	peers := make([]NodeID, 0, len(m.nodes))
	for other := range m.nodes {
		if other != id {
			peers = append(peers, other)
		}
	}
	m.mu.Unlock()

	for _, p := range peers {
		m.Disconnect(id, p)
		m.Disconnect(p, id)
	}
}

// Heal restores every link to and from a replica.
func (m *MemNetwork) Heal(id NodeID) {
	m.mu.Lock()
	peers := make([]NodeID, 0, len(m.nodes))
	for other := range m.nodes {
		if other != id {
			peers = append(peers, other)
		}
	}
	m.mu.Unlock()

	for _, p := range peers {
		m.Connect(id, p)
		m.Connect(p, id)
	}
}

// route reports whether a message may travel from -> to, and returns the
// destination replica when it may.
func (m *MemNetwork) route(from, to NodeID) (*Node, time.Duration, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.down[from] || m.down[to] {
		return nil, 0, fmt.Errorf("paxos/memnet: %s is down", to)
	}
	if m.blocked[from] != nil && m.blocked[from][to] {
		return nil, 0, fmt.Errorf("paxos/memnet: link %s->%s is partitioned", from, to)
	}
	node, ok := m.nodes[to]
	if !ok {
		return nil, 0, fmt.Errorf("paxos/memnet: no such node %s", to)
	}
	return node, m.latency, nil
}

type memTransport struct {
	net  *MemNetwork
	from NodeID
}

func (t memTransport) Send(ctx context.Context, to NodeID, msg Message) (Message, error) {
	node, latency, err := t.net.route(t.from, to)
	if err != nil {
		return Message{}, err
	}
	if latency > 0 {
		select {
		case <-time.After(latency):
		case <-ctx.Done():
			return Message{}, ctx.Err()
		}
	}
	return node.Handle(ctx, msg), nil
}
