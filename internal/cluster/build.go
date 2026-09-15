package cluster

import (
	"context"
	"fmt"
	"sync"

	"google.golang.org/grpc"

	"paxos-2pc-kvstore/internal/ledger"
	"paxos-2pc-kvstore/internal/paxos"
	"paxos-2pc-kvstore/internal/transport"
)

// NewInproc builds a cluster whose replicas are goroutines in this process,
// talking over the channel transport.
func NewInproc(cfg Config) (*Engine, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	cfg = cfg.withDefaults()

	faults := transport.NewFaults()
	e := newEngine(cfg, faults)
	network := transport.NewInproc(faults, e.onTrace)

	var nodes []*paxos.Node
	for s := 0; s < cfg.Shards; s++ {
		peers := make([]paxos.NodeID, cfg.NodesPerShard)
		for i := range peers {
			peers[i] = paxos.NodeID(NodeName(s, i))
		}
		var group []Replica
		for _, id := range peers {
			sm := ledger.New(cfg.MaxKeysPerShard)
			node := paxos.NewNode(id, peers, paxos.NewMemLog(), sm, network.For(id), cfg.Timing)
			network.Register(id, node.Handle)
			nodes = append(nodes, node)
			group = append(group, &localReplica{node: node, sm: sm})
		}
		e.shards = append(e.shards, group)
	}

	for _, n := range nodes {
		n.Start()
	}
	e.closeFn = func() {
		var wg sync.WaitGroup
		for _, n := range nodes {
			wg.Add(1)
			go func(n *paxos.Node) { defer wg.Done(); n.Stop() }(n)
		}
		wg.Wait()
		network.Close()
	}
	e.start()
	return e, nil
}

// NewRemote builds an engine over node processes already listening at addrs,
// keyed by replica name (s0n0 -> host:port).
func NewRemote(cfg Config, addrs map[string]string) (*Engine, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	cfg = cfg.withDefaults()

	e := newEngine(cfg, transport.NewFaults())
	var remotes []*remoteReplica
	for s := 0; s < cfg.Shards; s++ {
		var group []Replica
		for i := 0; i < cfg.NodesPerShard; i++ {
			name := NodeName(s, i)
			addr, ok := addrs[name]
			if !ok {
				return nil, fmt.Errorf("no address for %s", name)
			}
			conn, err := grpc.NewClient(addr, transport.DialOptions()...)
			if err != nil {
				return nil, fmt.Errorf("dial %s at %s: %w", name, addr, err)
			}
			r := &remoteReplica{id: name, conn: conn}
			remotes = append(remotes, r)
			group = append(group, r)
		}
		e.shards = append(e.shards, group)
	}

	e.pushFaults = func(ctx context.Context, st transport.FaultState) {
		for _, r := range remotes {
			_ = r.setFaults(ctx, st)
		}
	}
	e.drain = func(ctx context.Context) []transport.Trace {
		var out []transport.Trace
		for _, r := range remotes {
			out = append(out, r.drain(ctx)...)
		}
		return out
	}
	e.closeFn = func() {
		for _, r := range remotes {
			_ = r.conn.Close()
		}
	}

	// The node processes may have outlived an earlier gateway; start from a
	// whole network and discard traffic nobody asked to see.
	e.pushFaults(context.Background(), transport.FaultState{})
	e.drain(context.Background())
	e.start()
	return e, nil
}
