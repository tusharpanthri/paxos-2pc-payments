// Package cluster runs a sharded ledger: several Multi-Paxos groups from
// internal/paxos, a two-phase-commit coordinator across them, and the fault
// table the operator drives. It is the layer the browser gateway talks to.
//
// Replicas are reached through the Replica interface, so the same Engine
// drives goroutines in this process (inproc) or separate node processes over
// gRPC -- the operations, the narration and the fault semantics are shared.
package cluster

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"time"

	"paxos-2pc-kvstore/internal/ledger"
	"paxos-2pc-kvstore/internal/paxos"
)

// Config describes the topology.
type Config struct {
	Shards          int
	NodesPerShard   int
	MaxKeysPerShard int          // bound on distinct accounts per shard, 0 for none
	Timing          paxos.Config // zero fields take DefaultTiming values
}

// DefaultTiming is tuned for a demo: fast enough that an election after a
// kill reads as live, slow enough that heartbeats never race elections.
func DefaultTiming() paxos.Config {
	return paxos.Config{
		HeartbeatInterval: 100 * time.Millisecond,
		ElectionTimeout:   700 * time.Millisecond,
		RoundTimeout:      350 * time.Millisecond,
	}
}

// Validate reports whether the topology is buildable.
func (c Config) Validate() error {
	if c.Shards < 1 || c.Shards > 16 {
		return errors.New("shards must be between 1 and 16")
	}
	if c.NodesPerShard < 1 || c.NodesPerShard > 9 {
		return errors.New("nodes per shard must be between 1 and 9")
	}
	return nil
}

func (c Config) withDefaults() Config {
	d := DefaultTiming()
	if c.Timing.HeartbeatInterval <= 0 {
		c.Timing.HeartbeatInterval = d.HeartbeatInterval
	}
	if c.Timing.ElectionTimeout <= 0 {
		c.Timing.ElectionTimeout = d.ElectionTimeout
	}
	if c.Timing.RoundTimeout <= 0 {
		c.Timing.RoundTimeout = d.RoundTimeout
	}
	return c
}

// Quorum is a strict majority of one shard's replicas.
func (c Config) Quorum() int { return c.NodesPerShard/2 + 1 }

// NodeName names a replica s{shard}n{index}.
func NodeName(shard, index int) string { return fmt.Sprintf("s%dn%d", shard, index) }

// ShardTag is the log tag for a shard, e.g. "[s0]".
func ShardTag(shard int) string { return fmt.Sprintf("[s%d]", shard) }

// ParseNode splits a replica name into shard and index.
func ParseNode(name string) (shard, index int, err error) {
	var tail string
	if n, _ := fmt.Sscanf(name, "s%dn%d%s", &shard, &index, &tail); n != 2 || shard < 0 || index < 0 {
		return 0, 0, fmt.Errorf("%w: %q (node ids look like s0n1)", ErrUnknownNode, name)
	}
	return shard, index, nil
}

// ShardFor maps an account to its shard with an unsalted FNV-1a hash, so the
// mapping is stable across runs and the frontend's guided tour can rely on it.
func ShardFor(key string, shards int) int {
	if shards <= 1 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() % uint32(shards))
}

// Errors rendered as ABORT results: expected outcomes of breaking a cluster,
// not malfunctions.
var (
	ErrNoQuorum          = paxos.ErrNoQuorum
	ErrNoLeader          = errors.New("no leader")
	ErrUnknownNode       = errors.New("unknown node")
	ErrNoSuchKey         = ledger.ErrNoSuchKey
	ErrInsufficientFunds = ledger.ErrInsufficientFunds
	ErrLocked            = ledger.ErrLocked
	ErrTooManyKeys       = ledger.ErrTooManyKeys
)

// LogLine is one accepted log entry, for display.
type LogLine struct {
	Slot    uint64 `json:"slot"`
	Ballot  string `json:"ballot"`
	Command string `json:"command"`
}

// NodeInfo is one replica's view of itself.
type NodeInfo struct {
	ID       string           `json:"id"`
	Role     string           `json:"role"`
	Ballot   paxos.Ballot     `json:"ballot"` // highest promised
	Applied  uint64           `json:"applied"`
	Accounts []ledger.Account `json:"accounts"`
	Tail     []LogLine        `json:"tail"` // the last few accepted entries
}

// Replica is one member of a shard's Paxos group.
type Replica interface {
	ID() string
	// Propose replicates a command through this replica, which must lead.
	Propose(ctx context.Context, cmd ledger.Command) (ledger.Result, error)
	Info(ctx context.Context) (NodeInfo, error)
	// Campaign starts an election on this replica now.
	Campaign(ctx context.Context) error
}

// tailLen is how many log entries NodeInfo carries.
const tailLen = 5

// describe builds NodeInfo for an in-process node. It is shared by the local
// replica and the gRPC node service.
func describe(node *paxos.Node, sm *ledger.Machine) NodeInfo {
	info := NodeInfo{
		ID:       string(node.ID()),
		Role:     node.Role().String(),
		Ballot:   node.Promised(),
		Applied:  node.AppliedIndex(),
		Accounts: sm.Snapshot(),
	}
	entries := node.Log().EntriesFrom(1)
	if len(entries) > tailLen {
		entries = entries[len(entries)-tailLen:]
	}
	for _, e := range entries {
		text := "no-op"
		if !e.IsNoop() {
			if c, err := ledger.Decode(e.Command); err == nil {
				text = c.String()
			}
		}
		info.Tail = append(info.Tail, LogLine{Slot: e.Slot, Ballot: e.Ballot.String(), Command: text})
	}
	return info
}

// localReplica is a paxos.Node running in this process.
type localReplica struct {
	node *paxos.Node
	sm   *ledger.Machine
}

func (r *localReplica) ID() string { return string(r.node.ID()) }

func (r *localReplica) Propose(ctx context.Context, cmd ledger.Command) (ledger.Result, error) {
	return propose(ctx, r.node, cmd)
}

func (r *localReplica) Info(context.Context) (NodeInfo, error) { return describe(r.node, r.sm), nil }

func (r *localReplica) Campaign(context.Context) error {
	r.node.Campaign()
	return nil
}

func propose(ctx context.Context, node *paxos.Node, cmd ledger.Command) (ledger.Result, error) {
	raw, err := node.Propose(ctx, cmd.Encode())
	if err != nil {
		return ledger.Result{}, err
	}
	res, ok := raw.(ledger.Result)
	if !ok {
		return ledger.Result{}, fmt.Errorf("unexpected apply result %T", raw)
	}
	return res, nil
}
