package cluster

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"
)

// TestGRPCTransport runs every replica behind its own gRPC server on loopback,
// the same code path as separately launched node processes, and drives it with
// the same engine operations as inproc.
func TestGRPCTransport(t *testing.T) {
	cfg := Config{Shards: 2, NodesPerShard: 3, MaxKeysPerShard: 64}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	addrs := map[string]string{}
	for s := 0; s < cfg.Shards; s++ {
		for i := 0; i < cfg.NodesPerShard; i++ {
			addrs[NodeName(s, i)] = freeAddr(t)
		}
	}
	for s := 0; s < cfg.Shards; s++ {
		peers := map[string]string{}
		for i := 0; i < cfg.NodesPerShard; i++ {
			peers[NodeName(s, i)] = addrs[NodeName(s, i)]
		}
		for i := 0; i < cfg.NodesPerShard; i++ {
			opts := NodeOptions{ID: NodeName(s, i), Listen: addrs[NodeName(s, i)], Peers: peers, MaxKeys: 64}
			go func() {
				if err := ServeNode(ctx, opts); err != nil {
					t.Errorf("node %s: %v", opts.ID, err)
				}
			}()
		}
	}
	time.Sleep(200 * time.Millisecond)

	e, err := NewRemote(cfg, addrs)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })

	rec := &recorder{}
	e.Do(rec, func() { e.Bootstrap(context.Background()) })

	a, b := keysOnDifferentShards(cfg.Shards)
	for _, k := range []string{a, b} {
		if err := exec(e, rec, func(ctx context.Context) error { return e.Put(ctx, k, 50) }); err != nil {
			t.Fatalf("put %s: %v\n%s", k, err, rec.text())
		}
	}
	if !strings.Contains(rec.text(), "ACCEPTED slot=1") {
		t.Fatalf("traces were not drained from node processes:\n%s", rec.text())
	}
	if err := exec(e, rec, func(ctx context.Context) error { return e.Transfer(ctx, a, b, 20) }); err != nil {
		t.Fatalf("transfer: %v\n%s", err, rec.text())
	}
	mustBalance(t, e, rec, b, 70)

	// Faults are pushed to the node processes and enforced there.
	sa := ShardFor(a, cfg.Shards)
	if err := exec(e, rec, func(ctx context.Context) error { return e.Kill(ctx, NodeName(sa, 0)) }); err != nil {
		t.Fatal(err)
	}
	if err := exec(e, rec, func(ctx context.Context) error { return e.Kill(ctx, NodeName(sa, 1)) }); err != nil {
		t.Fatal(err)
	}
	if err := exec(e, rec, func(ctx context.Context) error { return e.Put(ctx, a, 1) }); !errors.Is(err, ErrNoQuorum) {
		t.Fatalf("write with 1/3 alive: %v", err)
	}
	if err := exec(e, rec, func(ctx context.Context) error { return e.Revive(ctx, NodeName(sa, 1)) }); err != nil {
		t.Fatal(err)
	}
	mustBalance(t, e, rec, a, 30)
}

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

func keysOnDifferentShards(shards int) (string, string) {
	a := "acct0"
	for i := 1; ; i++ {
		b := fmt.Sprintf("acct%d", i)
		if ShardFor(b, shards) != ShardFor(a, shards) {
			return a, b
		}
	}
}
