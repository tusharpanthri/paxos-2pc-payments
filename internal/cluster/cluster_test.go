package cluster

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"paxos-2pc-kvstore/internal/ledger"
	"paxos-2pc-kvstore/internal/logstream"
	"paxos-2pc-kvstore/internal/paxos"
)

type recorder struct {
	mu     sync.Mutex
	events []logstream.Event
}

func (r *recorder) Emit(e logstream.Event) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, e)
}

func (r *recorder) text() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var b strings.Builder
	for _, e := range r.events {
		b.WriteString(string(e.Level) + " " + e.Tag + " " + e.Msg + "\n")
	}
	return b.String()
}

func newTestCluster(t *testing.T) (*Engine, *recorder) {
	t.Helper()
	e, err := NewInproc(Config{Shards: 3, NodesPerShard: 3, MaxKeysPerShard: 64})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	rec := &recorder{}
	e.Do(rec, func() { e.Bootstrap(context.Background()) })
	st := e.Status(context.Background())
	for _, s := range st.Shards {
		if s.Leader != NodeName(s.Shard, 0) {
			t.Fatalf("s%d bootstrapped with leader %q, want n0", s.Shard, s.Leader)
		}
		if want := NodeName(s.Shard, 0) + " elected leader"; !strings.Contains(rec.text(), want) {
			t.Fatalf("bootstrap never announced %q:\n%s", want, rec.text())
		}
	}
	return e, rec
}

// do runs fn as one operator command, the way the gateway does.
func do[T any](e *Engine, rec *recorder, fn func(ctx context.Context) (T, error)) (T, error) {
	var (
		out T
		err error
	)
	e.Do(rec, func() { out, err = fn(context.Background()) })
	return out, err
}

func exec(e *Engine, rec *recorder, fn func(ctx context.Context) error) error {
	_, err := do(e, rec, func(ctx context.Context) (struct{}, error) { return struct{}{}, fn(ctx) })
	return err
}

func mustBalance(t *testing.T, e *Engine, rec *recorder, key string, want int64) {
	t.Helper()
	got, err := do(e, rec, func(ctx context.Context) (int64, error) { return e.Get(ctx, key) })
	if err != nil {
		t.Fatalf("get %s: %v\n%s", key, err, rec.text())
	}
	if got != want {
		t.Fatalf("%s = %d, want %d", key, got, want)
	}
}

func TestPutNarratesAcceptAndCommit(t *testing.T) {
	e, _ := newTestCluster(t)
	rec := &recorder{}
	if err := exec(e, rec, func(ctx context.Context) error { return e.Put(ctx, "tushar", 100) }); err != nil {
		t.Fatal(err)
	}
	out := rec.text()
	for _, want := range []string{"PREPARE/PROMISE already held", "ACCEPT slot=1", "ACCEPTED slot=1", "COMMIT slot=1 put tushar=100 (accepted by 3/3"} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
	for _, ev := range rec.events {
		if !logstream.Valid(ev.Level) {
			t.Errorf("invalid level %q", ev.Level)
		}
	}
}

// TestTour walks the same script as the frontend's guided tour.
func TestTour(t *testing.T) {
	e, rec := newTestCluster(t)
	bg := context.Background()

	for key, v := range map[string]int64{"tushar": 100, "ram": 50, "varun": 40} {
		if err := exec(e, rec, func(ctx context.Context) error { return e.Put(ctx, key, v) }); err != nil {
			t.Fatal(err)
		}
	}

	// Same shard: single round.
	if err := exec(e, rec, func(ctx context.Context) error { return e.Transfer(ctx, "tushar", "ram", 30) }); err != nil {
		t.Fatal(err)
	}
	mustBalance(t, e, rec, "tushar", 70)
	mustBalance(t, e, rec, "ram", 80)

	// Cross shard: 2PC.
	rec2 := &recorder{}
	if err := exec(e, rec2, func(ctx context.Context) error { return e.Transfer(ctx, "tushar", "varun", 25) }); err != nil {
		t.Fatalf("%v\n%s", err, rec2.text())
	}
	for _, want := range []string{"BEGIN tx1", "PREPARE tx1 -> s2", "vote YES", "PREPARE tx1 -> s1", "decision COMMIT", "END tx1"} {
		if !strings.Contains(rec2.text(), want) {
			t.Errorf("missing %q in:\n%s", want, rec2.text())
		}
	}
	mustBalance(t, e, rec, "tushar", 45)
	mustBalance(t, e, rec, "varun", 65)

	// Insufficient funds aborts and releases locks.
	err := exec(e, rec, func(ctx context.Context) error { return e.Transfer(ctx, "varun", "tushar", 1000) })
	if !errors.Is(err, ErrInsufficientFunds) {
		t.Fatalf("got %v, want insufficient funds", err)
	}
	if err := exec(e, rec, func(ctx context.Context) error { return e.Transfer(ctx, "varun", "tushar", 5) }); err != nil {
		t.Fatalf("lock was not released by abort: %v", err)
	}

	// Kill the leader of tushar's shard: re-election, writes continue.
	if err := exec(e, rec, func(ctx context.Context) error { return e.Kill(ctx, "s2n0") }); err != nil {
		t.Fatal(err)
	}
	st := e.Status(bg)
	if st.Shards[2].Leader == "" || st.Shards[2].Leader == "s2n0" {
		t.Fatalf("no new leader after killing s2n0: %+v\n%s", st.Shards[2], rec.text())
	}
	if err := exec(e, rec, func(ctx context.Context) error { return e.Transfer(ctx, "tushar", "varun", 10) }); err != nil {
		t.Fatalf("%v\n%s", err, rec.text())
	}

	// Second kill: no majority, writes refused.
	if err := exec(e, rec, func(ctx context.Context) error { return e.Kill(ctx, st.Shards[2].Leader) }); err != nil {
		t.Fatal(err)
	}
	err = exec(e, rec, func(ctx context.Context) error { return e.Put(ctx, "tushar", 999) })
	if !errors.Is(err, ErrNoQuorum) {
		t.Fatalf("write with 1/3 alive: got %v, want no quorum", err)
	}

	// Revive both: writes work and the revived replicas converge.
	for _, n := range []string{"s2n0", st.Shards[2].Leader} {
		if err := exec(e, rec, func(ctx context.Context) error { return e.Revive(ctx, n) }); err != nil {
			t.Fatal(err)
		}
	}
	mustBalance(t, e, rec, "tushar", 40)
	assertConverged(t, e, 2)

	// Partition every replica of s2 apart: all alive, no leader.
	if err := exec(e, rec, func(ctx context.Context) error {
		return e.Partition(ctx, [][]string{{"s2n0"}, {"s2n1"}, {"s2n2"}})
	}); err != nil {
		t.Fatal(err)
	}
	st = e.Status(bg)
	if st.Shards[2].Leader != "" || st.Shards[2].Alive != 3 {
		t.Fatalf("fully partitioned shard: %+v", st.Shards[2])
	}

	// Heal: a leader again, and everything still adds up.
	if err := exec(e, rec, func(ctx context.Context) error { return e.Heal(ctx) }); err != nil {
		t.Fatal(err)
	}
	mustBalance(t, e, rec, "tushar", 40)
	mustBalance(t, e, rec, "varun", 70)
	mustBalance(t, e, rec, "ram", 80)
}

// A leader isolated in a minority must step down, the majority must elect, and
// the old leader must not keep an entry it accepted alone once it rejoins.
func TestMinorityLeaderStepsDownAndConverges(t *testing.T) {
	e, rec := newTestCluster(t)
	if err := exec(e, rec, func(ctx context.Context) error { return e.Put(ctx, "varun", 10) }); err != nil {
		t.Fatal(err)
	}
	if err := exec(e, rec, func(ctx context.Context) error {
		return e.Partition(ctx, [][]string{{"s1n0"}, {"s1n1", "s1n2"}})
	}); err != nil {
		t.Fatal(err)
	}
	st := e.Status(context.Background())
	if l := st.Shards[1].Leader; l != "s1n1" && l != "s1n2" {
		t.Fatalf("majority side leader = %q\n%s", l, rec.text())
	}
	info, _ := e.shards[1][0].Info(context.Background())
	if info.Role == "leader" {
		t.Fatal("isolated s1n0 still claims leadership")
	}

	// Force the isolated old leader to accept an entry nobody else has: this is
	// the divergence the catch-up fix guards against.
	old := e.shards[1][0].(*localReplica)
	_ = old.node.Log().Accept(entryAt(old, 2, "put varun=666"))
	if got := e.shards[1][0].(*localReplica).sm.Snapshot(); len(got) != 1 || got[0].Balance != 10 {
		t.Fatalf("unexpected state on s1n0 before heal: %v", got)
	}

	if err := exec(e, rec, func(ctx context.Context) error { return e.Put(ctx, "varun", 20) }); err != nil {
		t.Fatal(err)
	}
	if err := exec(e, rec, func(ctx context.Context) error { return e.Heal(ctx) }); err != nil {
		t.Fatal(err)
	}
	mustBalance(t, e, rec, "varun", 20)
	assertConverged(t, e, 1)
}

func TestFollowerKillKeepsLeader(t *testing.T) {
	e, rec := newTestCluster(t)
	if err := exec(e, rec, func(ctx context.Context) error { return e.Kill(ctx, "s0n2") }); err != nil {
		t.Fatal(err)
	}
	if l := e.Status(context.Background()).Shards[0].Leader; l != "s0n0" {
		t.Fatalf("leader changed to %q after killing a follower", l)
	}
	if err := exec(e, rec, func(ctx context.Context) error { return e.Kill(ctx, "nope") }); !errors.Is(err, ErrUnknownNode) {
		t.Fatalf("got %v, want unknown node", err)
	}
}

func assertConverged(t *testing.T, e *Engine, s int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		infos := e.infos(context.Background(), s)
		same := true
		for _, info := range infos[1:] {
			if info.Applied != infos[0].Applied || fmtAccounts(info) != fmtAccounts(infos[0]) {
				same = false
			}
		}
		if same {
			return
		}
		if time.Now().After(deadline) {
			for _, info := range infos {
				t.Logf("%s applied=%d %s", info.ID, info.Applied, fmtAccounts(info))
			}
			t.Fatalf("s%d replicas did not converge", s)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func fmtAccounts(info NodeInfo) string {
	return fmt.Sprint(info.Accounts)
}

// entryAt builds a log entry under a replica's own promised ballot.
func entryAt(r *localReplica, slot uint64, _ string) paxos.Entry {
	cmd := ledger.Command{Op: ledger.OpPut, Key: "varun", Amount: 666}
	return paxos.Entry{Slot: slot, Ballot: r.node.Promised(), Command: cmd.Encode()}
}
