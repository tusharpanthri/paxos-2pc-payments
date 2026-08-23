package paxos

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"
)

// --- harness ------------------------------------------------------------

// recorder is a trivial state machine: it remembers the commands it was
// given, in the order it was given them. Comparing two recorders is how the
// tests assert that replication actually kept the group in step.
type recorder struct {
	mu      sync.Mutex
	applied []string
}

func (r *recorder) Apply(slot uint64, command []byte) any {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.applied = append(r.applied, string(command))
	return len(r.applied)
}

func (r *recorder) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.applied...)
}

type cluster struct {
	t     *testing.T
	net   *MemNetwork
	ids   []NodeID
	nodes map[NodeID]*Node
	sms   map[NodeID]*recorder
	logs  map[NodeID]*Log

	mu   sync.Mutex
	dead map[NodeID]bool // replicas the test has killed or isolated
}

// testConfig runs the protocol an order of magnitude faster than production
// so the suite finishes in seconds rather than minutes.
func testConfig() Config {
	return Config{
		HeartbeatInterval: 25 * time.Millisecond,
		ElectionTimeout:   150 * time.Millisecond,
		RoundTimeout:      100 * time.Millisecond,
	}
}

func newCluster(t *testing.T, size int) *cluster {
	t.Helper()

	c := &cluster{
		t:     t,
		net:   NewMemNetwork(),
		nodes: make(map[NodeID]*Node),
		sms:   make(map[NodeID]*recorder),
		logs:  make(map[NodeID]*Log),
		dead:  make(map[NodeID]bool),
	}
	for i := 1; i <= size; i++ {
		c.ids = append(c.ids, NodeID(fmt.Sprintf("n%d", i)))
	}

	dir := t.TempDir()
	for _, id := range c.ids {
		log, err := OpenLog(dir, id)
		if err != nil {
			t.Fatalf("open log for %s: %v", id, err)
		}
		sm := &recorder{}
		node := NewNode(id, c.ids, log, sm, c.net.Transport(id), testConfig())

		c.logs[id], c.sms[id], c.nodes[id] = log, sm, node
		c.net.Register(node)
	}

	for _, id := range c.ids {
		c.nodes[id].Start()
	}

	t.Cleanup(func() {
		for _, id := range c.ids {
			c.nodes[id].Stop()
			c.logs[id].Close()
		}
	})
	return c
}

// waitFor polls until cond holds or the deadline passes.
func waitFor(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// kill makes a replica unreachable and stops counting it as a candidate
// leader for the purposes of the assertions below.
func (c *cluster) kill(id NodeID) {
	c.net.Kill(id)
	c.markDead(id, true)
}

// isolate partitions a replica off from the group without stopping it.
func (c *cluster) isolate(id NodeID) {
	c.net.Isolate(id)
	c.markDead(id, true)
}

// heal restores a replica's links.
func (c *cluster) heal(id NodeID) {
	c.net.Heal(id)
	c.markDead(id, false)
}

func (c *cluster) markDead(id NodeID, dead bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if dead {
		c.dead[id] = true
	} else {
		delete(c.dead, id)
	}
}

func (c *cluster) isDead(id NodeID) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.dead[id]
}

// leaders returns every reachable replica currently claiming leadership.
func (c *cluster) leaders() []*Node {
	var out []*Node
	for _, id := range c.ids {
		if c.isDead(id) {
			continue
		}
		if c.nodes[id].IsLeader() {
			out = append(out, c.nodes[id])
		}
	}
	return out
}

// awaitLeader blocks until exactly one reachable replica is leading.
func (c *cluster) awaitLeader() *Node {
	c.t.Helper()
	var leader *Node
	waitFor(c.t, "a single leader", 5*time.Second, func() bool {
		ls := c.leaders()
		if len(ls) == 1 {
			leader = ls[0]
			return true
		}
		return false
	})
	return leader
}

// propose sends a command to whichever replica is currently leading,
// re-resolving and retrying if leadership moves mid-flight.
//
// This mirrors what any real caller has to do -- the gateway included.
// Leadership is not a property a client can pin down and hold: the leader it
// found a moment ago may have been outvoted, and the only correct response
// to ErrNotLeader is to find the new one and try again.
func (c *cluster) propose(command string) (any, error) {
	c.t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	var lastErr error

	for time.Now().Before(deadline) {
		ls := c.leaders()
		if len(ls) != 1 {
			time.Sleep(10 * time.Millisecond)
			continue
		}

		res, err := c.proposeTo(ls[0], command)
		if err == nil {
			return res, nil
		}
		lastErr = err
		if !errors.Is(err, ErrNotLeader) {
			return nil, err
		}
		time.Sleep(10 * time.Millisecond)
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("no leader available")
	}
	return nil, lastErr
}

// proposeTo sends a command to one specific replica, with no retry. Tests
// that care which replica answers use this directly.
func (c *cluster) proposeTo(node *Node, command string) (any, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return node.Propose(ctx, []byte(command))
}

// --- ballots ------------------------------------------------------------

func TestBallotOrderingIsTotal(t *testing.T) {
	a := Ballot{Num: 1, Node: "n1"}
	b := Ballot{Num: 1, Node: "n2"}
	c := Ballot{Num: 2, Node: "n1"}

	if !a.Less(b) {
		t.Error("same number: lower node id must sort first, so two candidates never tie")
	}
	if !b.Less(c) {
		t.Error("higher ballot number must dominate regardless of node id")
	}
	if a.Less(a) {
		t.Error("a ballot must not be less than itself")
	}
	if !a.AtLeast(a) {
		t.Error("a ballot must be at least itself")
	}
}

// --- durability ---------------------------------------------------------

// A replica that forgets its promise could accept a ballot it had already
// ruled out; one that forgets an accepted entry could tell a new leader a
// slot was free when it may already be committed. Both must survive restart.
func TestLogSurvivesRestart(t *testing.T) {
	dir := t.TempDir()

	log, err := OpenLog(dir, "n1")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	promised := Ballot{Num: 7, Node: "n2"}
	if err := log.Promise(promised); err != nil {
		t.Fatalf("promise: %v", err)
	}
	if err := log.Accept(Entry{Slot: 1, Ballot: promised, Command: []byte("set x=1")}); err != nil {
		t.Fatalf("accept: %v", err)
	}
	if err := log.Accept(Entry{Slot: 2, Ballot: promised, Command: []byte("set y=2")}); err != nil {
		t.Fatalf("accept: %v", err)
	}
	log.Close()

	reopened, err := OpenLog(dir, "n1")
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()

	if got := reopened.Promised(); got.Compare(promised) != 0 {
		t.Errorf("promised ballot lost across restart: got %v, want %v", got, promised)
	}
	if got := reopened.MaxSlot(); got != 2 {
		t.Errorf("max slot after replay = %d, want 2", got)
	}
	e, ok := reopened.Get(2)
	if !ok || string(e.Command) != "set y=2" {
		t.Errorf("entry 2 lost across restart: %+v (present=%v)", e, ok)
	}
}

// A slot re-proposed under a higher ballot must supersede the older entry.
func TestLogLastWriteWinsPerSlot(t *testing.T) {
	dir := t.TempDir()
	log, err := OpenLog(dir, "n1")
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	log.Accept(Entry{Slot: 1, Ballot: Ballot{Num: 1, Node: "n1"}, Command: []byte("old")})
	log.Accept(Entry{Slot: 1, Ballot: Ballot{Num: 2, Node: "n2"}, Command: []byte("new")})
	log.Close()

	reopened, err := OpenLog(dir, "n1")
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()

	e, _ := reopened.Get(1)
	if string(e.Command) != "new" {
		t.Errorf("slot 1 = %q after replay, want the higher-ballot entry %q", e.Command, "new")
	}
}

// --- election -----------------------------------------------------------

// The group must converge on one leader that every replica agrees about.
//
// Note what is deliberately *not* asserted: that only one replica ever
// believes itself to be leading. During a handover an outgoing leader keeps
// believing it leads until it hears a higher ballot, so two replicas can
// briefly both think they are in charge. That is harmless, and it is not the
// property Paxos actually promises. The guarantee is that only one of them
// can *commit*, because only one can assemble a quorum -- which is what
// TestStallsWithoutQuorum and TestConcurrentProposalsAgreeOnOneOrder pin
// down. Asserting single-belief here would be asserting something stronger
// than the protocol provides, and would flake accordingly.
func TestConvergesOnOneAgreedLeader(t *testing.T) {
	c := newCluster(t, 3)
	c.awaitLeader()

	waitFor(t, "every replica to agree on one leader", 5*time.Second, func() bool {
		ls := c.leaders()
		if len(ls) != 1 {
			return false
		}
		for _, id := range c.ids {
			if c.nodes[id].Leader() != ls[0].ID() {
				return false
			}
		}
		return true
	})

	// Once settled, leadership should be stable rather than churning: a
	// healthy group re-elects only when the leader actually stops heartbeating.
	settled := c.leaders()[0].ID()
	time.Sleep(400 * time.Millisecond)

	ls := c.leaders()
	if len(ls) != 1 || ls[0].ID() != settled {
		t.Errorf("leadership churned while the group was healthy: was %q, now %v",
			settled, leaderIDs(ls))
	}
}

func leaderIDs(nodes []*Node) []NodeID {
	out := make([]NodeID, 0, len(nodes))
	for _, n := range nodes {
		out = append(out, n.ID())
	}
	return out
}

// --- replication --------------------------------------------------------

func TestReplicatesToEveryReplica(t *testing.T) {
	c := newCluster(t, 3)
	c.awaitLeader()

	want := []string{"a=1", "b=2", "c=3", "d=4", "e=5"}
	for _, cmd := range want {
		if _, err := c.propose(cmd); err != nil {
			t.Fatalf("propose %q: %v", cmd, err)
		}
	}

	// Followers apply on the next heartbeat, so give them a moment.
	waitFor(t, "all replicas to converge", 3*time.Second, func() bool {
		for _, id := range c.ids {
			if !reflect.DeepEqual(c.sms[id].snapshot(), want) {
				return false
			}
		}
		return true
	})

	for _, id := range c.ids {
		if got := c.sms[id].snapshot(); !reflect.DeepEqual(got, want) {
			t.Errorf("replica %s applied %v, want %v", id, got, want)
		}
	}
}

func TestProposeOnFollowerIsRejected(t *testing.T) {
	c := newCluster(t, 3)
	leader := c.awaitLeader()

	for _, id := range c.ids {
		if id == leader.ID() {
			continue
		}
		if _, err := c.proposeTo(c.nodes[id], "nope"); err == nil {
			t.Errorf("follower %s accepted a proposal; only the leader may assign slots", id)
		}
		break
	}
}

// --- failure ------------------------------------------------------------

// A three-node group has a quorum of two, so losing one replica must not
// interrupt service.
func TestSurvivesOneFollowerFailure(t *testing.T) {
	c := newCluster(t, 3)
	leader := c.awaitLeader()

	var victim NodeID
	for _, id := range c.ids {
		if id != leader.ID() {
			victim = id
			break
		}
	}
	c.kill(victim)

	if _, err := c.propose("survives"); err != nil {
		t.Fatalf("group of 3 lost 1 replica and stopped serving: %v", err)
	}
}

// Losing two of three costs the quorum, and the honest behaviour is to stop
// rather than to commit something a majority never saw.
func TestStallsWithoutQuorum(t *testing.T) {
	c := newCluster(t, 3)
	leader := c.awaitLeader()

	for _, id := range c.ids {
		if id != leader.ID() {
			c.kill(id)
		}
	}

	if _, err := c.proposeTo(leader, "should not commit"); err == nil {
		t.Fatal("leader committed without a quorum, which would allow divergence")
	}
}

// The important one: a leader dies, the survivors elect a new one, and
// everything committed before the crash is still there afterwards.
func TestNewLeaderElectedAfterCrashAndDataSurvives(t *testing.T) {
	c := newCluster(t, 3)
	old := c.awaitLeader()

	committed := []string{"before=1", "before=2"}
	for _, cmd := range committed {
		if _, err := c.propose(cmd); err != nil {
			t.Fatalf("propose %q: %v", cmd, err)
		}
	}
	waitFor(t, "commits to reach the followers", 3*time.Second, func() bool {
		for _, id := range c.ids {
			if id != old.ID() && len(c.sms[id].snapshot()) < len(committed) {
				return false
			}
		}
		return true
	})

	c.kill(old.ID())

	fresh := c.awaitLeader()
	if fresh.ID() == old.ID() {
		t.Fatal("the crashed replica was re-elected")
	}

	if _, err := c.propose("after=3"); err != nil {
		t.Fatalf("new leader could not serve: %v", err)
	}

	want := append(append([]string(nil), committed...), "after=3")
	waitFor(t, "survivors to agree on the full log", 3*time.Second, func() bool {
		for _, id := range c.ids {
			if id == old.ID() {
				continue
			}
			if !reflect.DeepEqual(c.sms[id].snapshot(), want) {
				return false
			}
		}
		return true
	})
}

// A replica cut off from the group must not be able to commit alone, and
// must catch up on everything it missed once the partition heals.
func TestPartitionedReplicaCatchesUpAfterHealing(t *testing.T) {
	c := newCluster(t, 5)
	leader := c.awaitLeader()

	var straggler NodeID
	for _, id := range c.ids {
		if id != leader.ID() {
			straggler = id
			break
		}
	}
	c.isolate(straggler)

	want := []string{"x=1", "x=2", "x=3", "x=4"}
	for _, cmd := range want {
		if _, err := c.propose(cmd); err != nil {
			t.Fatalf("propose %q: %v", cmd, err)
		}
	}

	if got := c.sms[straggler].snapshot(); len(got) != 0 {
		t.Fatalf("isolated replica applied %v; a minority must never commit", got)
	}

	c.heal(straggler)

	waitFor(t, "the straggler to catch up", 5*time.Second, func() bool {
		return reflect.DeepEqual(c.sms[straggler].snapshot(), want)
	})
}

// Concurrent proposals must still produce one agreed order, identical
// everywhere -- that is what linearizability buys.
func TestConcurrentProposalsAgreeOnOneOrder(t *testing.T) {
	c := newCluster(t, 3)
	c.awaitLeader()

	const n = 25
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c.propose(fmt.Sprintf("cmd-%02d", i))
		}(i)
	}
	wg.Wait()

	waitFor(t, "replicas to converge under concurrency", 5*time.Second, func() bool {
		base := c.sms[c.ids[0]].snapshot()
		if len(base) != n {
			return false
		}
		for _, id := range c.ids[1:] {
			if !reflect.DeepEqual(c.sms[id].snapshot(), base) {
				return false
			}
		}
		return true
	})

	// Every command must appear exactly once: no duplicates, none lost.
	seen := make(map[string]int)
	for _, cmd := range c.sms[c.ids[0]].snapshot() {
		seen[cmd]++
	}
	if len(seen) != n {
		t.Errorf("expected %d distinct commands, got %d", n, len(seen))
	}
	for cmd, count := range seen {
		if count != 1 {
			t.Errorf("command %q applied %d times, want exactly once", cmd, count)
		}
	}
}
