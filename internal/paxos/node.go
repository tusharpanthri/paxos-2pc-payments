package paxos

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

// Role is what a replica currently believes itself to be.
type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	}
	return "unknown"
}

// Config tunes the timing of the protocol. The only hard requirement is that
// ElectionTimeout comfortably exceeds HeartbeatInterval -- otherwise
// followers unseat a perfectly healthy leader between its heartbeats.
type Config struct {
	HeartbeatInterval time.Duration // how often a leader asserts itself
	ElectionTimeout   time.Duration // silence after which a follower campaigns
	RoundTimeout      time.Duration // bound on one prepare/accept round trip
}

func (c Config) withDefaults() Config {
	if c.HeartbeatInterval <= 0 {
		c.HeartbeatInterval = 150 * time.Millisecond
	}
	if c.ElectionTimeout <= 0 {
		c.ElectionTimeout = 1 * time.Second
	}
	if c.RoundTimeout <= 0 {
		c.RoundTimeout = 500 * time.Millisecond
	}
	return c
}

// Node is one replica of a Paxos group.
type Node struct {
	id    NodeID
	peers []NodeID // every member of the group, including this one
	cfg   Config

	sm  StateMachine
	tr  Transport
	log *Log

	mu         sync.Mutex
	role       Role
	ballot     Ballot    // the ballot this node leads under, when leader
	leader     NodeID    // best guess at the current leader, for redirects
	electionAt time.Time // campaign if nothing is heard from a leader by then
	nextSlot   uint64    // leader only: the next slot to hand out

	committed    map[uint64]bool
	appliedIndex uint64
	results      map[uint64]chan any

	applyC chan struct{}
	stop   chan struct{}
	wg     sync.WaitGroup
	once   sync.Once
}

// NewNode builds a replica. peers must list every member of the group,
// including id itself, and must be identical on every replica.
func NewNode(id NodeID, peers []NodeID, log *Log, sm StateMachine, tr Transport, cfg Config) *Node {
	n := &Node{
		id:        id,
		peers:     append([]NodeID(nil), peers...),
		cfg:       cfg.withDefaults(),
		sm:        sm,
		tr:        tr,
		log:       log,
		role:      Follower,
		committed: make(map[uint64]bool),
		results:   make(map[uint64]chan any),
		applyC:    make(chan struct{}, 1),
		stop:      make(chan struct{}),
	}
	n.resetElectionLocked()
	return n
}

// ID returns this replica's identity.
func (n *Node) ID() NodeID { return n.id }

// Start launches the election timer and the apply loop.
func (n *Node) Start() {
	n.wg.Add(2)
	go n.runTimers()
	go n.runApply()
}

// Stop halts the replica. It does not close the log; the owner does that.
func (n *Node) Stop() {
	n.once.Do(func() { close(n.stop) })
	n.wg.Wait()
}

// Role reports what this replica currently believes itself to be.
func (n *Node) Role() Role {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.role
}

// IsLeader reports whether this replica is currently leading.
func (n *Node) IsLeader() bool { return n.Role() == Leader }

// Leader returns the replica this node believes is leading. It is a hint:
// correct most of the time, and never trusted for safety.
func (n *Node) Leader() NodeID {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.leader
}

// AppliedIndex is the highest slot applied to the state machine here.
func (n *Node) AppliedIndex() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.appliedIndex
}

// quorum is the number of replicas needed for a majority.
func (n *Node) quorum() int { return majority(len(n.peers)) }

// --- timers -------------------------------------------------------------

// runTimers drives both leader heartbeats and follower election timeouts.
func (n *Node) runTimers() {
	defer n.wg.Done()

	ticker := time.NewTicker(n.cfg.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-n.stop:
			return
		case <-ticker.C:
			n.mu.Lock()
			role, deadline := n.role, n.electionAt
			n.mu.Unlock()

			if role == Leader {
				n.broadcastCommit()
				continue
			}
			if time.Now().After(deadline) {
				n.campaign()
			}
		}
	}
}

// resetElectionLocked pushes the next campaign out by a freshly randomised
// interval. Callers must hold n.mu.
//
// The deadline is drawn once per reset rather than re-rolled on every tick,
// and that distinction matters: re-rolling would mean the smallest of many
// draws decides, so every replica would campaign at close to the base
// timeout and the randomisation would buy nothing. Drawing once is what
// actually staggers the replicas and keeps split votes rare.
func (n *Node) resetElectionLocked() {
	base := n.cfg.ElectionTimeout
	n.electionAt = time.Now().Add(base + time.Duration(rand.Int63n(int64(base))))
}

// --- phase 1: election --------------------------------------------------

// campaign runs PREPARE against the group under a fresh ballot. Winning a
// majority makes this replica leader for every future slot, which is what
// lets the steady state skip phase 1 entirely.
func (n *Node) campaign() {
	n.mu.Lock()
	if n.role == Leader {
		n.mu.Unlock()
		return
	}
	n.role = Candidate
	n.mu.Unlock()

	next := Ballot{Num: n.log.Promised().Num + 1, Node: n.id}
	if err := n.log.Promise(next); err != nil {
		n.stepDown(Ballot{})
		return
	}

	// Slots at or below appliedIndex are settled here and need no recovery.
	first := n.AppliedIndex() + 1

	req := Message{Kind: KindPrepare, From: n.id, Ballot: next, Slot: first}
	replies := n.broadcast(req)

	votes := 1 // this replica promised itself above
	gathered := n.log.EntriesFrom(first)
	highest := next

	for _, r := range replies {
		if r.OK {
			votes++
			gathered = append(gathered, r.Entries...)
			continue
		}
		if highest.Less(r.Ballot) {
			highest = r.Ballot
		}
	}

	if votes < n.quorum() {
		// Outvoted or unreachable. Adopt any higher ballot we learned about
		// so the next campaign starts above it rather than losing again.
		if next.Less(highest) {
			_ = n.log.Promise(highest)
		}
		n.stepDown(Ballot{})
		return
	}
	n.becomeLeader(next, gathered, first)
}

// becomeLeader adopts everything the group had already accepted, then starts
// serving new proposals.
//
// The adoption step is the one that makes Paxos safe across a leader change.
// Any entry in a promise might already be committed on some replica, so the
// new leader must re-propose it under its own ballot rather than assume the
// slot is free. Where several replicas report different commands for a slot,
// the one accepted under the highest ballot wins.
func (n *Node) becomeLeader(b Ballot, gathered []Entry, first uint64) {
	best := make(map[uint64]Entry)
	var maxSlot uint64

	for _, e := range gathered {
		if e.Slot < first {
			continue
		}
		if cur, ok := best[e.Slot]; !ok || cur.Ballot.Less(e.Ballot) {
			best[e.Slot] = e
		}
		if e.Slot > maxSlot {
			maxSlot = e.Slot
		}
	}

	n.mu.Lock()
	n.role = Leader
	n.leader = n.id
	n.ballot = b
	n.resetElectionLocked()
	n.nextSlot = maxSlot + 1
	if n.nextSlot < first {
		n.nextSlot = first
	}
	n.mu.Unlock()

	// Re-propose the recovered range. Gaps are filled with no-ops: the apply
	// loop runs contiguously, so a single hole would stall every slot after
	// it forever.
	go func() {
		for slot := first; slot <= maxSlot; slot++ {
			var cmd []byte
			if e, ok := best[slot]; ok {
				cmd = e.Command
			}
			ctx, cancel := context.WithTimeout(context.Background(), n.cfg.RoundTimeout*2)
			err := n.replicate(ctx, slot, cmd)
			cancel()
			if err != nil {
				return // lost leadership mid-recovery; the next leader retries
			}
		}
	}()
}

// stepDown reverts to follower, optionally recording a ballot seen from a
// more current leader.
func (n *Node) stepDown(seen Ballot) {
	n.mu.Lock()
	n.role = Follower
	n.resetElectionLocked()
	n.mu.Unlock()

	if !seen.IsZero() && n.log.Promised().Less(seen) {
		_ = n.log.Promise(seen)
	}
}

// --- phase 2: replication ----------------------------------------------

// Propose replicates a command and blocks until it has been applied here,
// returning whatever the state machine produced.
func (n *Node) Propose(ctx context.Context, command []byte) (any, error) {
	n.mu.Lock()
	if n.role != Leader {
		leader := n.leader
		n.mu.Unlock()
		return nil, fmt.Errorf("%w (try %q)", ErrNotLeader, leader)
	}
	slot := n.nextSlot
	n.nextSlot++
	done := make(chan any, 1)
	n.results[slot] = done
	n.mu.Unlock()

	if err := n.replicate(ctx, slot, command); err != nil {
		n.mu.Lock()
		delete(n.results, slot)
		n.mu.Unlock()
		return nil, err
	}

	select {
	case res := <-done:
		return res, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-n.stop:
		return nil, fmt.Errorf("paxos: node stopped")
	}
}

// replicate runs one ACCEPT round for a slot and commits it on success.
func (n *Node) replicate(ctx context.Context, slot uint64, command []byte) error {
	n.mu.Lock()
	if n.role != Leader {
		n.mu.Unlock()
		return ErrNotLeader
	}
	b := n.ballot
	n.mu.Unlock()

	entry := Entry{Slot: slot, Ballot: b, Command: command}
	if err := n.log.Accept(entry); err != nil {
		return fmt.Errorf("paxos: persist entry: %w", err)
	}

	req := Message{Kind: KindAccept, From: n.id, Ballot: b, Slot: slot, Command: command}
	replies := n.broadcast(req)

	votes := 1 // this replica accepted its own entry above
	for _, r := range replies {
		if r.OK {
			votes++
			continue
		}
		// A refusal carrying a higher ballot means we have been superseded.
		if b.Less(r.Ballot) {
			n.stepDown(r.Ballot)
			return ErrNotLeader
		}
	}

	if votes < n.quorum() {
		return ErrNoQuorum
	}

	n.markCommitted(slot)
	return nil
}

// markCommitted records a slot as decided and nudges the apply loop.
func (n *Node) markCommitted(slots ...uint64) {
	n.mu.Lock()
	for _, s := range slots {
		n.committed[s] = true
	}
	n.mu.Unlock()

	select {
	case n.applyC <- struct{}{}:
	default: // a wake-up is already pending
	}
}

// runApply is the only goroutine that touches the state machine, which is
// what guarantees commands are applied exactly once and in slot order.
func (n *Node) runApply() {
	defer n.wg.Done()

	for {
		select {
		case <-n.stop:
			return
		case <-n.applyC:
			n.drainApplicable()
		}
	}
}

func (n *Node) drainApplicable() {
	for {
		n.mu.Lock()
		next := n.appliedIndex + 1
		if !n.committed[next] {
			n.mu.Unlock()
			return
		}
		n.mu.Unlock()

		entry, ok := n.log.Get(next)
		if !ok {
			return // committed but not yet stored locally; catch-up will fill it
		}

		var result any
		if !entry.IsNoop() {
			result = n.sm.Apply(next, entry.Command)
		}

		n.mu.Lock()
		n.appliedIndex = next
		waiter := n.results[next]
		delete(n.results, next)
		n.mu.Unlock()

		if waiter != nil {
			waiter <- result
		}
	}
}

// --- message handling ---------------------------------------------------

// Handle processes an incoming protocol message and returns the reply. It is
// the entry point every transport funnels into.
func (n *Node) Handle(ctx context.Context, msg Message) Message {
	switch msg.Kind {
	case KindPrepare:
		return n.handlePrepare(msg)
	case KindAccept:
		return n.handleAccept(msg)
	case KindCommit:
		return n.handleCommit(msg)
	case KindCatchUp:
		return n.handleCatchUp(msg)
	}
	return Message{Kind: msg.Kind, From: n.id, OK: false}
}

// handlePrepare answers phase 1. Promising means two things: this replica
// will refuse anything below the new ballot from now on, and it hands over
// everything it has already accepted so the candidate can adopt it.
func (n *Node) handlePrepare(msg Message) Message {
	reply := Message{Kind: KindPromise, From: n.id}

	promised := n.log.Promised()
	if msg.Ballot.Less(promised) {
		reply.OK = false
		reply.Ballot = promised
		return reply
	}

	if err := n.log.Promise(msg.Ballot); err != nil {
		reply.OK = false
		reply.Ballot = promised
		return reply
	}

	n.mu.Lock()
	n.role = Follower
	n.leader = msg.From
	n.resetElectionLocked()
	n.mu.Unlock()

	reply.OK = true
	reply.Ballot = msg.Ballot
	reply.Entries = n.log.EntriesFrom(msg.Slot)
	return reply
}

// handleAccept answers phase 2, storing the entry durably before agreeing.
func (n *Node) handleAccept(msg Message) Message {
	reply := Message{Kind: KindAccepted, From: n.id, Slot: msg.Slot}

	promised := n.log.Promised()
	if msg.Ballot.Less(promised) {
		reply.OK = false
		reply.Ballot = promised
		return reply
	}

	// An accept from a ballot we have not explicitly promised is still
	// binding -- it can only come from a leader that won a majority, so
	// adopting it here keeps this replica from later accepting something
	// older.
	if promised.Less(msg.Ballot) {
		if err := n.log.Promise(msg.Ballot); err != nil {
			reply.OK = false
			reply.Ballot = promised
			return reply
		}
	}

	entry := Entry{Slot: msg.Slot, Ballot: msg.Ballot, Command: msg.Command}
	if err := n.log.Accept(entry); err != nil {
		reply.OK = false
		reply.Ballot = msg.Ballot
		return reply
	}

	n.mu.Lock()
	n.role = Follower
	n.leader = msg.From
	n.resetElectionLocked()
	n.mu.Unlock()

	reply.OK = true
	reply.Ballot = msg.Ballot
	return reply
}

// handleCommit is the leader's heartbeat. It both suppresses elections and
// tells followers how far it is safe to apply.
func (n *Node) handleCommit(msg Message) Message {
	reply := Message{Kind: KindCommit, From: n.id}

	promised := n.log.Promised()
	if msg.Ballot.Less(promised) {
		reply.OK = false
		reply.Ballot = promised
		return reply
	}

	n.mu.Lock()
	n.role = Follower
	n.leader = msg.From
	n.resetElectionLocked()
	applied := n.appliedIndex
	n.mu.Unlock()

	if msg.CommitIndex > applied {
		slots := make([]uint64, 0, msg.CommitIndex-applied)
		for s := applied + 1; s <= msg.CommitIndex; s++ {
			slots = append(slots, s)
		}
		n.markCommitted(slots...)

		// If the leader has committed past what we hold, fetch the gap.
		if n.log.MaxSlot() < msg.CommitIndex {
			go n.catchUp(msg.From, applied+1)
		}
	}

	reply.OK = true
	reply.Ballot = msg.Ballot
	return reply
}

// handleCatchUp serves entries to a replica that has fallen behind.
func (n *Node) handleCatchUp(msg Message) Message {
	return Message{
		Kind:    KindCatchUp,
		From:    n.id,
		OK:      true,
		Entries: n.log.EntriesFrom(msg.Slot),
	}
}

// catchUp pulls missing entries from the leader and stores them, so the
// apply loop can move past the gap.
func (n *Node) catchUp(from NodeID, first uint64) {
	ctx, cancel := context.WithTimeout(context.Background(), n.cfg.RoundTimeout*2)
	defer cancel()

	reply, err := n.tr.Send(ctx, from, Message{Kind: KindCatchUp, From: n.id, Slot: first})
	if err != nil || !reply.OK {
		return
	}
	for _, e := range reply.Entries {
		if _, have := n.log.Get(e.Slot); have {
			continue
		}
		_ = n.log.Accept(e)
	}

	select {
	case n.applyC <- struct{}{}:
	default:
	}
}

// broadcastCommit is the leader's periodic heartbeat.
func (n *Node) broadcastCommit() {
	n.mu.Lock()
	b, applied := n.ballot, n.appliedIndex
	n.mu.Unlock()

	replies := n.broadcast(Message{
		Kind:        KindCommit,
		From:        n.id,
		Ballot:      b,
		CommitIndex: applied,
	})

	// A peer refusing our heartbeat with a higher ballot means the group has
	// moved on without us -- stop acting as leader immediately.
	for _, r := range replies {
		if !r.OK && b.Less(r.Ballot) {
			n.stepDown(r.Ballot)
			return
		}
	}
}

// broadcast sends a message to every peer but this one, in parallel, and
// collects whatever comes back before the round timeout. Unreachable peers
// simply contribute nothing, which is exactly how a quorum protocol should
// treat them.
func (n *Node) broadcast(msg Message) []Message {
	ctx, cancel := context.WithTimeout(context.Background(), n.cfg.RoundTimeout)
	defer cancel()

	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		replies []Message
	)

	for _, peer := range n.peers {
		if peer == n.id {
			continue
		}
		wg.Add(1)
		go func(to NodeID) {
			defer wg.Done()
			reply, err := n.tr.Send(ctx, to, msg)
			if err != nil {
				return
			}
			mu.Lock()
			replies = append(replies, reply)
			mu.Unlock()
		}(peer)
	}

	wg.Wait()
	return replies
}
