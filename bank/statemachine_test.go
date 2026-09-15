package main

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"paxos-2pc-kvstore/internal/accounts"
	"paxos-2pc-kvstore/internal/paxos"
)

// This file proves the actual claim behind replicating a bank: a prepare and
// commit proposed on the leader converge onto every replica's own store, and
// a replica that crashes right after does not take that commit with it --
// the surviving majority elects a new leader whose store already reflects
// it. internal/paxos/paxos_test.go already covers the protocol itself in
// depth; this only exercises the bankStateMachine wiring on top of it.

// bankReplica is one member of a test bank group: its own account store,
// ledger, participant and Paxos node, all rooted in its own temp directory
// so replicas never share files -- exactly like separate processes would not.
type bankReplica struct {
	store *accounts.Store
	node  *paxos.Node
	log   *paxos.Log
}

type bankCluster struct {
	t        *testing.T
	net      *paxos.MemNetwork
	ids      []paxos.NodeID
	replicas map[paxos.NodeID]*bankReplica
	dead     map[paxos.NodeID]bool
}

func testPaxosConfig() paxos.Config {
	return paxos.Config{
		HeartbeatInterval: 20 * time.Millisecond,
		ElectionTimeout:   150 * time.Millisecond,
		RoundTimeout:      100 * time.Millisecond,
	}
}

// newBankCluster builds size replicas of one bank, each seeded with a SENDER
// holding openingBalance and an empty RECEIVER.
func newBankCluster(t *testing.T, size int, openingBalance float64) *bankCluster {
	t.Helper()

	c := &bankCluster{
		t:        t,
		net:      paxos.NewMemNetwork(),
		replicas: make(map[paxos.NodeID]*bankReplica),
		dead:     make(map[paxos.NodeID]bool),
	}
	for i := 1; i <= size; i++ {
		c.ids = append(c.ids, paxos.NodeID(fmt.Sprintf("n%d", i)))
	}

	for _, id := range c.ids {
		dir := t.TempDir()

		store, err := accounts.NewStore(dir, "TestBank")
		if err != nil {
			t.Fatalf("NewStore(%s): %v", id, err)
		}
		for accID, balance := range map[string]float64{"SENDER": openingBalance, "RECEIVER": 0} {
			if err := store.Create(accounts.Account{ID: accID, Username: accID, Bank: "TestBank", Balance: balance}); err != nil {
				t.Fatalf("seed %s on %s: %v", accID, id, err)
			}
		}
		l, err := openLedger(dir, "TestBank")
		if err != nil {
			t.Fatalf("openLedger(%s): %v", id, err)
		}
		plog, err := paxos.OpenLog(dir, id)
		if err != nil {
			t.Fatalf("OpenLog(%s): %v", id, err)
		}

		sm := newBankStateMachine(newParticipant(store, l))
		node := paxos.NewNode(id, c.ids, plog, sm, c.net.Transport(id), testPaxosConfig())

		c.replicas[id] = &bankReplica{store: store, node: node, log: plog}
		c.net.Register(node)
	}

	for _, r := range c.replicas {
		r.node.Start()
	}
	t.Cleanup(func() {
		for _, r := range c.replicas {
			r.node.Stop()
			r.log.Close()
		}
	})
	return c
}

// kill makes a replica unreachable, as a crashed process would be, and stops
// counting it as a candidate leader for the purposes of the helpers below.
func (c *bankCluster) kill(id paxos.NodeID) {
	c.net.Kill(id)
	c.dead[id] = true
}

// leaders returns every reachable replica currently claiming leadership. An
// unreachable node can go on believing itself leader (it simply cannot win
// the quorum any new proposal needs), so dead replicas are excluded here.
func (c *bankCluster) leaders() []paxos.NodeID {
	var out []paxos.NodeID
	for _, id := range c.ids {
		if c.dead[id] {
			continue
		}
		if c.replicas[id].node.IsLeader() {
			out = append(out, id)
		}
	}
	return out
}

// awaitLeader blocks until exactly one reachable replica is leading.
func (c *bankCluster) awaitLeader() paxos.NodeID {
	c.t.Helper()
	var leader paxos.NodeID
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
// re-resolving if leadership moves mid-flight -- the same thing bankService
// does by calling node.Propose and the same thing the gateway's retry loop
// does at the RPC layer.
func (c *bankCluster) propose(cmd command) (applyResult, error) {
	c.t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		ls := c.leaders()
		if len(ls) != 1 {
			time.Sleep(10 * time.Millisecond)
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		res, err := c.replicas[ls[0]].node.Propose(ctx, encodeCommand(cmd))
		cancel()
		if err == nil {
			return res.(applyResult), nil
		}
		lastErr = err
		if !errors.Is(err, paxos.ErrNotLeader) {
			return applyResult{}, err
		}
		time.Sleep(10 * time.Millisecond)
	}
	return applyResult{}, fmt.Errorf("propose: %v", lastErr)
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

func TestReplicatedPrepareAndCommitConvergeOnEveryReplica(t *testing.T) {
	c := newBankCluster(t, 3, 100)
	c.awaitLeader()

	key := transactionKey("txn1", opDebit)
	if _, err := c.propose(command{Kind: cmdPrepare, Key: key, Op: opDebit, AccountID: "SENDER", Counterparty: "RECEIVER", Amount: 40}); err != nil {
		t.Fatalf("prepare: %v", err)
	}
	result, err := c.propose(command{Kind: cmdCommit, Key: key, Op: opDebit, AccountID: "SENDER", Counterparty: "RECEIVER", Amount: 40})
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	if result.AlreadySettled {
		t.Error("first commit reported the transaction as already settled")
	}

	waitFor(t, "every replica to converge on balance 60", 2*time.Second, func() bool {
		for _, r := range c.replicas {
			b, err := r.store.Balance("SENDER")
			if err != nil || b != 60 {
				return false
			}
		}
		return true
	})
}

// TestBankGroupSurvivesLeaderCrash is the regression test for the whole point
// of replicating a bank: a commit that has been applied is not lost just
// because the replica that led it crashes immediately afterwards.
func TestBankGroupSurvivesLeaderCrash(t *testing.T) {
	c := newBankCluster(t, 3, 100)
	leader := c.awaitLeader()

	key := transactionKey("txn1", opDebit)
	if _, err := c.propose(command{Kind: cmdPrepare, Key: key, Op: opDebit, AccountID: "SENDER", Counterparty: "RECEIVER", Amount: 40}); err != nil {
		t.Fatalf("prepare: %v", err)
	}
	if _, err := c.propose(command{Kind: cmdCommit, Key: key, Op: opDebit, AccountID: "SENDER", Counterparty: "RECEIVER", Amount: 40}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	waitFor(t, "every replica to converge on balance 60 before the crash", 2*time.Second, func() bool {
		for _, r := range c.replicas {
			b, err := r.store.Balance("SENDER")
			if err != nil || b != 60 {
				return false
			}
		}
		return true
	})

	c.kill(leader)
	newLeader := c.awaitLeader()
	if newLeader == leader {
		t.Fatalf("the killed replica %s is still being counted as leader", leader)
	}

	b, err := c.replicas[newLeader].store.Balance("SENDER")
	if err != nil {
		t.Fatalf("Balance on new leader %s: %v", newLeader, err)
	}
	if b != 60 {
		t.Errorf("new leader %s has balance %v, want 60; the crash lost a committed transfer", newLeader, b)
	}
}

// TestReplicatedCommitIsIdempotentAcrossTheGroup covers the coordinator
// retrying a commit whose response it never saw -- the same scenario
// TestRetriedCommitIsIdempotent in twopc_test.go covers for a single
// participant, now through the replicated path.
func TestReplicatedCommitIsIdempotentAcrossTheGroup(t *testing.T) {
	c := newBankCluster(t, 3, 100)
	c.awaitLeader()

	key := transactionKey("txn1", opDebit)
	if _, err := c.propose(command{Kind: cmdPrepare, Key: key, Op: opDebit, AccountID: "SENDER", Counterparty: "RECEIVER", Amount: 40}); err != nil {
		t.Fatalf("prepare: %v", err)
	}
	if _, err := c.propose(command{Kind: cmdCommit, Key: key, Op: opDebit, AccountID: "SENDER", Counterparty: "RECEIVER", Amount: 40}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	result, err := c.propose(command{Kind: cmdCommit, Key: key, Op: opDebit, AccountID: "SENDER", Counterparty: "RECEIVER", Amount: 40})
	if err != nil {
		t.Fatalf("retried commit: %v", err)
	}
	if !result.AlreadySettled {
		t.Error("retried commit should report the transaction as already settled")
	}

	waitFor(t, "balance to stay at 60 after the retry", 2*time.Second, func() bool {
		b, err := c.replicas[c.awaitLeader()].store.Balance("SENDER")
		return err == nil && b == 60
	})
}
