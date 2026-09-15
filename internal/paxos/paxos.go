// Package paxos implements Multi-Paxos: leader election plus replication of
// an ordered command log across a group of replicas.
//
// The shape of the protocol is the classic one. A replica that wants to lead
// picks a ballot number and runs PREPARE against the group (phase 1). A
// majority of promises makes it leader, and -- this is the "multi" part --
// it then stays leader, committing further commands with ACCEPT alone
// (phase 2) until it crashes or is outvoted. Skipping phase 1 per command is
// the whole reason Multi-Paxos is usable for a real log.
//
// Safety rests on three rules, and every awkward corner of node.go exists to
// serve one of them:
//
//  1. A replica never accepts a message from a ballot lower than the highest
//     it has promised. This is what stops a partitioned old leader from
//     committing behind the group's back.
//
//  2. A promise carries back every entry the replica has already accepted.
//     A new leader must adopt those before proposing anything of its own,
//     because any one of them might already be committed somewhere.
//
//  3. A command is applied to the state machine only after a majority has
//     accepted it, and only in slot order. Two replicas that apply the same
//     log in the same order end up in the same state -- which is what makes
//     the replicated store linearizable.
package paxos

import (
	"context"
	"errors"
	"fmt"
)

// NodeID names one replica within a group, e.g. "ICICI-2".
type NodeID string

// ErrNotLeader is returned by Propose on a replica that is not currently the
// leader. Callers should retry against Leader(), which carries the hint.
var ErrNotLeader = errors.New("paxos: not the leader")

// ErrNoQuorum is returned when a round could not reach a majority, usually
// because too many replicas are down.
var ErrNoQuorum = errors.New("paxos: no quorum")

// Ballot is a proposal number. Ballots are totally ordered by (Num, Node),
// with the node id breaking ties so that two replicas that pick the same
// number never appear equal -- without that tiebreak, two candidates could
// each believe they had won the same ballot.
type Ballot struct {
	Num  uint64 `json:"num"`
	Node NodeID `json:"node"`
}

// Compare returns -1, 0 or 1 as b sorts before, equal to, or after other.
func (b Ballot) Compare(other Ballot) int {
	switch {
	case b.Num < other.Num:
		return -1
	case b.Num > other.Num:
		return 1
	case b.Node < other.Node:
		return -1
	case b.Node > other.Node:
		return 1
	}
	return 0
}

func (b Ballot) Less(other Ballot) bool    { return b.Compare(other) < 0 }
func (b Ballot) AtLeast(other Ballot) bool { return b.Compare(other) >= 0 }
func (b Ballot) IsZero() bool              { return b.Num == 0 && b.Node == "" }
func (b Ballot) String() string            { return fmt.Sprintf("%d.%s", b.Num, b.Node) }

// Entry is one slot of the replicated log: the command, and the ballot under
// which this replica accepted it.
type Entry struct {
	Slot    uint64 `json:"slot"`
	Ballot  Ballot `json:"ballot"`
	Command []byte `json:"command"`
}

// IsNoop reports whether the entry is a filler written by a new leader to
// close a gap in the log. Gaps must be filled -- the state machine applies
// slots contiguously, so one hole would stall every slot behind it.
func (e Entry) IsNoop() bool { return len(e.Command) == 0 }

// StateMachine is the application being replicated. Apply is called exactly
// once per slot, in slot order, on every replica. It must be deterministic:
// the same command applied to the same state must produce the same result
// everywhere, or the replicas diverge.
//
// The value returned is handed back to whichever caller proposed the command
// on this replica, and ignored elsewhere.
type StateMachine interface {
	Apply(slot uint64, command []byte) any
}

// MessageKind discriminates the protocol messages below.
type MessageKind string

const (
	KindPrepare  MessageKind = "prepare"  // phase 1 request
	KindPromise  MessageKind = "promise"  // phase 1 response
	KindAccept   MessageKind = "accept"   // phase 2 request
	KindAccepted MessageKind = "accepted" // phase 2 response
	KindCommit   MessageKind = "commit"   // leader heartbeat + commit index
	KindCatchUp  MessageKind = "catchup"  // follower asks for entries it missed
	KindPing     MessageKind = "ping"     // reachability probe before campaigning
)

// Message is the single envelope for every protocol exchange. One struct
// rather than six keeps the transport interface to a single method, which
// matters because the transport is implemented twice: in memory for tests,
// over gRPC in production.
type Message struct {
	Kind MessageKind `json:"kind"`
	From NodeID      `json:"from"`

	// Ballot is the sender's ballot on requests, and the responder's
	// currently promised ballot on responses.
	Ballot Ballot `json:"ballot"`

	// OK is the verdict on a response: did the replica promise, or accept?
	OK bool `json:"ok"`

	// Slot is the log position for accept/accepted, and the first slot of
	// interest for prepare/catchup.
	Slot uint64 `json:"slot,omitempty"`

	// Command is the payload of an accept.
	Command []byte `json:"command,omitempty"`

	// Entries carries accepted entries back on a promise, and missing
	// entries back on a catch-up response.
	Entries []Entry `json:"entries,omitempty"`

	// CommitIndex is the highest contiguous slot the sender has committed.
	// It rides along on commit heartbeats and is how followers learn what is
	// safe to apply.
	CommitIndex uint64 `json:"commitIndex,omitempty"`
}

// Transport delivers a message to one peer and returns its reply. An error
// means the peer could not be reached; a reply with OK false means it was
// reached and refused.
type Transport interface {
	Send(ctx context.Context, to NodeID, msg Message) (Message, error)
}

// majority is the smallest number of replicas that constitutes a quorum.
// Two quorums of this size always intersect, which is the property every
// safety argument in this package leans on.
func majority(groupSize int) int { return groupSize/2 + 1 }
