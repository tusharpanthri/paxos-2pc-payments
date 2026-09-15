package gateway

import (
	"paxos-2pc-kvstore/internal/cluster"
	"paxos-2pc-kvstore/internal/logstream"
)

// Wire frames. Field names and the closed set of levels are fixed by
// docs/control-plane.md, and the frontend is written against them.

// inbound is one command from the browser.
type inbound struct {
	Cmd string `json:"cmd"`
}

// logFrame is one line of cluster activity.
type logFrame struct {
	Type  string `json:"type"`  // always "log"
	Level string `json:"level"` // one of logstream's seven levels
	Tag   string `json:"tag"`
	Msg   string `json:"msg"`

	// Set only for a message travelling between two participants, so the
	// frontend's map can animate it. Replica ids, or "2pc" for the coordinator.
	From string `json:"from,omitempty"`
	To   string `json:"to,omitempty"`
	Kind string `json:"kind,omitempty"`
}

// stateFrame is a snapshot of the cluster for the frontend's map. Like log
// frames, zero or more may precede a result.
type stateFrame struct {
	Type       string         `json:"type"` // always "state"
	Shards     []shardState   `json:"shards"`
	Partitions [][]string     `json:"partitions"`
	Accounts   []accountState `json:"accounts"`
}

type shardState struct {
	Shard  int         `json:"shard"`
	Leader string      `json:"leader"`
	Term   uint64      `json:"term"`
	Quorum int         `json:"quorum"`
	Nodes  []nodeState `json:"nodes"`
}

type nodeState struct {
	ID      string `json:"id"`
	Alive   bool   `json:"alive"`
	Role    string `json:"role"`
	Applied uint64 `json:"applied"`
}

type accountState struct {
	Key      string `json:"key"`
	Shard    int    `json:"shard"`
	Balance  int64  `json:"balance"`
	LockedBy string `json:"locked_by,omitempty"`
}

func newStateFrame(st cluster.Status) stateFrame {
	f := stateFrame{Type: "state", Partitions: st.Partitions, Shards: []shardState{}, Accounts: []accountState{}}
	if f.Partitions == nil {
		f.Partitions = [][]string{}
	}
	for _, sh := range st.Shards {
		ss := shardState{Shard: sh.Shard, Leader: sh.Leader, Term: sh.Term, Quorum: sh.Quorum}
		for _, n := range sh.Nodes {
			ss.Nodes = append(ss.Nodes, nodeState{ID: n.ID, Alive: n.Alive, Role: n.Role, Applied: n.Applied})
		}
		f.Shards = append(f.Shards, ss)
	}
	for _, a := range st.Accounts {
		f.Accounts = append(f.Accounts, accountState{Key: a.Key, Shard: a.Shard, Balance: a.Balance, LockedBy: a.LockedBy})
	}
	return f
}

// resultFrame terminates a command. Exactly one is sent per command, after any
// log frames, so the frontend knows when to re-enable the prompt.
type resultFrame struct {
	Type string `json:"type"` // always "result"
	Msg  string `json:"msg"`
}

func newLogFrame(e logstream.Event) logFrame {
	// A level outside the closed set would reach the frontend uncoloured.
	// Downgrading to info keeps the contract intact.
	level := e.Level
	if !logstream.Valid(level) {
		level = logstream.Info
	}
	return logFrame{Type: "log", Level: string(level), Tag: e.Tag, Msg: e.Msg, From: e.From, To: e.To, Kind: e.Kind}
}

func newResultFrame(msg string) resultFrame {
	return resultFrame{Type: "result", Msg: msg}
}
