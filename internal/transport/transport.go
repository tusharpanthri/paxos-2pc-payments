// Package transport carries Paxos messages between replicas, and is the only
// place failure is injected.
//
// Both implementations satisfy paxos.Transport, the one-method interface the
// consensus engine already sends through. Kill, partition and heal live here
// rather than in the engine, so consensus never learns that a node was
// "killed": it sees a Send that fails, which is all a real replica ever sees.
// That is what lets the same fault injection drive goroutines in one process
// (inproc, the deployed default) and separate processes over gRPC.
package transport

import (
	"errors"

	"paxos-2pc-kvstore/internal/paxos"
)

// ErrUnreachable is returned when the fault layer refuses delivery. The
// engine treats it exactly like a timeout.
var ErrUnreachable = errors.New("unreachable")

// Trace records one completed Send, observed on the sending side. It exists so
// the control plane can narrate protocol traffic without the engine knowing
// anyone is watching.
type Trace struct {
	From  paxos.NodeID  `json:"from"`
	To    paxos.NodeID  `json:"to"`
	Req   paxos.Message `json:"req"`
	Reply paxos.Message `json:"reply"`
	Err   string        `json:"err,omitempty"`
}

// Observer receives traces. It is called from the sending goroutine and must
// not block.
type Observer func(Trace)

// slim drops payloads a trace never displays, so a trace buffered for the gRPC
// drain stays small.
func slim(m paxos.Message) paxos.Message {
	m.Entries = nil
	m.Command = nil
	return m
}

func newTrace(from, to paxos.NodeID, req, reply paxos.Message, err error) Trace {
	t := Trace{From: from, To: to, Req: slim(req), Reply: slim(reply)}
	if err != nil {
		t.Err = err.Error()
	}
	return t
}
