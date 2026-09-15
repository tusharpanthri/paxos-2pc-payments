package cluster

import (
	"context"
	"fmt"
	"strings"

	"paxos-2pc-kvstore/internal/logstream"
	"paxos-2pc-kvstore/internal/paxos"
	"paxos-2pc-kvstore/internal/transport"
)

// onTrace turns one observed Send into log events. It is the transport's
// observer in inproc mode and is fed from drained node buffers in gRPC mode.
//
// Only the two phases a reader can follow are shown. Heartbeats, catch-up
// fetches and reachability pings run several times a second and would bury
// everything else.
func (e *Engine) onTrace(t transport.Trace) {
	tag := "[net]"
	if s, _, err := ParseNode(string(t.From)); err == nil {
		tag = ShardTag(s)
	}
	reason := strings.TrimPrefix(t.Err, transport.ErrUnreachable.Error()+": ")

	switch t.Req.Kind {
	case paxos.KindPrepare:
		if t.Err != "" {
			e.emit(logstream.Net, "[net]", fmt.Sprintf("drop PREPARE %s -> %s: %s", t.From, t.To, reason))
			return
		}
		e.emit(logstream.Paxos, tag, fmt.Sprintf("PREPARE ballot=%s %s -> %s", t.Req.Ballot, t.From, t.To))
		if t.Reply.OK {
			e.emit(logstream.Paxos, tag, fmt.Sprintf("PROMISE %s -> %s", t.To, t.From))
		} else {
			e.emit(logstream.Paxos, tag, fmt.Sprintf("REJECT %s -> %s: already promised %s", t.To, t.From, t.Reply.Ballot))
		}

	case paxos.KindAccept:
		if t.Err != "" {
			e.emit(logstream.Net, "[net]", fmt.Sprintf("drop ACCEPT slot=%d %s -> %s: %s", t.Req.Slot, t.From, t.To, reason))
			return
		}
		e.emit(logstream.Paxos, tag, fmt.Sprintf("ACCEPT slot=%d ballot=%s %s -> %s", t.Req.Slot, t.Req.Ballot, t.From, t.To))
		if t.Reply.OK {
			e.traceMu.Lock()
			if len(e.acks) > 4096 {
				e.acks = make(map[string]int) // unread entries from recovery rounds
			}
			e.acks[ackKey(string(t.From), t.Req.Slot)]++
			e.traceMu.Unlock()
			e.emit(logstream.Paxos, tag, fmt.Sprintf("ACCEPTED slot=%d %s -> %s", t.Req.Slot, t.To, t.From))
		} else {
			e.emit(logstream.Paxos, tag, fmt.Sprintf("REJECT slot=%d %s -> %s: promised %s", t.Req.Slot, t.To, t.From, t.Reply.Ballot))
		}
	}
}

func ackKey(node string, slot uint64) string { return fmt.Sprintf("%s/%d", node, slot) }

// takeAcks returns and forgets the ACCEPTED count for a slot.
func (e *Engine) takeAcks(node string, slot uint64) int {
	e.traceMu.Lock()
	defer e.traceMu.Unlock()
	k := ackKey(node, slot)
	n := e.acks[k]
	delete(e.acks, k)
	return n
}

// collectTraces pulls traces buffered in node processes. A no-op in inproc
// mode, where onTrace is called as each Send completes.
func (e *Engine) collectTraces(ctx context.Context) {
	if e.drain == nil {
		return
	}
	for _, t := range e.drain(ctx) {
		e.onTrace(t)
	}
}

// pollRoles compares every live replica's role with the last poll and narrates
// elections and step-downs. Changes on dead replicas are tracked silently: a
// killed leader that later notices it lost its majority is not news.
func (e *Engine) pollRoles(ctx context.Context) {
	e.roleMu.Lock()
	defer e.roleMu.Unlock()

	for s := range e.shards {
		for _, info := range e.infos(ctx, s) {
			prev, seen := e.roles[info.ID]
			e.roles[info.ID] = info.Role
			if !seen || prev == info.Role || e.faults.Dead(info.ID) {
				continue
			}
			tag := ShardTag(s)
			switch {
			case info.Role == paxos.Leader.String():
				e.emit(logstream.Leader, tag, fmt.Sprintf("%s elected leader, ballot %s (term %d)", info.ID, info.Ballot, info.Ballot.Num))
			case prev == paxos.Leader.String():
				e.emit(logstream.Leader, tag, fmt.Sprintf("%s stepped down", info.ID))
			}
		}
	}
}
