package gateway

import "paxos-2pc-kvstore/internal/logstream"

// Wire frames. Field names and the closed set of levels are fixed by
// docs/control-plane.md — the frontend is written against them.

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
	return logFrame{Type: "log", Level: string(level), Tag: e.Tag, Msg: e.Msg}
}

func newResultFrame(msg string) resultFrame {
	return resultFrame{Type: "result", Msg: msg}
}
