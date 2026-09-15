package main

import (
	"encoding/json"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// cmdKind discriminates the three operations a bank replica group replicates.
type cmdKind string

const (
	cmdPrepare cmdKind = "prepare"
	cmdCommit  cmdKind = "commit"
	cmdAbort   cmdKind = "abort"
)

// command is one entry in the Paxos log: everything a replica needs to redo
// the same prepare/commit/abort call the leader made, in the same order.
type command struct {
	Kind         cmdKind   `json:"kind"`
	Key          string    `json:"key"`
	Op           operation `json:"op"`
	AccountID    string    `json:"accountId"`
	Counterparty string    `json:"counterparty"`
	Amount       float64   `json:"amount"`
}

func encodeCommand(c command) []byte {
	raw, err := json.Marshal(c)
	if err != nil {
		// c's fields are all plain strings and a float64; this cannot fail.
		panic(fmt.Sprintf("bank: encode command: %v", err))
	}
	return raw
}

func decodeCommand(raw []byte) (command, error) {
	var c command
	if err := json.Unmarshal(raw, &c); err != nil {
		return command{}, fmt.Errorf("decode command: %w", err)
	}
	return c, nil
}

// applyResult is what Apply hands back to whichever replica proposed the
// command. It carries a gRPC status rather than an error directly, because
// only the code and message survive a JSON round trip -- and every replica
// (not just the proposer) needs to reach the same verdict deterministically.
type applyResult struct {
	Code           uint32 `json:"code"`
	Message        string `json:"message"`
	AlreadySettled bool   `json:"alreadySettled"`
}

func (r applyResult) err() error {
	if r.Code == uint32(codes.OK) {
		return nil
	}
	return status.Error(codes.Code(r.Code), r.Message)
}

// bankStateMachine adapts the participant onto paxos.StateMachine. Apply is
// called exactly once per slot, in slot order, on every replica -- which is
// what keeps every replica's reservations, balances and ledger identical.
type bankStateMachine struct {
	p *participant
}

func newBankStateMachine(p *participant) *bankStateMachine {
	return &bankStateMachine{p: p}
}

func (m *bankStateMachine) Apply(_ uint64, raw []byte) any {
	cmd, err := decodeCommand(raw)
	if err != nil {
		return applyResult{Code: uint32(codes.Internal), Message: err.Error()}
	}

	switch cmd.Kind {
	case cmdPrepare:
		if err := m.p.prepare(cmd.Key, cmd.Op, cmd.AccountID, cmd.Counterparty, cmd.Amount); err != nil {
			st := status.Convert(err)
			return applyResult{Code: uint32(st.Code()), Message: st.Message()}
		}
		return applyResult{}

	case cmdCommit:
		settled, err := m.p.commit(cmd.Key, cmd.Op, cmd.AccountID, cmd.Counterparty, cmd.Amount)
		if err != nil {
			st := status.Convert(err)
			return applyResult{Code: uint32(st.Code()), Message: st.Message()}
		}
		return applyResult{AlreadySettled: settled}

	case cmdAbort:
		m.p.abort(cmd.Key)
		return applyResult{}

	default:
		return applyResult{Code: uint32(codes.Internal), Message: fmt.Sprintf("unknown command kind %q", cmd.Kind)}
	}
}
