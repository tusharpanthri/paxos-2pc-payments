// Package ledger is the state machine each shard replicates: a map of account
// to integer balance, plus the lock table two-phase commit needs.
//
// It is the same design as the bank participant in bank/twopc.go -- PREPARE
// holds the funds, COMMIT and ABORT are idempotent -- rebuilt on integer
// minor units and with no disk, because the browser control plane creates a
// fresh cluster per connection. Everything here is applied through Paxos, so
// Apply must be deterministic: the same log applied to two replicas has to
// produce the same balances, locks and verdicts on both.
package ledger

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
)

// Op names one replicated operation.
type Op string

const (
	OpPut      Op = "put"      // set a balance, creating the account
	OpGet      Op = "get"      // read a balance through the log
	OpTransfer Op = "transfer" // move funds between two accounts on this shard
	OpPrepare  Op = "prepare"  // 2PC phase 1: lock an account for a delta
	OpCommit   Op = "commit"   // 2PC phase 2: apply the prepared delta
	OpAbort    Op = "abort"    // 2PC phase 2: release the lock
)

// Command is one entry in a shard's Paxos log.
type Command struct {
	Op     Op     `json:"op"`
	Key    string `json:"key,omitempty"`
	To     string `json:"to,omitempty"`
	Amount int64  `json:"amount,omitempty"` // value for put, amount for transfer, signed delta for prepare
	Tx     string `json:"tx,omitempty"`
}

// Encode serialises a command for the log.
func (c Command) Encode() []byte {
	raw, err := json.Marshal(c)
	if err != nil {
		panic(fmt.Sprintf("ledger: encode command: %v", err)) // plain fields; cannot fail
	}
	return raw
}

// Decode parses a log entry.
func Decode(raw []byte) (Command, error) {
	var c Command
	if err := json.Unmarshal(raw, &c); err != nil {
		return Command{}, fmt.Errorf("ledger: decode command: %w", err)
	}
	return c, nil
}

// String renders a command the way the terminal shows it.
func (c Command) String() string {
	switch c.Op {
	case OpPut:
		return fmt.Sprintf("put %s=%d", c.Key, c.Amount)
	case OpGet:
		return "get " + c.Key
	case OpTransfer:
		return fmt.Sprintf("transfer %s->%s %d", c.Key, c.To, c.Amount)
	case OpPrepare:
		return fmt.Sprintf("prepare %s %s%+d", c.Tx, c.Key, c.Amount)
	case OpCommit:
		return "commit " + c.Tx
	case OpAbort:
		return "abort " + c.Tx
	}
	return string(c.Op)
}

// Code is a deterministic verdict. It travels as a string rather than an
// error because the proposer receives it after a round trip through the log
// and, in the gRPC mode, through JSON.
type Code string

const (
	CodeOK           Code = ""
	CodeNoSuchKey    Code = "no_such_key"
	CodeInsufficient Code = "insufficient_funds"
	CodeLocked       Code = "locked"
	CodeTooManyKeys  Code = "too_many_keys"
	CodeUnknownTx    Code = "unknown_tx"
	CodeBadCommand   Code = "bad_command"
)

// Sentinel errors for each non-OK code, so callers can use errors.Is.
var (
	ErrNoSuchKey         = errors.New("no such key")
	ErrInsufficientFunds = errors.New("insufficient funds")
	ErrLocked            = errors.New("account is locked by an in-flight transaction")
	ErrTooManyKeys       = errors.New("too many distinct keys")
	ErrUnknownTx         = errors.New("unknown transaction")
	ErrBadCommand        = errors.New("bad command")
)

// Result is what Apply returns to the proposer.
type Result struct {
	Code  Code   `json:"code,omitempty"`
	Msg   string `json:"msg,omitempty"`
	Value int64  `json:"value,omitempty"`
	Slot  uint64 `json:"slot"`
}

// Err converts a non-OK result into an error wrapping the matching sentinel.
func (r Result) Err() error {
	var base error
	switch r.Code {
	case CodeOK:
		return nil
	case CodeNoSuchKey:
		base = ErrNoSuchKey
	case CodeInsufficient:
		base = ErrInsufficientFunds
	case CodeLocked:
		base = ErrLocked
	case CodeTooManyKeys:
		base = ErrTooManyKeys
	case CodeUnknownTx:
		base = ErrUnknownTx
	default:
		base = ErrBadCommand
	}
	if r.Msg == "" {
		return base
	}
	return fmt.Errorf("%w: %s", base, r.Msg)
}

// Machine is one replica's copy of a shard's accounts.
type Machine struct {
	maxKeys int

	mu       sync.Mutex
	balances map[string]int64
	locks    map[string]string  // account -> tx holding it
	prepared map[string]Command // tx -> its prepare
	decided  map[string]Op      // tx -> commit or abort, for idempotent retries
}

// New returns an empty machine. maxKeys bounds distinct accounts (0 means no
// bound); it must be identical on every replica, or they diverge on the put
// that crosses it.
func New(maxKeys int) *Machine {
	return &Machine{
		maxKeys:  maxKeys,
		balances: make(map[string]int64),
		locks:    make(map[string]string),
		prepared: make(map[string]Command),
		decided:  make(map[string]Op),
	}
}

// Apply implements paxos.StateMachine.
func (m *Machine) Apply(slot uint64, raw []byte) any {
	cmd, err := Decode(raw)
	if err != nil {
		return Result{Code: CodeBadCommand, Msg: err.Error(), Slot: slot}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	r := m.applyLocked(cmd)
	r.Slot = slot
	return r
}

func (m *Machine) applyLocked(c Command) Result {
	switch c.Op {
	case OpPut:
		if tx, held := m.locks[c.Key]; held {
			return Result{Code: CodeLocked, Msg: fmt.Sprintf("%s is held by %s", c.Key, tx)}
		}
		if _, exists := m.balances[c.Key]; !exists && m.maxKeys > 0 && len(m.balances) >= m.maxKeys {
			return Result{Code: CodeTooManyKeys, Msg: fmt.Sprintf("limit is %d per shard", m.maxKeys)}
		}
		m.balances[c.Key] = c.Amount
		return Result{Value: c.Amount}

	case OpGet:
		v, ok := m.balances[c.Key]
		if !ok {
			return Result{Code: CodeNoSuchKey, Msg: c.Key}
		}
		return Result{Value: v}

	case OpTransfer:
		if r, ok := m.checkLocked(c.Key, c.To); !ok {
			return r
		}
		from := m.balances[c.Key]
		if from < c.Amount {
			return Result{Code: CodeInsufficient, Msg: fmt.Sprintf("%s has %d, needs %d", c.Key, from, c.Amount)}
		}
		m.balances[c.Key] = from - c.Amount
		m.balances[c.To] += c.Amount
		return Result{Value: m.balances[c.Key]}

	case OpPrepare:
		return m.prepareLocked(c)

	case OpCommit:
		if m.decided[c.Tx] == OpCommit {
			return Result{} // a retried commit: already applied
		}
		p, ok := m.prepared[c.Tx]
		if !ok {
			return Result{Code: CodeUnknownTx, Msg: c.Tx + " was never prepared here or was aborted"}
		}
		m.balances[p.Key] += p.Amount
		delete(m.locks, p.Key)
		delete(m.prepared, c.Tx)
		m.decided[c.Tx] = OpCommit
		return Result{Value: m.balances[p.Key]}

	case OpAbort:
		// Aborting a transaction that never prepared here is not an error: the
		// coordinator may be cleaning up after a prepare that never arrived.
		if p, ok := m.prepared[c.Tx]; ok {
			delete(m.locks, p.Key)
			delete(m.prepared, c.Tx)
		}
		if m.decided[c.Tx] != OpCommit {
			m.decided[c.Tx] = OpAbort
		}
		return Result{}
	}
	return Result{Code: CodeBadCommand, Msg: fmt.Sprintf("unknown op %q", c.Op)}
}

// prepareLocked is a participant's vote. YES means the lock is held and the
// commit is guaranteed to apply: a debit is checked against the balance now,
// and the lock stops anything else spending those funds before the decision.
func (m *Machine) prepareLocked(c Command) Result {
	if existing, ok := m.prepared[c.Tx]; ok {
		if existing == c {
			return Result{} // a retried prepare
		}
		return Result{Code: CodeBadCommand, Msg: c.Tx + " already prepared with different details"}
	}
	if op, done := m.decided[c.Tx]; done {
		return Result{Code: CodeUnknownTx, Msg: fmt.Sprintf("%s was already decided (%s)", c.Tx, op)}
	}
	if r, ok := m.checkLocked(c.Key); !ok {
		return r
	}
	balance := m.balances[c.Key]
	if c.Amount < 0 && balance+c.Amount < 0 {
		return Result{Code: CodeInsufficient, Msg: fmt.Sprintf("%s has %d, needs %d", c.Key, balance, -c.Amount)}
	}
	m.locks[c.Key] = c.Tx
	m.prepared[c.Tx] = c
	return Result{Value: balance}
}

// checkLocked verifies every key exists and is free.
func (m *Machine) checkLocked(keys ...string) (Result, bool) {
	for _, k := range keys {
		if _, ok := m.balances[k]; !ok {
			return Result{Code: CodeNoSuchKey, Msg: k}, false
		}
		if tx, held := m.locks[k]; held {
			return Result{Code: CodeLocked, Msg: fmt.Sprintf("%s is held by %s", k, tx)}, false
		}
	}
	return Result{}, true
}

// Account is one row of a snapshot.
type Account struct {
	Key      string `json:"key"`
	Balance  int64  `json:"balance"`
	LockedBy string `json:"lockedBy,omitempty"`
}

// Snapshot returns every account in key order. It reads this replica's applied
// state and is not linearizable on its own; status uses it for display only.
func (m *Machine) Snapshot() []Account {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Account, 0, len(m.balances))
	for k, v := range m.balances {
		out = append(out, Account{Key: k, Balance: v, LockedBy: m.locks[k]})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out
}
