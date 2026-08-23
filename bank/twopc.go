package main

import (
	"errors"
	"fmt"
	"sync"

	"paxos-2pc-kvstore/internal/accounts"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// operation is the half of a transfer this bank is responsible for.
type operation string

const (
	opDebit  operation = "debit"
	opCredit operation = "credit"
)

// reservation is the state a participant keeps between PREPARE and
// COMMIT/ABORT. Holding it is what lets the bank promise the coordinator
// that a later commit cannot fail for lack of funds.
type reservation struct {
	op           operation
	accountID    string
	counterparty string
	amount       float64

	// settled marks a reservation created for a transaction that was
	// already in the ledger when PREPARE arrived. Committing it changes no
	// balances -- it exists purely so a retried transfer walks the same
	// prepare/commit path and reports success instead of failing with
	// "no matching prepared transaction".
	settled bool
}

// participant implements this bank's side of two-phase commit.
//
// Two invariants do the real work here:
//
//  1. PREPARE reserves funds. The available balance for a debit is the
//     stored balance minus every outstanding debit reservation on that
//     account, so two concurrent transfers cannot both pass the funds check
//     and then both commit into an overdraft.
//
//  2. Every phase is idempotent. The ledger is consulted before doing
//     anything, so a coordinator that retries after a timeout gets the same
//     answer it would have got the first time.
type participant struct {
	store  *accounts.Store
	ledger *ledger

	mu       sync.Mutex
	prepared map[string]*reservation
}

func newParticipant(store *accounts.Store, l *ledger) *participant {
	return &participant{
		store:    store,
		ledger:   l,
		prepared: make(map[string]*reservation),
	}
}

// reservedLocked totals the outstanding debit reservations against an
// account. Callers must hold p.mu.
func (p *participant) reservedLocked(accountID string) float64 {
	var total float64
	for _, r := range p.prepared {
		if r.op == opDebit && r.accountID == accountID && !r.settled {
			total += r.amount
		}
	}
	return total
}

// prepare validates one half of a transfer and, if it can be honoured, holds
// a reservation for it.
func (p *participant) prepare(key string, op operation, accountID, counterparty string, amount float64) error {
	if accountID == counterparty {
		return status.Error(codes.InvalidArgument, "self-transfer is not allowed")
	}
	if amount <= 0 {
		return status.Errorf(codes.InvalidArgument, "amount must be positive, got %.2f", amount)
	}

	// A transaction already in the ledger has been applied. Record a settled
	// reservation so the matching commit succeeds as a no-op.
	if p.ledger.has(key) {
		p.mu.Lock()
		p.prepared[key] = &reservation{op: op, accountID: accountID, counterparty: counterparty, amount: amount, settled: true}
		p.mu.Unlock()
		return nil
	}

	balance, err := p.store.Balance(accountID)
	switch {
	case errors.Is(err, accounts.ErrNotFound):
		return status.Errorf(codes.NotFound, "account %s does not exist at this bank", accountID)
	case err != nil:
		return status.Errorf(codes.Internal, "read account %s: %v", accountID, err)
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	// Re-preparing the same key is safe: the coordinator may simply be
	// retrying a PREPARE whose response it never saw.
	if existing, ok := p.prepared[key]; ok {
		if existing.op != op || existing.accountID != accountID ||
			existing.counterparty != counterparty || existing.amount != amount {
			return status.Errorf(codes.AlreadyExists, "transaction %s is already prepared with different details", key)
		}
		return nil
	}

	if op == opDebit {
		if available := balance - p.reservedLocked(accountID); available < amount {
			return status.Errorf(codes.FailedPrecondition,
				"insufficient funds in %s: %.2f available, %.2f requested", accountID, available, amount)
		}
	}

	p.prepared[key] = &reservation{op: op, accountID: accountID, counterparty: counterparty, amount: amount}
	return nil
}

// commit applies a previously prepared reservation. It reports whether the
// transaction had already been settled by an earlier attempt.
func (p *participant) commit(key string, op operation, accountID, counterparty string, amount float64) (alreadySettled bool, err error) {
	// Commit is retried by the coordinator on timeout, so check the ledger
	// before touching the reservation table.
	if p.ledger.has(key) {
		p.mu.Lock()
		delete(p.prepared, key)
		p.mu.Unlock()
		return true, nil
	}

	p.mu.Lock()
	r, ok := p.prepared[key]
	if !ok {
		p.mu.Unlock()
		return false, status.Errorf(codes.Aborted, "no prepared transaction %s; it was never prepared or has been aborted", key)
	}
	if r.op != op || r.accountID != accountID || r.counterparty != counterparty || r.amount != amount {
		p.mu.Unlock()
		return false, status.Errorf(codes.InvalidArgument, "commit for %s does not match what was prepared", key)
	}
	settled := r.settled
	// Keep the reservation in place until the balance is written: releasing
	// it early would let a concurrent prepare spend funds this commit is
	// about to consume.
	p.mu.Unlock()

	if settled {
		p.release(key)
		return true, nil
	}

	delta := amount
	if op == opDebit {
		delta = -amount
	}
	if _, err := p.store.AddDelta(accountID, delta); err != nil {
		if errors.Is(err, accounts.ErrNotFound) {
			p.release(key)
			return false, status.Errorf(codes.NotFound, "account %s disappeared before commit", accountID)
		}
		return false, status.Errorf(codes.Internal, "update account %s: %v", accountID, err)
	}

	from, to := accountID, counterparty
	if op == opCredit {
		from, to = counterparty, accountID
	}
	if err := p.ledger.record(key, from, to, string(op), amount); err != nil {
		// The money moved but the ledger write failed. Say so loudly rather
		// than pretending the transfer did not happen.
		p.release(key)
		return false, status.Errorf(codes.DataLoss,
			"%s applied to %s but the ledger write failed: %v", op, accountID, err)
	}

	p.release(key)
	return false, nil
}

// abort drops a reservation, releasing any funds it was holding. Aborting an
// unknown key is not an error: the coordinator may be cleaning up after a
// prepare that never arrived.
func (p *participant) abort(key string) {
	p.release(key)
}

func (p *participant) release(key string) {
	p.mu.Lock()
	delete(p.prepared, key)
	p.mu.Unlock()
}

// pendingCount reports how many reservations are outstanding. Used for logging.
func (p *participant) pendingCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.prepared)
}

// transactionKey builds the ledger key for one half of a transfer. Each bank
// records its own side, so the debit and credit legs of a transfer need
// distinct keys.
func transactionKey(transactionID string, op operation) string {
	return fmt.Sprintf("%s-%s", transactionID, op)
}
