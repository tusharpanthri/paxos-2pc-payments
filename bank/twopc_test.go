package main

import (
	"path/filepath"
	"sync"
	"testing"

	"paxos-2pc-kvstore/internal/accounts"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// newTestParticipant builds a participant backed by temporary files, seeded
// with a sender holding openingBalance and an empty receiver.
func newTestParticipant(t *testing.T, openingBalance float64) *participant {
	t.Helper()
	dir := t.TempDir()

	store, err := accounts.NewStore(dir, "TestBank")
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	for id, balance := range map[string]float64{"SENDER": openingBalance, "RECEIVER": 0} {
		if err := store.Create(accounts.Account{ID: id, Username: id, Bank: "TestBank", Balance: balance}); err != nil {
			t.Fatalf("seed %s: %v", id, err)
		}
	}

	l, err := openLedger(dir, "TestBank")
	if err != nil {
		t.Fatalf("openLedger: %v", err)
	}
	return newParticipant(store, l)
}

func balance(t *testing.T, p *participant, id string) float64 {
	t.Helper()
	b, err := p.store.Balance(id)
	if err != nil {
		t.Fatalf("Balance(%s): %v", id, err)
	}
	return b
}

func wantCode(t *testing.T, err error, want codes.Code) {
	t.Helper()
	if got := status.Code(err); got != want {
		t.Fatalf("got code %v (%v), want %v", got, err, want)
	}
}

func TestDebitPrepareCommitMovesMoney(t *testing.T) {
	p := newTestParticipant(t, 100)

	if err := p.prepare("txn1-debit", opDebit, "SENDER", "RECEIVER", 40); err != nil {
		t.Fatalf("prepare: %v", err)
	}
	if b := balance(t, p, "SENDER"); b != 100 {
		t.Errorf("prepare moved money: balance = %v, want it untouched at 100", b)
	}

	settled, err := p.commit("txn1-debit", opDebit, "SENDER", "RECEIVER", 40)
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	if settled {
		t.Error("first commit reported the transaction as already settled")
	}
	if b := balance(t, p, "SENDER"); b != 60 {
		t.Errorf("balance after commit = %v, want 60", b)
	}
}

func TestPrepareRejectsInsufficientFunds(t *testing.T) {
	p := newTestParticipant(t, 30)

	err := p.prepare("txn1-debit", opDebit, "SENDER", "RECEIVER", 40)
	wantCode(t, err, codes.FailedPrecondition)
}

func TestPrepareRejectsSelfTransfer(t *testing.T) {
	p := newTestParticipant(t, 100)

	err := p.prepare("txn1-debit", opDebit, "SENDER", "SENDER", 10)
	wantCode(t, err, codes.InvalidArgument)
}

func TestPrepareRejectsUnknownAccount(t *testing.T) {
	p := newTestParticipant(t, 100)

	err := p.prepare("txn1-debit", opDebit, "GHOST", "RECEIVER", 10)
	wantCode(t, err, codes.NotFound)
}

// TestReservationsPreventOverdraft is the regression test for the original
// bug: PREPARE checked the balance but did not reserve it, so two transfers
// that each fit individually could both be prepared and both committed,
// leaving the account overdrawn.
func TestReservationsPreventOverdraft(t *testing.T) {
	p := newTestParticipant(t, 100)

	if err := p.prepare("txnA-debit", opDebit, "SENDER", "RECEIVER", 80); err != nil {
		t.Fatalf("first prepare: %v", err)
	}
	// Only 20 is still unreserved, so this must be refused even though the
	// stored balance is still 100.
	err := p.prepare("txnB-debit", opDebit, "SENDER", "RECEIVER", 80)
	wantCode(t, err, codes.FailedPrecondition)
}

// TestConcurrentPreparesCannotOverdraw runs the same scenario from many
// goroutines: whatever interleaving occurs, the reservations must never add
// up to more than the balance.
func TestConcurrentPreparesCannotOverdraw(t *testing.T) {
	p := newTestParticipant(t, 100)

	const attempts = 20
	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		accepted int
	)
	wg.Add(attempts)
	for i := 0; i < attempts; i++ {
		go func(i int) {
			defer wg.Done()
			key := transactionKey(string(rune('a'+i)), opDebit)
			if err := p.prepare(key, opDebit, "SENDER", "RECEIVER", 10); err == nil {
				mu.Lock()
				accepted++
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()

	if accepted != 10 {
		t.Errorf("%d prepares of 10 were accepted against a balance of 100, want exactly 10", accepted)
	}
}

func TestAbortReleasesTheReservation(t *testing.T) {
	p := newTestParticipant(t, 100)

	if err := p.prepare("txnA-debit", opDebit, "SENDER", "RECEIVER", 80); err != nil {
		t.Fatalf("prepare: %v", err)
	}
	p.abort("txnA-debit")

	if err := p.prepare("txnB-debit", opDebit, "SENDER", "RECEIVER", 80); err != nil {
		t.Errorf("prepare after abort: %v; the reservation was not released", err)
	}
}

// TestReplayedTransferIsAppliedOnce is the regression test for the broken
// idempotency path. Replaying a settled transaction used to fail at commit
// with "no matching prepared debit found"; it must now report success without
// moving money a second time.
func TestReplayedTransferIsAppliedOnce(t *testing.T) {
	p := newTestParticipant(t, 100)

	if err := p.prepare("txn1-debit", opDebit, "SENDER", "RECEIVER", 40); err != nil {
		t.Fatalf("prepare: %v", err)
	}
	if _, err := p.commit("txn1-debit", opDebit, "SENDER", "RECEIVER", 40); err != nil {
		t.Fatalf("commit: %v", err)
	}

	// The whole transfer arrives again, exactly as a client retry would send it.
	if err := p.prepare("txn1-debit", opDebit, "SENDER", "RECEIVER", 40); err != nil {
		t.Fatalf("replayed prepare: %v", err)
	}
	settled, err := p.commit("txn1-debit", opDebit, "SENDER", "RECEIVER", 40)
	if err != nil {
		t.Fatalf("replayed commit: %v", err)
	}
	if !settled {
		t.Error("replayed commit did not report the transaction as already settled")
	}
	if b := balance(t, p, "SENDER"); b != 60 {
		t.Errorf("balance = %v, want 60; the replay moved money twice", b)
	}
}

// TestRetriedCommitIsIdempotent covers the coordinator retrying a commit whose
// response it never saw.
func TestRetriedCommitIsIdempotent(t *testing.T) {
	p := newTestParticipant(t, 100)

	if err := p.prepare("txn1-debit", opDebit, "SENDER", "RECEIVER", 40); err != nil {
		t.Fatalf("prepare: %v", err)
	}
	if _, err := p.commit("txn1-debit", opDebit, "SENDER", "RECEIVER", 40); err != nil {
		t.Fatalf("commit: %v", err)
	}

	settled, err := p.commit("txn1-debit", opDebit, "SENDER", "RECEIVER", 40)
	if err != nil {
		t.Fatalf("retried commit: %v", err)
	}
	if !settled {
		t.Error("retried commit should report the transaction as already settled")
	}
	if b := balance(t, p, "SENDER"); b != 60 {
		t.Errorf("balance = %v, want 60; the retry debited twice", b)
	}
}

func TestCommitWithoutPrepareIsRejected(t *testing.T) {
	p := newTestParticipant(t, 100)

	_, err := p.commit("txn1-debit", opDebit, "SENDER", "RECEIVER", 40)
	wantCode(t, err, codes.Aborted)
}

func TestCommitMustMatchWhatWasPrepared(t *testing.T) {
	p := newTestParticipant(t, 100)

	if err := p.prepare("txn1-debit", opDebit, "SENDER", "RECEIVER", 40); err != nil {
		t.Fatalf("prepare: %v", err)
	}
	_, err := p.commit("txn1-debit", opDebit, "SENDER", "RECEIVER", 90)
	wantCode(t, err, codes.InvalidArgument)
}

func TestCreditCommitAddsFunds(t *testing.T) {
	p := newTestParticipant(t, 100)

	if err := p.prepare("txn1-credit", opCredit, "RECEIVER", "SENDER", 25); err != nil {
		t.Fatalf("prepare: %v", err)
	}
	if _, err := p.commit("txn1-credit", opCredit, "RECEIVER", "SENDER", 25); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if b := balance(t, p, "RECEIVER"); b != 25 {
		t.Errorf("receiver balance = %v, want 25", b)
	}
}

// TestLedgerSurvivesRestart proves the idempotency index is rebuilt from disk,
// so a restarted bank still recognises a transaction it already applied.
func TestLedgerSurvivesRestart(t *testing.T) {
	p := newTestParticipant(t, 100)

	if err := p.prepare("txn1-debit", opDebit, "SENDER", "RECEIVER", 40); err != nil {
		t.Fatalf("prepare: %v", err)
	}
	if _, err := p.commit("txn1-debit", opDebit, "SENDER", "RECEIVER", 40); err != nil {
		t.Fatalf("commit: %v", err)
	}

	reopened, err := openLedger(dirOf(p.ledger.path), "TestBank")
	if err != nil {
		t.Fatalf("reopen ledger: %v", err)
	}
	if !reopened.has("txn1-debit") {
		t.Error("a restarted bank forgot a transaction it had already applied")
	}
}

func TestTransactionKeySeparatesTheTwoLegs(t *testing.T) {
	if debit, credit := transactionKey("TXN-1", opDebit), transactionKey("TXN-1", opCredit); debit == credit {
		t.Fatal("the debit and credit legs of a transfer must have distinct ledger keys")
	}
	// The gateway sends the bare transaction id; the suffix is added here and
	// only here. Adding it on both sides is what produced "TXN-1-debit-debit"
	// in the ledger.
	if got, want := transactionKey("TXN-1", opDebit), "TXN-1-debit"; got != want {
		t.Errorf("transactionKey = %q, want %q", got, want)
	}
}

// dirOf returns the directory portion of a path.
func dirOf(path string) string {
	return filepath.Dir(path)
}
