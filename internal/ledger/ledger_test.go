package ledger

import (
	"errors"
	"testing"
)

func apply(m *Machine, c Command) Result { return m.Apply(1, c.Encode()).(Result) }

func TestPrepareLocksAndCommitApplies(t *testing.T) {
	m := New(0)
	apply(m, Command{Op: OpPut, Key: "a", Amount: 100})

	if err := apply(m, Command{Op: OpPrepare, Tx: "tx1", Key: "a", Amount: -60}).Err(); err != nil {
		t.Fatal(err)
	}
	// While tx1 holds a, nothing else may spend or overwrite it.
	if err := apply(m, Command{Op: OpPrepare, Tx: "tx2", Key: "a", Amount: -60}).Err(); !errors.Is(err, ErrLocked) {
		t.Fatalf("second prepare: got %v, want locked", err)
	}
	if err := apply(m, Command{Op: OpPut, Key: "a", Amount: 5}).Err(); !errors.Is(err, ErrLocked) {
		t.Fatalf("put on locked account: got %v, want locked", err)
	}

	if err := apply(m, Command{Op: OpCommit, Tx: "tx1"}).Err(); err != nil {
		t.Fatal(err)
	}
	// A retried commit is a no-op, not a second debit.
	if err := apply(m, Command{Op: OpCommit, Tx: "tx1"}).Err(); err != nil {
		t.Fatal(err)
	}
	if got := apply(m, Command{Op: OpGet, Key: "a"}).Value; got != 40 {
		t.Fatalf("a = %d, want 40", got)
	}
}

func TestPrepareRefusesOverdraftAndAbortReleases(t *testing.T) {
	m := New(0)
	apply(m, Command{Op: OpPut, Key: "a", Amount: 10})

	if err := apply(m, Command{Op: OpPrepare, Tx: "tx1", Key: "a", Amount: -11}).Err(); !errors.Is(err, ErrInsufficientFunds) {
		t.Fatalf("got %v, want insufficient funds", err)
	}
	if err := apply(m, Command{Op: OpPrepare, Tx: "tx2", Key: "a", Amount: -10}).Err(); err != nil {
		t.Fatal(err)
	}
	apply(m, Command{Op: OpAbort, Tx: "tx2"})
	if err := apply(m, Command{Op: OpCommit, Tx: "tx2"}).Err(); !errors.Is(err, ErrUnknownTx) {
		t.Fatalf("commit after abort: got %v, want unknown tx", err)
	}
	if err := apply(m, Command{Op: OpTransfer, Key: "a", To: "a2", Amount: 1}).Err(); !errors.Is(err, ErrNoSuchKey) {
		t.Fatalf("transfer to missing account: got %v", err)
	}
	if got := m.Snapshot(); len(got) != 1 || got[0].Balance != 10 || got[0].LockedBy != "" {
		t.Fatalf("state after abort: %+v", got)
	}
}

func TestKeyLimitIsDeterministic(t *testing.T) {
	m := New(1)
	apply(m, Command{Op: OpPut, Key: "a", Amount: 1})
	if err := apply(m, Command{Op: OpPut, Key: "a", Amount: 2}).Err(); err != nil {
		t.Fatalf("updating an existing key must not count against the limit: %v", err)
	}
	if err := apply(m, Command{Op: OpPut, Key: "b", Amount: 1}).Err(); !errors.Is(err, ErrTooManyKeys) {
		t.Fatalf("got %v, want too many keys", err)
	}
}
