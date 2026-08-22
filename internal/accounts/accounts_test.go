package accounts

import (
	"errors"
	"path/filepath"
	"sync"
	"testing"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	s, err := NewStore(t.TempDir(), "TestBank")
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	return s
}

func seed(t *testing.T, s *Store, id string, balance float64) {
	t.Helper()
	if err := s.Create(Account{ID: id, Username: id, Password: "pw", Bank: "TestBank", Balance: balance}); err != nil {
		t.Fatalf("Create(%s): %v", id, err)
	}
}

func TestCreateAndBalance(t *testing.T) {
	s := newTestStore(t)
	seed(t, s, "ACC1", 100)

	got, err := s.Balance("ACC1")
	if err != nil {
		t.Fatalf("Balance: %v", err)
	}
	if got != 100 {
		t.Errorf("Balance = %v, want 100", got)
	}
}

func TestBalanceUnknownAccount(t *testing.T) {
	s := newTestStore(t)
	if _, err := s.Balance("nope"); !errors.Is(err, ErrNotFound) {
		t.Errorf("Balance of an unknown account = %v, want ErrNotFound", err)
	}
}

func TestCreateIsIdempotentForIdenticalCredentials(t *testing.T) {
	s := newTestStore(t)
	seed(t, s, "ACC1", 100)

	// Re-registering the same user must not duplicate the row or reset the
	// balance, because clients call this on every start-up.
	if err := s.Create(Account{ID: "ACC1", Username: "ACC1", Password: "pw", Bank: "TestBank", Balance: 999}); err != nil {
		t.Fatalf("second Create: %v", err)
	}
	all, err := s.List()
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(all) != 1 {
		t.Fatalf("got %d accounts, want 1", len(all))
	}
	if all[0].Balance != 100 {
		t.Errorf("balance = %v, want the original 100", all[0].Balance)
	}
}

func TestCreateRejectsConflictingCredentials(t *testing.T) {
	s := newTestStore(t)
	seed(t, s, "ACC1", 100)

	err := s.Create(Account{ID: "ACC1", Username: "someone-else", Password: "pw", Bank: "TestBank"})
	if err == nil {
		t.Error("claiming an existing account with different credentials should fail")
	}
}

func TestAddDeltaPersists(t *testing.T) {
	s := newTestStore(t)
	seed(t, s, "ACC1", 100)

	if _, err := s.AddDelta("ACC1", -30); err != nil {
		t.Fatalf("AddDelta: %v", err)
	}

	// Re-open the same file to prove the change reached disk.
	reopened, err := NewStore(filepath.Dir(s.Path()), "TestBank")
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	got, err := reopened.Balance("ACC1")
	if err != nil {
		t.Fatalf("Balance: %v", err)
	}
	if got != 70 {
		t.Errorf("balance after debit = %v, want 70", got)
	}
}

// TestConcurrentAddDeltaDoesNotLoseUpdates is the regression test for the
// read-modify-write race: without the lock, concurrent updates would each read
// the same starting balance and the last writer would win.
func TestConcurrentAddDeltaDoesNotLoseUpdates(t *testing.T) {
	s := newTestStore(t)
	seed(t, s, "ACC1", 0)

	const goroutines = 20
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			if _, err := s.AddDelta("ACC1", 1); err != nil {
				t.Errorf("AddDelta: %v", err)
			}
		}()
	}
	wg.Wait()

	got, err := s.Balance("ACC1")
	if err != nil {
		t.Fatalf("Balance: %v", err)
	}
	if got != goroutines {
		t.Errorf("balance = %v, want %v; updates were lost", got, float64(goroutines))
	}
}
