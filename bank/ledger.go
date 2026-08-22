package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// ledger is the append-only record of every debit and credit this bank has
// committed. It doubles as the idempotency index: a transaction key that is
// already in the ledger has been applied, and applying it again must be a
// no-op rather than a second movement of money.
//
// The in-memory set is the fast path; the file is the source of truth and is
// replayed into the set at start-up so that a restart does not forget which
// transfers were already settled.
type ledger struct {
	mu      sync.Mutex
	path    string
	applied map[string]bool
}

// openLedger loads (or creates) the transaction log for bankName under dir.
func openLedger(dir, bankName string) (*ledger, error) {
	l := &ledger{
		path:    filepath.Join(dir, fmt.Sprintf("%s_transactions.txt", bankName)),
		applied: make(map[string]bool),
	}

	f, err := os.OpenFile(l.path, os.O_RDONLY|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open ledger %s: %w", l.path, err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		if key, _, ok := strings.Cut(line, ","); ok {
			l.applied[key] = true
		}
	}
	return l, scanner.Err()
}

// applied reports whether key has already been committed.
func (l *ledger) has(key string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.applied[key]
}

// record appends one settled operation. It is called only after the balance
// change has been persisted, so the ledger never claims more than happened.
func (l *ledger) record(key, from, to, operation string, amount float64) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	f, err := os.OpenFile(l.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()

	if _, err := fmt.Fprintf(f, "%s,%s,%s,%.2f,%s\n", key, from, to, amount, operation); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	l.applied[key] = true
	return nil
}

// count returns how many operations have been settled. Used for start-up logging.
func (l *ledger) count() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.applied)
}
