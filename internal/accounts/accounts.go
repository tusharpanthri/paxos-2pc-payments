// Package accounts implements the CSV-backed account store used by the bank
// servers.
//
// Each bank owns exactly one file, "<BankName>_users.txt", with the header
// row:
//
//	AccountId,username,password,bank_name,balance
//
// The store is deliberately simple -- this is a teaching project, not a
// database -- but it does guarantee two things a naive implementation does
// not: mutations are serialised through a mutex, and writes go to a
// temporary file that is renamed into place, so a crash mid-write cannot
// leave a half-written ledger behind.
package accounts

import (
	"encoding/csv"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
)

// ErrNotFound is returned when an account id is not present in the file.
var ErrNotFound = errors.New("account not found")

// Header is the first row of every bank users file.
var Header = []string{"AccountId", "username", "password", "bank_name", "balance"}

// Account is one row of the bank users file.
//
// Password is legacy: the bank never authenticates anyone, the gateway does.
// It is kept only so the on-disk format stays readable by older builds.
type Account struct {
	ID       string
	Username string
	Password string
	Bank     string
	Balance  float64
}

// Store is a mutex-guarded view over a single bank's CSV file.
type Store struct {
	mu   sync.Mutex
	path string
}

// NewStore returns a store for bankName rooted at dir. The file is created
// with just a header row if it does not exist yet.
func NewStore(dir, bankName string) (*Store, error) {
	s := &Store{path: filepath.Join(dir, fmt.Sprintf("%s_users.txt", bankName))}
	if _, err := os.Stat(s.path); errors.Is(err, os.ErrNotExist) {
		if err := s.write(nil); err != nil {
			return nil, fmt.Errorf("create %s: %w", s.path, err)
		}
	}
	return s, nil
}

// Path is the file this store reads and writes. Useful in log messages.
func (s *Store) Path() string { return s.path }

// List returns every account in the file.
func (s *Store) List() ([]Account, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.read()
}

// Balance returns the balance of one account.
func (s *Store) Balance(id string) (float64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	all, err := s.read()
	if err != nil {
		return 0, err
	}
	for _, a := range all {
		if a.ID == id {
			return a.Balance, nil
		}
	}
	return 0, ErrNotFound
}

// Exists reports whether an account id is known to this bank.
func (s *Store) Exists(id string) (bool, error) {
	_, err := s.Balance(id)
	switch {
	case err == nil:
		return true, nil
	case errors.Is(err, ErrNotFound):
		return false, nil
	default:
		return false, err
	}
}

// AddDelta applies delta to an account's balance and persists the result.
// A negative delta debits. The read, the modification and the write happen
// under one lock, so two concurrent transfers cannot lose an update.
func (s *Store) AddDelta(id string, delta float64) (float64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	all, err := s.read()
	if err != nil {
		return 0, err
	}
	for i := range all {
		if all[i].ID != id {
			continue
		}
		all[i].Balance += delta
		if err := s.write(all); err != nil {
			return 0, err
		}
		return all[i].Balance, nil
	}
	return 0, ErrNotFound
}

// Create adds a new account. It is an error if the id already exists with
// different credentials; re-registering with identical credentials is a
// no-op so that restarting a client is harmless.
func (s *Store) Create(a Account) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	all, err := s.read()
	if err != nil {
		return err
	}
	for _, existing := range all {
		if existing.ID != a.ID {
			continue
		}
		if existing.Username == a.Username && existing.Password == a.Password {
			return nil
		}
		return fmt.Errorf("account %s already registered to a different user", a.ID)
	}
	return s.write(append(all, a))
}

// read parses the CSV file. The caller must hold s.mu.
func (s *Store) read() ([]Account, error) {
	f, err := os.Open(s.path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	rows, err := csv.NewReader(f).ReadAll()
	if err != nil {
		return nil, err
	}

	accounts := make([]Account, 0, len(rows))
	for i, row := range rows {
		if i == 0 || len(row) < len(Header) {
			continue // header, or a truncated line we cannot trust
		}
		balance, err := strconv.ParseFloat(row[4], 64)
		if err != nil {
			return nil, fmt.Errorf("%s line %d: bad balance %q: %w", s.path, i+1, row[4], err)
		}
		accounts = append(accounts, Account{
			ID:       row[0],
			Username: row[1],
			Password: row[2],
			Bank:     row[3],
			Balance:  balance,
		})
	}
	return accounts, nil
}

// write replaces the file atomically. The caller must hold s.mu.
func (s *Store) write(accounts []Account) error {
	rows := make([][]string, 0, len(accounts)+1)
	rows = append(rows, Header)
	for _, a := range accounts {
		rows = append(rows, []string{
			a.ID, a.Username, a.Password, a.Bank, strconv.FormatFloat(a.Balance, 'f', 2, 64),
		})
	}

	tmp, err := os.CreateTemp(filepath.Dir(s.path), filepath.Base(s.path)+".tmp*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name()) // no-op once the rename below succeeds

	w := csv.NewWriter(tmp)
	if err := w.WriteAll(rows); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), s.path)
}
