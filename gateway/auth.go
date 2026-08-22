package main

import (
	"crypto/rand"
	"encoding/csv"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"golang.org/x/crypto/bcrypt"
)

// tokenTTL bounds how long a session token stays valid. Tokens used to live
// forever, which meant a single leaked token was a permanent credential.
const tokenTTL = 30 * time.Minute

var errBadCredentials = errors.New("invalid username or password")

// dummyHash is compared against when the username is unknown, so that a
// failed lookup costs the same as a wrong password.
var dummyHash = func() string {
	h, err := bcrypt.GenerateFromPassword([]byte("not-a-real-password"), bcrypt.DefaultCost)
	if err != nil {
		panic(fmt.Sprintf("bcrypt is unusable: %v", err))
	}
	return string(h)
}()

// identity is what the gateway knows about a registered user. The account and
// bank are recorded at registration time so that the gateway can later check
// that a caller is moving money out of their own account rather than someone
// else's.
type identity struct {
	Username     string
	PasswordHash string
	AccountID    string
	Bank         string
}

// session is an issued token together with the identity it authenticates.
type session struct {
	username  string
	expiresAt time.Time
}

// authStore holds registered users and live sessions.
//
// Passwords are stored as bcrypt hashes. Rows written by older builds of this
// project contain plaintext, so they are re-hashed the first time the file is
// loaded and written back -- the file never keeps a readable password after a
// single start-up.
type authStore struct {
	path string

	mu       sync.RWMutex
	users    map[string]identity
	sessions map[string]session
}

// gatewayUsersHeader is the first row of gateway_users.txt.
var gatewayUsersHeader = []string{"username", "password_hash", "account_id", "bank_name"}

func newAuthStore(dataDir string) (*authStore, error) {
	s := &authStore{
		path:     filepath.Join(dataDir, "gateway_users.txt"),
		users:    make(map[string]identity),
		sessions: make(map[string]session),
	}
	if err := s.load(); err != nil {
		return nil, err
	}
	go s.expireSessions()
	return s, nil
}

// errAccountClaimed is returned when a caller tries to register against an
// account that already belongs to somebody else.
var errAccountClaimed = errors.New("account is already registered to another user")

// register adds or updates a user and persists the change.
//
// An account id may only ever be claimed by one username. Without that rule
// the ownership check in requireOwnership would be bypassable: anyone could
// re-register themselves against a victim's account id and then transfer out
// of it perfectly legitimately.
func (s *authStore) register(username, password, accountID, bank string) error {
	if username == "" || password == "" {
		return fmt.Errorf("username and password are required")
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return fmt.Errorf("hash password: %w", err)
	}

	s.mu.Lock()
	for _, existing := range s.users {
		if existing.AccountID == accountID && existing.Username != username {
			s.mu.Unlock()
			return errAccountClaimed
		}
	}
	s.users[username] = identity{
		Username:     username,
		PasswordHash: string(hash),
		AccountID:    accountID,
		Bank:         bank,
	}
	s.mu.Unlock()

	return s.save()
}

// authenticate verifies a password and issues a session token.
func (s *authStore) authenticate(username, password string) (string, error) {
	s.mu.RLock()
	user, ok := s.users[username]
	s.mu.RUnlock()

	// Always run a bcrypt comparison, even for an unknown user, so that the
	// response time does not reveal which usernames exist.
	hash := user.PasswordHash
	if !ok {
		hash = dummyHash
	}
	if err := bcrypt.CompareHashAndPassword([]byte(hash), []byte(password)); err != nil || !ok {
		return "", errBadCredentials
	}

	token, err := newToken()
	if err != nil {
		return "", err
	}
	s.mu.Lock()
	s.sessions[token] = session{username: username, expiresAt: time.Now().Add(tokenTTL)}
	s.mu.Unlock()
	return token, nil
}

// lookupSession resolves a token to the identity that owns it. It returns
// false for unknown or expired tokens.
func (s *authStore) lookupSession(token string) (identity, bool) {
	s.mu.RLock()
	sess, ok := s.sessions[token]
	user, userOK := s.users[sess.username]
	s.mu.RUnlock()

	if !ok || !userOK || time.Now().After(sess.expiresAt) {
		return identity{}, false
	}
	return user, true
}

// expireSessions periodically drops tokens that have aged out, so the map
// does not grow without bound in a long-running gateway.
func (s *authStore) expireSessions() {
	for range time.Tick(tokenTTL) {
		now := time.Now()
		s.mu.Lock()
		for token, sess := range s.sessions {
			if now.After(sess.expiresAt) {
				delete(s.sessions, token)
			}
		}
		s.mu.Unlock()
	}
}

// count returns the number of registered users, for start-up logging.
// Note that neither passwords nor hashes are ever logged.
func (s *authStore) count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.users)
}

// --- persistence ---

func (s *authStore) load() error {
	rows, err := s.readRows()
	if err != nil {
		return err
	}

	upgraded := 0
	for i, row := range rows {
		if i == 0 && len(row) > 0 && row[0] == "username" {
			continue // header
		}
		if len(row) < 2 || row[0] == "" {
			continue
		}
		user := identity{Username: row[0], PasswordHash: row[1]}
		if len(row) > 2 {
			user.AccountID = row[2]
		}
		if len(row) > 3 {
			user.Bank = row[3]
		}

		// Rows from older builds stored the password in the clear.
		if !strings.HasPrefix(user.PasswordHash, "$2") {
			hash, err := bcrypt.GenerateFromPassword([]byte(user.PasswordHash), bcrypt.DefaultCost)
			if err != nil {
				return fmt.Errorf("upgrade password for %s: %w", user.Username, err)
			}
			user.PasswordHash = string(hash)
			upgraded++
		}
		s.users[user.Username] = user
	}

	log.Printf("[auth] loaded %d user(s) from %s", len(s.users), s.path)
	if upgraded > 0 {
		log.Printf("[auth] re-hashed %d plaintext password(s) from an older format", upgraded)
		// save() replaces the file by renaming over it, which Windows refuses
		// while any handle to it is still open -- hence readRows() below,
		// which closes the file before returning rather than deferring it to
		// the end of this function.
		return s.save()
	}
	return nil
}

// readRows reads and closes the users file, returning an empty result (and
// creating the file) when it does not exist yet.
func (s *authStore) readRows() ([][]string, error) {
	f, err := os.Open(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, s.save()
	}
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", s.path, err)
	}
	defer f.Close()

	reader := csv.NewReader(f)
	reader.FieldsPerRecord = -1 // legacy rows have fewer columns
	rows, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", s.path, err)
	}
	return rows, nil
}

func (s *authStore) save() error {
	s.mu.RLock()
	rows := make([][]string, 0, len(s.users)+1)
	rows = append(rows, gatewayUsersHeader)
	for _, u := range s.users {
		rows = append(rows, []string{u.Username, u.PasswordHash, u.AccountID, u.Bank})
	}
	s.mu.RUnlock()

	tmp, err := os.CreateTemp(filepath.Dir(s.path), filepath.Base(s.path)+".tmp*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())

	w := csv.NewWriter(tmp)
	if err := w.WriteAll(rows); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), s.path)
}

// newToken returns a cryptographically random opaque session token. The old
// implementation used the wall-clock nanosecond count, which is guessable.
func newToken() (string, error) {
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("generate token: %w", err)
	}
	return hex.EncodeToString(buf), nil
}
