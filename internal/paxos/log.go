package paxos

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// Log is a replica's durable Paxos state: the entries it has accepted, and
// the highest ballot it has promised.
//
// Both have to survive a crash. If a replica forgot its promise it could
// accept an older ballot it had already ruled out; if it forgot an accepted
// entry it could tell a new leader that a slot was empty when that slot may
// already be committed elsewhere. Either would break the protocol, so both
// files are fsynced before the in-memory state is considered updated.
//
// The format is one JSON object per line, appended, last write for a slot
// wins on replay. It is deliberately readable -- you can tail -f a replica's
// log and watch consensus happen, which is worth more on a project like this
// than the speed of a binary encoding.
type Log struct {
	mu sync.Mutex

	dir       string
	entryF    *os.File
	statePath string

	entries  map[uint64]Entry
	maxSlot  uint64
	promised Ballot
}

// persistedState is the small sidecar file holding the promised ballot.
type persistedState struct {
	Promised Ballot `json:"promised"`
}

// OpenLog loads (or creates) the durable state for one replica under dir.
// Replaying the entry file rebuilds the accepted set; a later line for the
// same slot supersedes an earlier one, which is how a re-proposal at a
// higher ballot overwrites what was there.
func OpenLog(dir string, id NodeID) (*Log, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("paxos: create %s: %w", dir, err)
	}

	l := &Log{
		dir:       dir,
		statePath: filepath.Join(dir, fmt.Sprintf("%s.state", id)),
		entries:   make(map[uint64]Entry),
	}

	entryPath := filepath.Join(dir, fmt.Sprintf("%s.log", id))
	if err := l.replay(entryPath); err != nil {
		return nil, err
	}
	if err := l.loadState(); err != nil {
		return nil, err
	}

	f, err := os.OpenFile(entryPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("paxos: open log %s: %w", entryPath, err)
	}
	l.entryF = f
	return l, nil
}

// replay reads every accepted entry back into memory.
func (l *Log) replay(path string) error {
	f, err := os.OpenFile(path, os.O_RDONLY|os.O_CREATE, 0o644)
	if err != nil {
		return fmt.Errorf("paxos: open log %s: %w", path, err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)
	for line := 1; scanner.Scan(); line++ {
		raw := scanner.Bytes()
		if len(raw) == 0 {
			continue
		}
		var e Entry
		if err := json.Unmarshal(raw, &e); err != nil {
			// A torn final line is expected after a crash mid-append: the
			// entry was never acknowledged, so dropping it is safe.
			continue
		}
		l.entries[e.Slot] = e
		if e.Slot > l.maxSlot {
			l.maxSlot = e.Slot
		}
	}
	return scanner.Err()
}

// loadState reads the promised ballot, defaulting to zero on first start.
func (l *Log) loadState() error {
	raw, err := os.ReadFile(l.statePath)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("paxos: read state %s: %w", l.statePath, err)
	}
	var s persistedState
	if err := json.Unmarshal(raw, &s); err != nil {
		return fmt.Errorf("paxos: parse state %s: %w", l.statePath, err)
	}
	l.promised = s.Promised
	return nil
}

// Promised returns the highest ballot this replica has promised.
func (l *Log) Promised() Ballot {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.promised
}

// Promise durably records a new promised ballot. It is written via a
// temporary file and renamed, so a crash mid-write leaves the old value
// intact rather than a truncated one.
func (l *Log) Promise(b Ballot) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	raw, err := json.Marshal(persistedState{Promised: b})
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(l.dir, filepath.Base(l.statePath)+".tmp*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())

	if _, err := tmp.Write(raw); err != nil {
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
	if err := os.Rename(tmp.Name(), l.statePath); err != nil {
		return err
	}
	l.promised = b
	return nil
}

// Accept durably records an accepted entry, replacing any earlier entry for
// the same slot. It fsyncs before returning: the caller is about to tell the
// leader "I have this", and that claim has to survive a power cut.
func (l *Log) Accept(e Entry) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	raw, err := json.Marshal(e)
	if err != nil {
		return err
	}
	if _, err := l.entryF.Write(append(raw, '\n')); err != nil {
		return err
	}
	if err := l.entryF.Sync(); err != nil {
		return err
	}

	l.entries[e.Slot] = e
	if e.Slot > l.maxSlot {
		l.maxSlot = e.Slot
	}
	return nil
}

// Get returns the entry at a slot, if this replica has accepted one.
func (l *Log) Get(slot uint64) (Entry, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	e, ok := l.entries[slot]
	return e, ok
}

// MaxSlot is the highest slot this replica has accepted anything for.
func (l *Log) MaxSlot() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.maxSlot
}

// EntriesFrom returns every accepted entry at or above first, in slot order.
// It answers both promises and catch-up requests.
func (l *Log) EntriesFrom(first uint64) []Entry {
	l.mu.Lock()
	defer l.mu.Unlock()

	out := make([]Entry, 0, len(l.entries))
	for slot := first; slot <= l.maxSlot; slot++ {
		if e, ok := l.entries[slot]; ok {
			out = append(out, e)
		}
	}
	return out
}

// Close releases the entry file.
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.entryF == nil {
		return nil
	}
	err := l.entryF.Close()
	l.entryF = nil
	return err
}
