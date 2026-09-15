// Package twopc coordinates a transfer across two shards with two-phase
// commit.
//
// Each participant is a whole Paxos group, so a vote is not one machine's
// opinion: a YES means the prepare was chosen by a majority of that shard's
// replicas and survives the leader that proposed it crashing. The coordinator
// follows the same two rules as gateway/transfer.go:
//
//  1. PREPARE locks, it does not just check. A YES guarantees the commit
//     can apply.
//  2. Once every participant has voted YES the decision is COMMIT, and it is
//     final. A commit that cannot be delivered is retried, never reversed.
package twopc

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"paxos-2pc-kvstore/internal/logstream"
)

// Participant is one shard's side of a transaction.
type Participant interface {
	// Name is the shard's display name, e.g. "s0".
	Name() string
	// Prepare locks key for a signed delta. A nil error is a YES vote.
	Prepare(ctx context.Context, tx, key string, delta int64) error
	// Commit applies a prepared transaction. It must be idempotent.
	Commit(ctx context.Context, tx string) error
	// Abort releases a transaction. It must be idempotent and must succeed
	// for a transaction that never prepared.
	Abort(ctx context.Context, tx string) error
}

// ErrInDoubt means the decision was COMMIT but at least one participant could
// not be told yet. Its account stays locked until a background retry lands.
var ErrInDoubt = errors.New("in doubt")

// Coordinator runs transactions. It is safe for use by one caller at a time;
// background commit retries run concurrently with later transactions.
type Coordinator struct {
	log *logstream.Emitter
	seq atomic.Uint64

	// RetryEvery is the gap between attempts to deliver a decided commit.
	RetryEvery time.Duration

	bg     sync.WaitGroup
	stop   chan struct{}
	closed atomic.Bool
}

// New returns a coordinator emitting under the [2PC] tag.
func New(sink logstream.Sink) *Coordinator {
	return &Coordinator{
		log:        logstream.NewEmitter(sink, "[2PC]"),
		RetryEvery: 400 * time.Millisecond,
		stop:       make(chan struct{}),
	}
}

// leg is one participant's half of a transfer.
type leg struct {
	p     Participant
	key   string
	delta int64
}

// Transfer moves amount from fromKey on one shard to toKey on another.
func (c *Coordinator) Transfer(ctx context.Context, from Participant, fromKey string, to Participant, toKey string, amount int64) error {
	tx := fmt.Sprintf("tx%d", c.seq.Add(1))
	legs := []leg{
		{p: from, key: fromKey, delta: -amount},
		{p: to, key: toKey, delta: amount},
	}

	c.log.TwoPC(fmt.Sprintf("BEGIN %s: %s@%s -> %s@%s amount %d", tx, fromKey, from.Name(), toKey, to.Name(), amount))

	// Phase 1. Prepares go out one shard at a time so each shard's Paxos round
	// reads as its own block in the log. A real coordinator would send them in
	// parallel; the protocol is the same, and a NO lets this one skip the rest.
	var refusal error
	prepared := 0
	for _, l := range legs {
		verb := "credit"
		if l.delta < 0 {
			verb = "debit"
		}
		if refusal != nil {
			c.log.TwoPC(fmt.Sprintf("PREPARE %s skipped: a participant already voted NO", l.p.Name()))
			continue
		}
		c.log.TwoPC(fmt.Sprintf("PREPARE %s -> %s: %s %s %d", tx, l.p.Name(), verb, l.key, abs(l.delta)))

		if err := l.p.Prepare(ctx, tx, l.key, l.delta); err != nil {
			c.log.WithTag("[" + l.p.Name() + "]").Error("vote NO: " + err.Error())
			refusal = fmt.Errorf("%s voted NO: %w", l.p.Name(), err)
			continue
		}
		prepared++
		c.log.WithTag("[" + l.p.Name() + "]").TwoPC(fmt.Sprintf("vote YES: %s locked for %s", l.key, tx))
	}

	if refusal != nil {
		c.log.TwoPC(fmt.Sprintf("decision ABORT %s (%d/%d yes)", tx, prepared, len(legs)))
		// Abort goes to every participant, voted or not: one whose YES was
		// chosen but whose reply was lost still holds a lock.
		for _, l := range legs {
			if err := l.p.Abort(ctx, tx); err != nil {
				c.log.WithTag("[" + l.p.Name() + "]").Error("abort not delivered, lock may linger: " + err.Error())
				continue
			}
			c.log.TwoPC("ABORT -> " + l.p.Name() + " released")
		}
		return refusal
	}

	// Phase 2. From here the answer is COMMIT no matter what.
	c.log.TwoPC(fmt.Sprintf("decision COMMIT %s (%d/%d yes)", tx, prepared, len(legs)))

	var undelivered []Participant
	for _, l := range legs {
		if err := l.p.Commit(ctx, tx); err != nil {
			c.log.WithTag("[" + l.p.Name() + "]").Error("commit not delivered: " + err.Error())
			undelivered = append(undelivered, l.p)
			continue
		}
		c.log.TwoPC(fmt.Sprintf("COMMIT -> %s applied", l.p.Name()))
	}

	if len(undelivered) > 0 {
		names := ""
		for i, p := range undelivered {
			if i > 0 {
				names += ", "
			}
			names += p.Name()
			c.retryInBackground(tx, p)
		}
		c.log.TwoPC(fmt.Sprintf("%s is decided COMMIT; retrying %s in the background until it lands", tx, names))
		return fmt.Errorf("%w: %s committed, but %s has not applied it yet (account stays locked until it does)", ErrInDoubt, tx, names)
	}
	c.log.TwoPC(fmt.Sprintf("END %s committed on %s and %s", tx, from.Name(), to.Name()))
	return nil
}

// retryInBackground keeps delivering a decided commit until it lands or the
// coordinator is closed.
func (c *Coordinator) retryInBackground(tx string, p Participant) {
	if c.closed.Load() {
		return
	}
	c.bg.Add(1)
	go func() {
		defer c.bg.Done()
		t := time.NewTicker(c.RetryEvery)
		defer t.Stop()
		for {
			select {
			case <-c.stop:
				return
			case <-t.C:
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			err := p.Commit(ctx, tx)
			cancel()
			if err == nil {
				c.log.TwoPC(fmt.Sprintf("COMMIT -> %s applied on retry; %s is settled", p.Name(), tx))
				return
			}
		}
	}()
}

// Close stops background retries.
func (c *Coordinator) Close() {
	if c.closed.Swap(true) {
		return
	}
	close(c.stop)
	c.bg.Wait()
}

func abs(v int64) int64 {
	if v < 0 {
		return -v
	}
	return v
}
