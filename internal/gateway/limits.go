package gateway

import (
	"sync"
	"time"
)

// Limits on a public, free-tier endpoint. Without them, an unbounded put loop
// from anyone who finds the URL is free compute.
const (
	commandsPerSecond = 10
	commandBurst      = 20

	// maxFrameBytes bounds one inbound message. Commands are a line of text.
	maxFrameBytes = 4 << 10

	// idleTimeout closes a connection nobody is typing into, which also
	// releases its cluster.
	idleTimeout = 10 * time.Minute

	// maxSessions bounds concurrent clusters in one process. Each session owns
	// a full topology, so this is the real memory bound.
	maxSessions = 32

	// maxDistinctKeys bounds the state machine per session.
	maxDistinctKeys = 256
)

// rateLimiter is a token bucket refilling at rate tokens per second up to
// burst. It is intentionally tiny: adding a dependency for this would be the
// wrong trade in a project whose selling point is having few.
type rateLimiter struct {
	mu     sync.Mutex
	tokens float64
	burst  float64
	rate   float64
	last   time.Time
}

func newRateLimiter(rate, burst float64) *rateLimiter {
	return &rateLimiter{tokens: burst, burst: burst, rate: rate, last: time.Now()}
}

func (r *rateLimiter) allow() bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now()
	r.tokens += now.Sub(r.last).Seconds() * r.rate
	if r.tokens > r.burst {
		r.tokens = r.burst
	}
	r.last = now

	if r.tokens < 1 {
		return false
	}
	r.tokens--
	return true
}
