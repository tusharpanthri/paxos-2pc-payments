// Package logstream carries structured events from the cluster out to whoever
// is watching. It is deliberately tiny and depends on nothing: the consensus
// and transport packages emit through it without knowing that a WebSocket
// exists on the other end.
package logstream

// Level classifies an event. The set is closed — the frontend colour-codes on
// these exact strings, so adding one here without adding it to docs/control-plane.md
// and the frontend produces an uncoloured line.
type Level string

const (
	Paxos  Level = "paxos"  // PREPARE, PROMISE, ACCEPT, COMMIT
	TwoPC  Level = "twopc"  // 2PC begin, votes, decision
	Leader Level = "leader" // elections, step-downs, heartbeat loss
	Net    Level = "net"    // transport: drops, kills, partitions
	Error  Level = "error"  // something failed
	Client Level = "client" // echo of the operator's intent
	Info   Level = "info"   // everything else
)

// Valid reports whether l is one of the seven levels the protocol allows.
func Valid(l Level) bool {
	switch l {
	case Paxos, TwoPC, Leader, Net, Error, Client, Info:
		return true
	}
	return false
}

// Event is one line of cluster activity.
type Event struct {
	Level Level
	Tag   string // short origin marker: "[s0]", "[2PC]", "[net]"
	Msg   string
}

// Sink receives events. Implementations must be safe for concurrent use: the
// nodes of a shard emit from their own goroutines.
//
// Emit must not block indefinitely. A slow consumer has to drop rather than
// stall consensus — a demo that wedges because someone's browser stopped
// reading is worse than a demo with a gap in its log.
type Sink interface {
	Emit(Event)
}

// SinkFunc adapts a function to Sink.
type SinkFunc func(Event)

func (f SinkFunc) Emit(e Event) { f(e) }

// Discard drops everything. Useful in tests and for nodes running without a
// watcher attached.
var Discard Sink = SinkFunc(func(Event) {})

// Emitter is a small convenience wrapper that stamps every event with the same
// tag, so call sites read as s.log.Paxos("PREPARE ballot=%v", b) rather than
// repeating the tag on each line.
type Emitter struct {
	sink Sink
	tag  string
}

// NewEmitter returns an Emitter writing to sink with the given tag. A nil sink
// is treated as Discard so callers never have to nil-check.
func NewEmitter(sink Sink, tag string) *Emitter {
	if sink == nil {
		sink = Discard
	}
	return &Emitter{sink: sink, tag: tag}
}

// WithTag returns a copy of e writing under a different tag.
func (e *Emitter) WithTag(tag string) *Emitter {
	return &Emitter{sink: e.sink, tag: tag}
}

// Tag reports the tag events are stamped with.
func (e *Emitter) Tag() string { return e.tag }

func (e *Emitter) emit(level Level, msg string) {
	if e == nil {
		return
	}
	e.sink.Emit(Event{Level: level, Tag: e.tag, Msg: msg})
}

func (e *Emitter) Paxos(msg string)  { e.emit(Paxos, msg) }
func (e *Emitter) TwoPC(msg string)  { e.emit(TwoPC, msg) }
func (e *Emitter) Leader(msg string) { e.emit(Leader, msg) }
func (e *Emitter) Net(msg string)    { e.emit(Net, msg) }
func (e *Emitter) Error(msg string)  { e.emit(Error, msg) }
func (e *Emitter) Client(msg string) { e.emit(Client, msg) }
func (e *Emitter) Info(msg string)   { e.emit(Info, msg) }
