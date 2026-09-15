package transport

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"paxos-2pc-kvstore/internal/paxos"
)

// inboxSize bounds each replica's queue of undelivered messages.
const inboxSize = 256

// envelope is one request travelling to a replica's inbox, carrying the
// channel its reply comes back on.
type envelope struct {
	ctx   context.Context
	msg   paxos.Message
	reply chan paxos.Message
}

// Handler processes one inbound message on a replica. *paxos.Node.Handle has
// this shape.
type Handler func(ctx context.Context, msg paxos.Message) paxos.Message

// Inproc runs every replica as goroutines in one process, delivering messages
// over Go channels. It is the default and what gets deployed: one process, one
// container, and still a real message-passing system in which a request and
// its reply are separate deliveries that the fault layer can each drop.
type Inproc struct {
	faults   *Faults
	observer Observer

	mu      sync.RWMutex
	inboxes map[paxos.NodeID]chan envelope
	closed  bool

	stop chan struct{}
	wg   sync.WaitGroup
}

// NewInproc returns an empty in-process network. observer may be nil.
func NewInproc(faults *Faults, observer Observer) *Inproc {
	return &Inproc{
		faults:   faults,
		observer: observer,
		inboxes:  make(map[paxos.NodeID]chan envelope),
		stop:     make(chan struct{}),
	}
}

// Register attaches a replica and starts the goroutine that serves its inbox.
func (n *Inproc) Register(id paxos.NodeID, h Handler) {
	inbox := make(chan envelope, inboxSize)

	n.mu.Lock()
	n.inboxes[id] = inbox
	n.mu.Unlock()

	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		for {
			select {
			case <-n.stop:
				return
			case env := <-inbox:
				// Replies are buffered, so a sender that gave up waiting never
				// wedges the receiver.
				env.reply <- h(env.ctx, env.msg)
			}
		}
	}()
}

// For returns the paxos.Transport a given replica sends through.
func (n *Inproc) For(from paxos.NodeID) paxos.Transport {
	return inprocSender{net: n, from: from}
}

// Close stops every inbox goroutine.
func (n *Inproc) Close() {
	n.mu.Lock()
	if n.closed {
		n.mu.Unlock()
		return
	}
	n.closed = true
	n.mu.Unlock()
	close(n.stop)
	n.wg.Wait()
}

type inprocSender struct {
	net  *Inproc
	from paxos.NodeID
}

func (s inprocSender) Send(ctx context.Context, to paxos.NodeID, msg paxos.Message) (paxos.Message, error) {
	reply, err := s.deliver(ctx, to, msg)
	if s.net.observer != nil {
		s.net.observer(newTrace(s.from, to, msg, reply, err))
	}
	return reply, err
}

func (s inprocSender) deliver(ctx context.Context, to paxos.NodeID, msg paxos.Message) (paxos.Message, error) {
	if err := s.net.faults.Check(string(s.from), string(to)); err != nil {
		return paxos.Message{}, err
	}

	s.net.mu.RLock()
	inbox, ok := s.net.inboxes[to]
	closed := s.net.closed
	s.net.mu.RUnlock()
	if closed {
		return paxos.Message{}, errors.New("transport closed")
	}
	if !ok {
		return paxos.Message{}, fmt.Errorf("no replica %s", to)
	}

	env := envelope{ctx: ctx, msg: msg, reply: make(chan paxos.Message, 1)}
	select {
	case inbox <- env:
	case <-ctx.Done():
		return paxos.Message{}, ctx.Err()
	case <-s.net.stop:
		return paxos.Message{}, errors.New("transport closed")
	}

	select {
	case reply := <-env.reply:
		// The reply is its own delivery. If the network changed while the
		// request was being handled -- the receiver was killed, or a partition
		// landed between them -- the reply is lost even though the request
		// took effect, which is the case a real network forces consensus to
		// survive.
		if err := s.net.faults.Check(string(to), string(s.from)); err != nil {
			return paxos.Message{}, err
		}
		return reply, nil
	case <-ctx.Done():
		return paxos.Message{}, ctx.Err()
	case <-s.net.stop:
		return paxos.Message{}, errors.New("transport closed")
	}
}
