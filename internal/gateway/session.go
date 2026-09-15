package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"

	"paxos-2pc-kvstore/internal/cluster"
	"paxos-2pc-kvstore/internal/logstream"
	"paxos-2pc-kvstore/internal/twopc"
)

// outboxSize bounds how many frames may be queued for one browser. A consumer
// that stops reading must not stall the cluster, so past this point log frames
// are dropped rather than blocking the emitting goroutine.
const outboxSize = 512

// session is one WebSocket connection and the cluster it drives.
type session struct {
	conn   *websocket.Conn
	engine *cluster.Engine
	owned  bool // the cluster was built for this connection and dies with it
	pace   time.Duration
	logger *slog.Logger

	// out carries log and result frames in one queue, so a command's logs are
	// always written ahead of its result: zero or more logs, then one result.
	out     chan any
	dropped atomic.Int64
}

func newSession(conn *websocket.Conn, engine *cluster.Engine, owned bool, pace time.Duration, logger *slog.Logger) *session {
	return &session{
		conn:   conn,
		engine: engine,
		owned:  owned,
		pace:   pace,
		logger: logger,
		out:    make(chan any, outboxSize),
	}
}

// Emit implements logstream.Sink. It never blocks.
func (s *session) Emit(e logstream.Event) {
	select {
	case s.out <- newLogFrame(e):
	default:
		s.dropped.Add(1)
	}
}

// Snapshot implements cluster.Snapshotter. Mid-command snapshots may be dropped
// under backpressure like logs; every command still ends with a fresh one.
func (s *session) Snapshot(st cluster.Status) {
	select {
	case s.out <- newStateFrame(st):
	default:
		s.dropped.Add(1)
	}
}

// publishState queues the cluster's current state, waiting for room, so the
// map is always correct by the time a command's result arrives.
func (s *session) publishState(ctx context.Context) {
	var st cluster.Status
	s.engine.Do(logstream.Discard, func() { st = s.engine.Status(ctx) })
	select {
	case s.out <- newStateFrame(st):
	case <-time.After(2 * time.Second):
		s.logger.Warn("dropped state frame, outbox full")
	}
}

func (s *session) info(tag, msg string) {
	s.Emit(logstream.Event{Level: logstream.Info, Tag: tag, Msg: msg})
}

// result queues the terminating frame for a command. It waits for room rather
// than dropping, because the result is what re-enables the caller's prompt.
func (s *session) result(msg string) {
	select {
	case s.out <- newResultFrame(msg):
	case <-time.After(2 * time.Second):
		s.logger.Warn("dropped result frame, outbox full", "msg", msg)
	}
}

// greet brings the cluster up and narrates it. It ends with a result frame,
// exactly like a command, so the frontend holds its prompt until the startup
// elections have finished.
func (s *session) greet(ctx context.Context) {
	cfg := s.engine.Config()
	s.info("[gw]", fmt.Sprintf("%d shards x %d replicas, quorum %d. multi-paxos per shard (internal/paxos), 2pc across shards",
		cfg.Shards, cfg.NodesPerShard, cfg.Quorum()))

	if s.owned {
		s.info("[gw]", "this cluster is yours alone and resets when you disconnect. electing leaders...")
		s.engine.Do(s, func() { s.engine.Bootstrap(ctx) })
	} else {
		s.info("[gw]", "shared multi-process cluster (grpc transport): every connection drives the same replicas")
	}

	led := 0
	var st cluster.Status
	s.engine.Do(logstream.Discard, func() { st = s.engine.Status(ctx) })
	for _, sh := range st.Shards {
		if sh.Leader != "" {
			led++
		}
	}
	s.Snapshot(st)
	s.result(fmt.Sprintf("OK cluster up, %d/%d shards led. type help", led, len(st.Shards)))
}

// run drives the connection until the client disconnects or ctx is cancelled.
func (s *session) run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if s.owned {
		defer func() { _ = s.engine.Close() }()
	}

	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		s.writeLoop(ctx)
	}()

	s.greet(ctx)
	err := s.readLoop(ctx)
	cancel()
	<-writerDone
	return err
}

// writeLoop is the only goroutine that writes to the socket.
func (s *session) writeLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case frame := <-s.out:
			// Pacing is presentation only. It lives here, at the edge, and never
			// inside consensus: a stream that arrives as one instant dump is
			// unreadable, but a Paxos round that sleeps would misstate how fast
			// the system is.
			if _, isLog := frame.(logFrame); isLog && s.pace > 0 {
				select {
				case <-ctx.Done():
					return
				case <-time.After(s.pace):
				}
			}
			payload, err := json.Marshal(frame)
			if err != nil {
				s.logger.Error("marshalling frame", "err", err)
				continue
			}
			writeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			err = s.conn.Write(writeCtx, websocket.MessageText, payload)
			cancel()
			if err != nil {
				return
			}
		}
	}
}

// readLoop runs one command to completion before reading the next.
func (s *session) readLoop(ctx context.Context) error {
	limiter := newRateLimiter(commandsPerSecond, commandBurst)
	for {
		readCtx, cancel := context.WithTimeout(ctx, idleTimeout)
		_, data, err := s.conn.Read(readCtx)
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if errors.Is(err, context.DeadlineExceeded) {
				s.result("ABORT idle for too long, closing the connection")
				s.drain()
				return s.conn.Close(websocket.StatusNormalClosure, "idle timeout")
			}
			return nil // the client hung up
		}

		var msg inbound
		if err := json.Unmarshal(data, &msg); err != nil {
			s.result(`ERROR expected a JSON object of the form {"cmd": "..."}`)
			continue
		}
		if !limiter.allow() {
			s.result("ERROR slow down: too many commands per second")
			continue
		}
		s.dispatch(ctx, msg.Cmd)
	}
}

// drain gives the writer a moment to flush before the socket closes.
func (s *session) drain() {
	deadline := time.Now().Add(2 * time.Second)
	for len(s.out) > 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
}

// dispatch parses one line, runs it, and guarantees exactly one result frame.
func (s *session) dispatch(ctx context.Context, line string) {
	cmd, err := parse(line)
	if errors.Is(err, errEmpty) {
		s.result("")
		return
	}
	if err != nil {
		s.result("ERROR " + err.Error())
		return
	}

	switch cmd.Verb {
	case verbClear, verbDemo:
		// Client-side only; accepted so a client forwarding every line verbatim
		// gets no error for a command its own UI handles.
		s.result("")
		return
	case verbHelp:
		for _, l := range strings.Split(helpText, "\n") {
			s.info("", l)
		}
		s.result("OK")
		return
	}

	s.Emit(logstream.Event{Level: logstream.Client, Tag: "[you]", Msg: strings.TrimSpace(line)})

	var result string
	s.engine.Do(s, func() { result = s.execute(ctx, cmd) })
	s.publishState(ctx)
	s.result(result)
}

// execute runs a parsed command against the engine and renders its result line.
func (s *session) execute(ctx context.Context, cmd command) string {
	e := s.engine
	shards := e.Config().Shards

	switch cmd.Verb {
	case verbStatus:
		return s.status(ctx)

	case verbDatastore:
		lines := e.Datastore(ctx)
		for _, l := range lines {
			s.Emit(logstream.Event{Level: logstream.Paxos, Tag: "[log]", Msg: l})
		}
		return fmt.Sprintf("OK %d replicas", len(lines))

	case verbPut:
		if err := e.Put(ctx, cmd.Key, cmd.Value); err != nil {
			return s.failure(err)
		}
		return fmt.Sprintf("OK %s=%d on s%d", cmd.Key, cmd.Value, cluster.ShardFor(cmd.Key, shards))

	case verbGet:
		v, err := e.Get(ctx, cmd.Key)
		if err != nil {
			return s.failure(err)
		}
		return fmt.Sprintf("OK %s=%d", cmd.Key, v)

	case verbTransfer:
		if err := e.Transfer(ctx, cmd.From, cmd.To, cmd.Value); err != nil {
			return s.failure(err)
		}
		return fmt.Sprintf("OK moved %d from %s to %s", cmd.Value, cmd.From, cmd.To)

	case verbKill:
		if err := e.Kill(ctx, cmd.Node); err != nil {
			return s.failure(err)
		}
		return "OK " + cmd.Node + " is down"

	case verbRevive:
		if err := e.Revive(ctx, cmd.Node); err != nil {
			return s.failure(err)
		}
		return "OK " + cmd.Node + " is up"

	case verbPartition:
		if err := e.Partition(ctx, cmd.Groups); err != nil {
			return s.failure(err)
		}
		return fmt.Sprintf("OK network split into %d groups", len(cmd.Groups))

	case verbHeal:
		if err := e.Heal(ctx); err != nil {
			return s.failure(err)
		}
		return "OK partitions removed"
	}
	return "ERROR unknown command"
}

// failure renders an engine error. Losing quorum or running out of funds is an
// expected outcome of this demo, so those read as ABORT. Anything else --
// including a 2PC left in doubt, which is decided but not yet applied
// everywhere and so is not an abort -- is an ERROR.
func (s *session) failure(err error) string {
	expected := []error{
		cluster.ErrNoQuorum, cluster.ErrNoLeader, cluster.ErrNoSuchKey, cluster.ErrLocked,
		cluster.ErrInsufficientFunds, cluster.ErrTooManyKeys,
	}
	for _, target := range expected {
		if errors.Is(err, target) && !errors.Is(err, twopc.ErrInDoubt) {
			return "ABORT " + err.Error()
		}
	}
	return "ERROR " + err.Error()
}

func (s *session) status(ctx context.Context) string {
	st := s.engine.Status(ctx)

	// Writable is not "enough nodes alive": a shard can have every replica up
	// and still no leader, if a partition leaves no side with a majority.
	writable := 0
	for _, sh := range st.Shards {
		leader := sh.Leader
		if leader == "" {
			leader = "none"
		} else {
			writable++
		}
		members := make([]string, 0, len(sh.Nodes))
		for _, n := range sh.Nodes {
			state := "up"
			if !n.Alive {
				state = "DOWN"
			}
			marker := ""
			if n.ID == sh.Leader {
				marker = "*"
			}
			members = append(members, fmt.Sprintf("%s%s:%s", marker, n.ID, state))
		}
		s.Emit(logstream.Event{
			Level: logstream.Leader,
			Tag:   cluster.ShardTag(sh.Shard),
			Msg: fmt.Sprintf("leader=%s term=%d alive=%d/%d needs=%d  %s",
				leader, sh.Term, sh.Alive, sh.Total, sh.Quorum, strings.Join(members, " ")),
		})
	}

	if len(st.Partitions) > 0 {
		rendered := make([]string, len(st.Partitions))
		for i, g := range st.Partitions {
			rendered[i] = "{" + strings.Join(g, " ") + "}"
		}
		s.Emit(logstream.Event{Level: logstream.Net, Tag: "[net]", Msg: "partitioned: " + strings.Join(rendered, " | ")})
	}

	if len(st.Accounts) == 0 {
		s.info("[keys]", "no accounts yet, try: put tushar 100")
	}
	for _, a := range st.Accounts {
		msg := fmt.Sprintf("%s = %d", a.Key, a.Balance)
		if a.LockedBy != "" {
			msg += "  (locked by " + a.LockedBy + ")"
		}
		if a.Stale {
			msg += "  (last known: shard has no leader)"
		}
		s.info(cluster.ShardTag(a.Shard), msg)
	}
	return fmt.Sprintf("OK %d shards, %d writable, %d accounts", len(st.Shards), writable, len(st.Accounts))
}
