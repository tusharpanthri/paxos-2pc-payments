package cluster

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"paxos-2pc-kvstore/internal/ledger"
	"paxos-2pc-kvstore/internal/logstream"
	"paxos-2pc-kvstore/internal/paxos"
	"paxos-2pc-kvstore/internal/transport"
	"paxos-2pc-kvstore/internal/twopc"
)

const (
	// proposeTimeout bounds one write end to end.
	proposeTimeout = 3 * time.Second
	// settleTimeout bounds how long a fault command waits for elections and
	// step-downs to finish, so its log shows the consequence, not just the cause.
	settleTimeout = 4 * time.Second
	pollEvery     = 40 * time.Millisecond
)

// Engine is a running cluster.
type Engine struct {
	cfg    Config
	shards [][]Replica
	faults *transport.Faults
	coord  *twopc.Coordinator

	// pushFaults distributes the fault table to node processes; nil when the
	// replicas share e.faults directly.
	pushFaults func(context.Context, transport.FaultState)
	// drain collects traces buffered in node processes; nil for inproc, where
	// traces arrive synchronously.
	drain   func(context.Context) []transport.Trace
	closeFn func()

	cmdMu sync.Mutex   // one operator command at a time
	sink  atomic.Value // holds sinkBox: where events go right now

	traceMu sync.Mutex
	acks    map[string]int // "node/slot" -> ACCEPTED replies seen

	roleMu sync.Mutex
	roles  map[string]string

	stop      chan struct{}
	wg        sync.WaitGroup
	closeOnce sync.Once
}

type sinkBox struct{ logstream.Sink }

func newEngine(cfg Config, faults *transport.Faults) *Engine {
	e := &Engine{
		cfg:    cfg,
		faults: faults,
		acks:   make(map[string]int),
		roles:  make(map[string]string),
		stop:   make(chan struct{}),
	}
	e.sink.Store(sinkBox{logstream.Discard})
	e.coord = twopc.New(logstream.SinkFunc(e.emitEvent))
	return e
}

// start launches the background watcher. Call once replicas are in place.
func (e *Engine) start() {
	// Record every replica's starting role now. The watcher only narrates
	// changes against a baseline, so without this an election that lands
	// before its first tick would never be announced.
	e.pollRoles(context.Background())

	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		t := time.NewTicker(100 * time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-e.stop:
				return
			case <-t.C:
				e.collectTraces(context.Background())
				e.pollRoles(context.Background())
			}
		}
	}()
}

// Config returns the topology.
func (e *Engine) Config() Config { return e.cfg }

// Do runs one operator command with its events routed to sink. Events that
// happen between commands -- heartbeats, a background commit retry -- go
// nowhere, so the terminal only ever shows activity caused by what was typed.
func (e *Engine) Do(sink logstream.Sink, fn func()) {
	e.cmdMu.Lock()
	defer e.cmdMu.Unlock()
	e.sink.Store(sinkBox{sink})
	defer e.sink.Store(sinkBox{logstream.Discard})
	fn()
	e.flush(context.Background())
}

// flush makes sure buffered traces and role changes are emitted before a
// command's result.
func (e *Engine) flush(ctx context.Context) {
	e.collectTraces(ctx)
	e.pollRoles(ctx)
}

func (e *Engine) emitEvent(ev logstream.Event) {
	e.sink.Load().(sinkBox).Emit(ev)
}

func (e *Engine) emit(level logstream.Level, tag, msg string) {
	e.emitEvent(logstream.Event{Level: level, Tag: tag, Msg: msg})
}

// Close stops every replica.
func (e *Engine) Close() error {
	e.closeOnce.Do(func() {
		close(e.stop)
		e.wg.Wait()
		e.coord.Close()
		if e.closeFn != nil {
			e.closeFn()
		}
	})
	return nil
}

// --- topology helpers ---------------------------------------------------

func (e *Engine) replica(name string) (Replica, int, error) {
	s, i, err := ParseNode(name)
	if err != nil {
		return nil, 0, err
	}
	if s >= e.cfg.Shards || i >= e.cfg.NodesPerShard {
		return nil, 0, fmt.Errorf("%w: %s (this cluster has s0..s%d, n0..n%d)", ErrUnknownNode, name, e.cfg.Shards-1, e.cfg.NodesPerShard-1)
	}
	return e.shards[s][i], s, nil
}

func (e *Engine) alive(r Replica) bool { return !e.faults.Dead(r.ID()) }

// majorityGroup returns the replicas of a shard that are alive and on the
// largest side of any partition, and whether that side is a quorum.
func (e *Engine) majorityGroup(s int) ([]Replica, bool) {
	groups := map[int][]Replica{}
	for _, r := range e.shards[s] {
		if e.alive(r) {
			g := e.faults.GroupOf(r.ID())
			groups[g] = append(groups[g], r)
		}
	}
	var best []Replica
	for _, g := range groups {
		if len(g) > len(best) || (len(g) == len(best) && len(g) > 0 && g[0].ID() < best[0].ID()) {
			best = g
		}
	}
	return best, len(best) >= e.cfg.Quorum()
}

// infos fetches every replica's view of one shard.
func (e *Engine) infos(ctx context.Context, s int) []NodeInfo {
	out := make([]NodeInfo, len(e.shards[s]))
	for i, r := range e.shards[s] {
		info, err := r.Info(ctx)
		if err != nil {
			info = NodeInfo{ID: r.ID(), Role: "unreachable"}
		}
		out[i] = info
	}
	return out
}

// leaderOf returns the replica that can actually lead a shard right now: alive,
// believing itself leader, and inside a group that holds a quorum. A leader cut
// off in a minority is not returned even before it notices and steps down.
func (e *Engine) leaderOf(ctx context.Context, s int) (Replica, NodeInfo, bool) {
	group, ok := e.majorityGroup(s)
	if !ok {
		return nil, NodeInfo{}, false
	}
	var (
		best     Replica
		bestInfo NodeInfo
	)
	for _, r := range group {
		info, err := r.Info(ctx)
		if err != nil || info.Role != paxos.Leader.String() {
			continue
		}
		if best == nil || bestInfo.Ballot.Less(info.Ballot) {
			best, bestInfo = r, info
		}
	}
	return best, bestInfo, best != nil
}

// settled reports whether a shard is in a stable state: no replica outside the
// majority still claims leadership, and if a majority exists it has a leader.
func (e *Engine) settled(ctx context.Context, s int) bool {
	group, ok := e.majorityGroup(s)
	inGroup := map[string]bool{}
	for _, r := range group {
		inGroup[r.ID()] = true
	}
	leaders := 0
	for _, info := range e.infos(ctx, s) {
		if info.Role != paxos.Leader.String() || e.faults.Dead(info.ID) {
			continue
		}
		if !ok || !inGroup[info.ID] {
			return false // a stale leader has not stepped down yet
		}
		leaders++
	}
	return !ok || leaders >= 1
}

// awaitSettled polls until every listed shard is settled or time runs out.
func (e *Engine) awaitSettled(ctx context.Context, shards []int) bool {
	deadline := time.Now().Add(settleTimeout)
	for {
		all := true
		for _, s := range shards {
			if !e.settled(ctx, s) {
				all = false
				break
			}
		}
		e.flush(ctx)
		if all {
			return true
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			return false
		}
		time.Sleep(pollEvery)
	}
}

// elect starts an election in a shard's majority group, if it has one and no
// leader, and waits for the outcome.
func (e *Engine) elect(ctx context.Context, s int, why string) {
	tag := ShardTag(s)
	group, ok := e.majorityGroup(s)
	if !ok {
		e.emit(logstream.Error, tag, fmt.Sprintf("no quorum: the largest reachable group has %d of %d replicas, needs %d. shard is leaderless; writes will fail",
			len(group), e.cfg.NodesPerShard, e.cfg.Quorum()))
		e.awaitSettled(ctx, []int{s})
		return
	}
	if _, _, has := e.leaderOf(ctx, s); !has {
		candidate := group[0]
		e.emit(logstream.Leader, tag, fmt.Sprintf("%s: %s campaigns, sending PREPARE under a fresh ballot", why, candidate.ID()))
		_ = candidate.Campaign(ctx)
	}
	if !e.awaitSettled(ctx, []int{s}) {
		e.emit(logstream.Error, tag, "election did not settle yet; the replicas' own timers will retry")
	}
}

// leaderFor finds a shard's leader for a command, waiting briefly through an
// election in progress.
func (e *Engine) leaderFor(ctx context.Context, s int) (Replica, NodeInfo, error) {
	group, ok := e.majorityGroup(s)
	if !ok {
		return nil, NodeInfo{}, fmt.Errorf("s%d cannot reach a majority (%d of %d reachable, needs %d): %w",
			s, len(group), e.cfg.NodesPerShard, e.cfg.Quorum(), ErrNoQuorum)
	}
	deadline := time.Now().Add(2 * time.Second)
	nudged := false
	for {
		if r, info, has := e.leaderOf(ctx, s); has {
			return r, info, nil
		}
		if !nudged {
			nudged = true
			e.emit(logstream.Leader, ShardTag(s), "no leader yet; "+group[0].ID()+" campaigns")
			_ = group[0].Campaign(ctx)
			continue
		}
		if time.Now().After(deadline) {
			return nil, NodeInfo{}, fmt.Errorf("s%d: %w elected yet, try again", s, ErrNoLeader)
		}
		time.Sleep(pollEvery)
	}
}

// propose runs one command through a shard's leader and narrates the round.
func (e *Engine) propose(ctx context.Context, s int, cmd ledger.Command) (ledger.Result, error) {
	tag := ShardTag(s)
	leader, info, err := e.leaderFor(ctx, s)
	if err != nil {
		return ledger.Result{}, err
	}

	// Multi-Paxos runs phase 1 once per leadership, not once per command.
	// Saying so beats either faking a PREPARE or silently omitting it.
	e.emit(logstream.Paxos, tag, fmt.Sprintf("PREPARE/PROMISE already held: %s leads under ballot %s, promised by a majority when it was elected",
		leader.ID(), info.Ballot))
	e.emit(logstream.Paxos, tag, fmt.Sprintf("%s proposes %q", leader.ID(), cmd.String()))

	pctx, cancel := context.WithTimeout(ctx, proposeTimeout)
	res, err := leader.Propose(pctx, cmd)
	cancel()
	e.collectTraces(ctx)

	if err != nil {
		switch {
		case errors.Is(err, paxos.ErrNoQuorum), errors.Is(err, context.DeadlineExceeded):
			e.emit(logstream.Error, tag, fmt.Sprintf("ACCEPT did not reach %d of %d; %s steps down rather than commit alone", e.cfg.Quorum(), e.cfg.NodesPerShard, leader.ID()))
			return ledger.Result{}, fmt.Errorf("s%d: %w for %s", s, ErrNoQuorum, cmd.String())
		case errors.Is(err, paxos.ErrNotLeader):
			return ledger.Result{}, fmt.Errorf("s%d: leadership moved mid-round (%w), try again", s, ErrNoLeader)
		}
		return ledger.Result{}, err
	}

	acks := e.takeAcks(leader.ID(), res.Slot) + 1
	e.emit(logstream.Paxos, tag, fmt.Sprintf("COMMIT slot=%d %s (accepted by %d/%d, quorum %d)",
		res.Slot, cmd.String(), acks, e.cfg.NodesPerShard, e.cfg.Quorum()))
	return res, nil
}

// --- operator commands --------------------------------------------------

// Bootstrap elects replica n0 of every shard, so a fresh cluster starts with
// predictable leaders instead of whichever timer fires first.
//
// Shards elect one after another rather than in parallel, purely so each
// shard's PREPARE/PROMISE exchange reads as its own block in the terminal.
func (e *Engine) Bootstrap(ctx context.Context) {
	for s := range e.shards {
		e.elect(ctx, s, "startup")
	}
}

// Put sets an account's balance.
func (e *Engine) Put(ctx context.Context, key string, value int64) error {
	s := ShardFor(key, e.cfg.Shards)
	res, err := e.propose(ctx, s, ledger.Command{Op: ledger.OpPut, Key: key, Amount: value})
	if err != nil {
		return err
	}
	return res.Err()
}

// Get reads an account through the shard's log.
func (e *Engine) Get(ctx context.Context, key string) (int64, error) {
	s := ShardFor(key, e.cfg.Shards)
	res, err := e.propose(ctx, s, ledger.Command{Op: ledger.OpGet, Key: key})
	if err != nil {
		return 0, err
	}
	if err := res.Err(); err != nil {
		return 0, err
	}
	e.emit(logstream.Paxos, ShardTag(s), fmt.Sprintf("read served at slot %d: every write acknowledged before it is visible (linearizable)", res.Slot))
	return res.Value, nil
}

// Transfer moves funds, with 2PC when the accounts live on different shards.
func (e *Engine) Transfer(ctx context.Context, from, to string, amount int64) error {
	fs, ts := ShardFor(from, e.cfg.Shards), ShardFor(to, e.cfg.Shards)
	if fs == ts {
		e.emit(logstream.TwoPC, "[2PC]", fmt.Sprintf("not needed: %s and %s both live on s%d, so one Paxos round moves the money atomically", from, to, fs))
		res, err := e.propose(ctx, fs, ledger.Command{Op: ledger.OpTransfer, Key: from, To: to, Amount: amount})
		if err != nil {
			return err
		}
		return res.Err()
	}
	return e.coord.Transfer(ctx, participant{e, fs}, from, participant{e, ts}, to, amount)
}

// participant adapts one shard onto twopc.Participant.
type participant struct {
	e *Engine
	s int
}

func (p participant) Name() string { return fmt.Sprintf("s%d", p.s) }

func (p participant) run(ctx context.Context, cmd ledger.Command) error {
	res, err := p.e.propose(ctx, p.s, cmd)
	if err != nil {
		return err
	}
	return res.Err()
}

func (p participant) Prepare(ctx context.Context, tx, key string, delta int64) error {
	return p.run(ctx, ledger.Command{Op: ledger.OpPrepare, Tx: tx, Key: key, Amount: delta})
}

func (p participant) Commit(ctx context.Context, tx string) error {
	return p.run(ctx, ledger.Command{Op: ledger.OpCommit, Tx: tx})
}

func (p participant) Abort(ctx context.Context, tx string) error {
	return p.run(ctx, ledger.Command{Op: ledger.OpAbort, Tx: tx})
}

// Kill fails a replica.
func (e *Engine) Kill(ctx context.Context, name string) error {
	r, s, err := e.replica(name)
	if err != nil {
		return err
	}
	if !e.alive(r) {
		return fmt.Errorf("%s is already down", name)
	}
	leader, _, wasLeader := e.leaderOf(ctx, s)
	wasLeader = wasLeader && leader.ID() == name

	e.faults.Kill(name)
	e.push(ctx)
	e.emit(logstream.Net, "[net]", name+" killed: it stops sending and receiving")

	if !wasLeader {
		_, ok := e.majorityGroup(s)
		if ok {
			e.emit(logstream.Info, ShardTag(s), fmt.Sprintf("%s was a follower; the leader keeps its majority", name))
		} else {
			e.emit(logstream.Error, ShardTag(s), "that was one replica too many: no majority is left, so the leader will step down")
		}
		e.awaitSettled(ctx, []int{s})
		return nil
	}
	e.emit(logstream.Leader, ShardTag(s), name+" was the leader")
	e.elect(ctx, s, "leader lost")
	return nil
}

// Revive brings a killed replica back.
func (e *Engine) Revive(ctx context.Context, name string) error {
	r, s, err := e.replica(name)
	if err != nil {
		return err
	}
	if e.alive(r) {
		return fmt.Errorf("%s is already up", name)
	}
	before, _ := r.Info(ctx)

	e.faults.Revive(name)
	e.push(ctx)
	e.emit(logstream.Net, "[net]", name+" revived: it rejoins with the log it had when it went down")

	if _, _, has := e.leaderOf(ctx, s); !has {
		e.elect(ctx, s, "majority restored")
	} else {
		e.awaitSettled(ctx, []int{s})
	}

	leader, linfo, has := e.leaderOf(ctx, s)
	if !has || e.faults.GroupOf(name) != e.faults.GroupOf(leader.ID()) {
		return nil
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		now, err := r.Info(ctx)
		if err == nil && now.Applied >= linfo.Applied {
			missed := now.Applied - before.Applied
			switch {
			case leader.ID() == name:
				e.emit(logstream.Paxos, ShardTag(s), fmt.Sprintf("%s now leads: it adopted the %d slot(s) it missed from its peers' PROMISEs and re-proposed them, applied through slot %d",
					name, missed, now.Applied))
			case missed == 0:
				e.emit(logstream.Paxos, ShardTag(s), fmt.Sprintf("%s already had every commit, through slot %d", name, now.Applied))
			default:
				e.emit(logstream.Paxos, ShardTag(s), fmt.Sprintf("%s caught up from %s: applied through slot %d (missed %d while down)",
					name, leader.ID(), now.Applied, missed))
			}
			return nil
		}
		time.Sleep(pollEvery)
	}
	e.emit(logstream.Paxos, ShardTag(s), name+" is still catching up in the background")
	return nil
}

// Partition splits the network into groups of replicas.
func (e *Engine) Partition(ctx context.Context, groups [][]string) error {
	listed := map[string]bool{}
	for _, g := range groups {
		for _, name := range g {
			if _, _, err := e.replica(name); err != nil {
				return err
			}
			listed[name] = true
		}
	}

	e.faults.Partition(groups)
	e.push(ctx)

	for i, g := range groups {
		e.emit(logstream.Net, "[net]", fmt.Sprintf("group %d: {%s}", i+1, strings.Join(g, " ")))
	}
	var rest []string
	var affected []int
	for s := range e.shards {
		hit := false
		for _, r := range e.shards[s] {
			if !listed[r.ID()] {
				rest = append(rest, r.ID())
			} else {
				hit = true
			}
		}
		if hit {
			affected = append(affected, s)
		}
	}
	if len(rest) > 0 {
		e.emit(logstream.Net, "[net]", fmt.Sprintf("unlisted replicas form their own group: {%s}", strings.Join(rest, " ")))
	}
	e.emit(logstream.Net, "[net]", "messages between groups are now dropped")

	for _, s := range affected {
		tag := ShardTag(s)
		group, ok := e.majorityGroup(s)
		leader, _, has := e.leaderOfAny(ctx, s)
		switch {
		case !ok:
			if has {
				e.emit(logstream.Leader, tag, fmt.Sprintf("%s can no longer reach a majority; it steps down at its next heartbeat (check-quorum)", leader.ID()))
			}
		case has && inGroup(group, leader.ID()):
			e.emit(logstream.Info, tag, fmt.Sprintf("leader %s is on the majority side and keeps leading", leader.ID()))
		case has:
			e.emit(logstream.Leader, tag, fmt.Sprintf("leader %s is cut off in a minority and will step down (check-quorum)", leader.ID()))
		}
		e.elect(ctx, s, "majority side has no leader")
	}
	return nil
}

// Heal removes every partition.
func (e *Engine) Heal(ctx context.Context) error {
	if !e.faults.Partitioned() {
		return errors.New("the network is not partitioned")
	}
	e.faults.Heal()
	e.push(ctx)
	e.emit(logstream.Net, "[net]", "network healed: every live replica can reach every other")
	for s := range e.shards {
		if _, _, has := e.leaderOf(ctx, s); !has {
			e.elect(ctx, s, "reconnected")
		}
	}
	all := make([]int, e.cfg.Shards)
	for s := range all {
		all[s] = s
	}
	e.awaitSettled(ctx, all)
	return nil
}

// leaderOfAny returns any live replica claiming leadership, majority or not.
func (e *Engine) leaderOfAny(ctx context.Context, s int) (Replica, NodeInfo, bool) {
	var best Replica
	var bestInfo NodeInfo
	for _, r := range e.shards[s] {
		if !e.alive(r) {
			continue
		}
		info, err := r.Info(ctx)
		if err != nil || info.Role != paxos.Leader.String() {
			continue
		}
		if best == nil || bestInfo.Ballot.Less(info.Ballot) {
			best, bestInfo = r, info
		}
	}
	return best, bestInfo, best != nil
}

func inGroup(group []Replica, id string) bool {
	for _, r := range group {
		if r.ID() == id {
			return true
		}
	}
	return false
}

// push applies a fault table change everywhere it needs to go: node processes
// in gRPC mode, and any viewer drawing the cluster.
func (e *Engine) push(ctx context.Context) {
	if e.pushFaults != nil {
		e.pushFaults(ctx, e.faults.State())
	}
	e.snapshot(ctx)
}

// --- inspection ---------------------------------------------------------

// ShardStatus summarises one shard.
type ShardStatus struct {
	Shard  int
	Leader string // empty when leaderless
	Term   uint64 // the leader's ballot number, or the highest seen
	Alive  int
	Total  int
	Quorum int
	Nodes  []NodeStatus
}

// NodeStatus is one replica's liveness and role.
type NodeStatus struct {
	ID      string
	Alive   bool
	Role    string
	Applied uint64
}

// AccountStatus is one account and where it lives.
type AccountStatus struct {
	Key      string
	Shard    int
	Balance  int64
	LockedBy string
	Stale    bool // read from a replica of a leaderless shard
}

// Status is the snapshot behind the status command.
type Status struct {
	Shards     []ShardStatus
	Accounts   []AccountStatus
	Partitions [][]string
}

// Status summarises the cluster. Balances are read from each shard leader's
// applied state (or its most advanced replica when leaderless), without a
// Paxos round: status is an operator view, get is the linearizable read.
func (e *Engine) Status(ctx context.Context) Status {
	st := Status{Partitions: e.faults.State().Groups}
	for s := range e.shards {
		ss := ShardStatus{Shard: s, Total: e.cfg.NodesPerShard, Quorum: e.cfg.Quorum()}
		infos := e.infos(ctx, s)
		leader, linfo, has := e.leaderOf(ctx, s)

		source := -1
		for i, info := range infos {
			alive := !e.faults.Dead(info.ID)
			if alive {
				ss.Alive++
				if info.Ballot.Num > ss.Term {
					ss.Term = info.Ballot.Num
				}
				if source < 0 || info.Applied > infos[source].Applied {
					source = i
				}
			}
			ss.Nodes = append(ss.Nodes, NodeStatus{ID: info.ID, Alive: alive, Role: info.Role, Applied: info.Applied})
		}
		if has {
			ss.Leader = leader.ID()
			ss.Term = linfo.Ballot.Num
			for i, info := range infos {
				if info.ID == ss.Leader {
					source = i
				}
			}
		}
		if source >= 0 {
			for _, a := range infos[source].Accounts {
				st.Accounts = append(st.Accounts, AccountStatus{Key: a.Key, Shard: s, Balance: a.Balance, LockedBy: a.LockedBy, Stale: !has})
			}
		}
		st.Shards = append(st.Shards, ss)
	}
	sort.SliceStable(st.Accounts, func(i, j int) bool {
		if st.Accounts[i].Shard != st.Accounts[j].Shard {
			return st.Accounts[i].Shard < st.Accounts[j].Shard
		}
		return st.Accounts[i].Key < st.Accounts[j].Key
	})
	return st
}

// Datastore renders every replica's state and log tail, one line each, so
// replicas converging (or not) is visible rather than asserted.
func (e *Engine) Datastore(ctx context.Context) []string {
	var lines []string
	for s := range e.shards {
		for _, info := range e.infos(ctx, s) {
			state := info.Role
			if e.faults.Dead(info.ID) {
				state = "DOWN"
			}
			accts := make([]string, 0, len(info.Accounts))
			for _, a := range info.Accounts {
				accts = append(accts, fmt.Sprintf("%s=%d", a.Key, a.Balance))
			}
			tail := make([]string, 0, len(info.Tail))
			for _, l := range info.Tail {
				tail = append(tail, fmt.Sprintf("%d:%s", l.Slot, l.Command))
			}
			lines = append(lines, fmt.Sprintf("%s %-8s promised=%-7s applied=%-3d {%s}  log ..%s",
				info.ID, state, info.Ballot.String(), info.Applied, strings.Join(accts, " "), strings.Join(tail, " | ")))
		}
	}
	return lines
}
