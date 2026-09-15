package cluster

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"paxos-2pc-kvstore/internal/ledger"
	"paxos-2pc-kvstore/internal/paxos"
	"paxos-2pc-kvstore/internal/transport"
)

// The control service is how a gateway drives a node process: propose, inspect,
// campaign, set faults, and drain traces. Replica-to-replica consensus traffic
// never uses it; that goes over the peer service in internal/transport.
const controlService = "paxosdemo.Control"

type empty struct{}

type proposeReply struct {
	Result    ledger.Result `json:"result"`
	Err       string        `json:"err,omitempty"`
	NotLeader bool          `json:"notLeader,omitempty"`
	NoQuorum  bool          `json:"noQuorum,omitempty"`
}

type drainReply struct {
	Traces []transport.Trace `json:"traces"`
}

// NodeOptions configure one node process.
type NodeOptions struct {
	ID      string            // e.g. s0n1
	Listen  string            // host:port for both services
	Peers   map[string]string // every member of this replica's shard, itself included
	MaxKeys int
	Timing  paxos.Config
}

// maxBufferedTraces bounds a node's trace buffer between drains.
const maxBufferedTraces = 4096

// ServeNode runs one replica as its own process, serving consensus traffic to
// its peers and control traffic to a gateway, until ctx is cancelled.
func ServeNode(ctx context.Context, opts NodeOptions) error {
	if _, _, err := ParseNode(opts.ID); err != nil {
		return err
	}
	if _, ok := opts.Peers[opts.ID]; !ok {
		return fmt.Errorf("peers must include %s itself", opts.ID)
	}
	timing := Config{Timing: opts.Timing}.withDefaults().Timing

	var (
		mu     sync.Mutex
		traces []transport.Trace
	)
	observe := func(t transport.Trace) {
		mu.Lock()
		defer mu.Unlock()
		if len(traces) < maxBufferedTraces {
			traces = append(traces, t)
		}
	}

	peerAddrs := make(map[paxos.NodeID]string, len(opts.Peers))
	peerIDs := make([]paxos.NodeID, 0, len(opts.Peers))
	for id, addr := range opts.Peers {
		peerAddrs[paxos.NodeID(id)] = addr
		peerIDs = append(peerIDs, paxos.NodeID(id))
	}

	faults := transport.NewFaults()
	tr := transport.NewGRPC(paxos.NodeID(opts.ID), peerAddrs, faults, observe)
	defer tr.Close()

	sm := ledger.New(opts.MaxKeys)
	node := paxos.NewNode(paxos.NodeID(opts.ID), peerIDs, paxos.NewMemLog(), sm, tr, timing)

	srv := grpc.NewServer()
	transport.RegisterPeer(srv, paxos.NodeID(opts.ID), faults, node.Handle)
	srv.RegisterService(&grpc.ServiceDesc{
		ServiceName: controlService,
		HandlerType: (*any)(nil),
		Methods: []grpc.MethodDesc{
			unary("Propose", func(ctx context.Context, cmd *ledger.Command) (*proposeReply, error) {
				res, err := propose(ctx, node, *cmd)
				out := &proposeReply{Result: res}
				if err != nil {
					out.Err = err.Error()
					out.NotLeader = errors.Is(err, paxos.ErrNotLeader)
					out.NoQuorum = errors.Is(err, paxos.ErrNoQuorum) || errors.Is(err, context.DeadlineExceeded)
				}
				return out, nil
			}),
			unary("Info", func(context.Context, *empty) (*NodeInfo, error) {
				info := describe(node, sm)
				return &info, nil
			}),
			unary("Campaign", func(context.Context, *empty) (*empty, error) {
				node.Campaign()
				return &empty{}, nil
			}),
			unary("SetFaults", func(_ context.Context, st *transport.FaultState) (*empty, error) {
				faults.Restore(*st)
				return &empty{}, nil
			}),
			unary("Drain", func(context.Context, *empty) (*drainReply, error) {
				mu.Lock()
				defer mu.Unlock()
				out := &drainReply{Traces: traces}
				traces = nil
				return out, nil
			}),
		},
	}, struct{}{})

	lis, err := net.Listen("tcp", opts.Listen)
	if err != nil {
		return err
	}
	node.Start()
	defer node.Stop()

	errc := make(chan error, 1)
	go func() { errc <- srv.Serve(lis) }()
	select {
	case <-ctx.Done():
		srv.GracefulStop()
		return nil
	case err := <-errc:
		return err
	}
}

// unary adapts a typed function onto a gRPC method descriptor.
func unary[Req, Resp any](name string, fn func(context.Context, *Req) (*Resp, error)) grpc.MethodDesc {
	return grpc.MethodDesc{
		MethodName: name,
		Handler: func(_ any, ctx context.Context, dec func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
			req := new(Req)
			if err := dec(req); err != nil {
				return nil, status.Error(codes.InvalidArgument, err.Error())
			}
			return fn(ctx, req)
		},
	}
}

// remoteReplica drives a node process over the control service.
type remoteReplica struct {
	id   string
	conn *grpc.ClientConn
}

func (r *remoteReplica) ID() string { return r.id }

func (r *remoteReplica) call(ctx context.Context, method string, req, reply any) error {
	ctx, cancel := context.WithTimeout(ctx, proposeTimeout+time.Second)
	defer cancel()
	return r.conn.Invoke(ctx, "/"+controlService+"/"+method, req, reply)
}

func (r *remoteReplica) Propose(ctx context.Context, cmd ledger.Command) (ledger.Result, error) {
	var out proposeReply
	if err := r.call(ctx, "Propose", &cmd, &out); err != nil {
		return ledger.Result{}, fmt.Errorf("%s: %w", r.id, err)
	}
	switch {
	case out.NotLeader:
		return ledger.Result{}, fmt.Errorf("%s: %w", r.id, paxos.ErrNotLeader)
	case out.NoQuorum:
		return ledger.Result{}, fmt.Errorf("%s: %w", r.id, paxos.ErrNoQuorum)
	case out.Err != "":
		return ledger.Result{}, errors.New(out.Err)
	}
	return out.Result, nil
}

func (r *remoteReplica) Info(ctx context.Context) (NodeInfo, error) {
	var out NodeInfo
	err := r.call(ctx, "Info", &empty{}, &out)
	return out, err
}

func (r *remoteReplica) Campaign(ctx context.Context) error {
	return r.call(ctx, "Campaign", &empty{}, &empty{})
}

func (r *remoteReplica) setFaults(ctx context.Context, st transport.FaultState) error {
	return r.call(ctx, "SetFaults", &st, &empty{})
}

func (r *remoteReplica) drain(ctx context.Context) []transport.Trace {
	var out drainReply
	if err := r.call(ctx, "Drain", &empty{}, &out); err != nil {
		return nil
	}
	return out.Traces
}
