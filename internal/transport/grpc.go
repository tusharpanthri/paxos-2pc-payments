package transport

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/status"

	"paxos-2pc-kvstore/internal/paxos"
)

// CodecName is the gRPC content subtype every control-plane RPC uses.
//
// The messages are plain Go structs encoded as JSON, registered as a gRPC
// codec. That keeps the demo free of a protoc step: paxos.Message already
// round-trips through JSON for the HTTP transport the banks use, and nothing
// about gRPC requires protobuf on the wire.
const CodecName = "json"

type jsonCodec struct{}

func (jsonCodec) Marshal(v any) ([]byte, error)      { return json.Marshal(v) }
func (jsonCodec) Unmarshal(data []byte, v any) error { return json.Unmarshal(data, v) }
func (jsonCodec) Name() string                       { return CodecName }

func init() { encoding.RegisterCodec(jsonCodec{}) }

// DialOptions are the options every control-plane client uses: plaintext on
// localhost, JSON codec.
func DialOptions() []grpc.DialOption {
	return []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.CallContentSubtype(CodecName)),
	}
}

const (
	peerService = "paxosdemo.Peer"
	peerMethod  = "/" + peerService + "/Paxos"
)

// peerServer is the receiving side of the gRPC transport.
type peerServer struct {
	self    paxos.NodeID
	faults  *Faults
	handler Handler
}

// RegisterPeer mounts the Paxos peer service for one replica on a gRPC server.
// Inbound messages are checked against the node's own copy of the fault table
// before they reach the engine, so a killed or partitioned node refuses
// traffic on arrival as well as on departure.
func RegisterPeer(s *grpc.Server, self paxos.NodeID, faults *Faults, h Handler) {
	srv := &peerServer{self: self, faults: faults, handler: h}
	s.RegisterService(&grpc.ServiceDesc{
		ServiceName: peerService,
		HandlerType: (*any)(nil),
		Methods: []grpc.MethodDesc{{
			MethodName: "Paxos",
			Handler: func(_ any, ctx context.Context, dec func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
				var msg paxos.Message
				if err := dec(&msg); err != nil {
					return nil, err
				}
				return srv.handle(ctx, msg)
			},
		}},
	}, srv)
}

func (p *peerServer) handle(ctx context.Context, msg paxos.Message) (*paxos.Message, error) {
	if err := p.faults.Check(string(msg.From), string(p.self)); err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	reply := p.handler(ctx, msg)
	return &reply, nil
}

// GRPC is a paxos.Transport over real gRPC between node processes.
type GRPC struct {
	self     paxos.NodeID
	peers    map[paxos.NodeID]string
	faults   *Faults
	observer Observer

	mu    sync.Mutex
	conns map[paxos.NodeID]*grpc.ClientConn
}

// NewGRPC returns a transport for one replica. peers maps replica id to
// host:port; observer may be nil.
func NewGRPC(self paxos.NodeID, peers map[paxos.NodeID]string, faults *Faults, observer Observer) *GRPC {
	return &GRPC{
		self:     self,
		peers:    peers,
		faults:   faults,
		observer: observer,
		conns:    make(map[paxos.NodeID]*grpc.ClientConn),
	}
}

// Send implements paxos.Transport.
func (g *GRPC) Send(ctx context.Context, to paxos.NodeID, msg paxos.Message) (paxos.Message, error) {
	reply, err := g.send(ctx, to, msg)
	if g.observer != nil {
		g.observer(newTrace(g.self, to, msg, reply, err))
	}
	return reply, err
}

func (g *GRPC) send(ctx context.Context, to paxos.NodeID, msg paxos.Message) (paxos.Message, error) {
	if err := g.faults.Check(string(g.self), string(to)); err != nil {
		return paxos.Message{}, err
	}
	conn, err := g.conn(to)
	if err != nil {
		return paxos.Message{}, err
	}
	var reply paxos.Message
	if err := conn.Invoke(ctx, peerMethod, &msg, &reply); err != nil {
		return paxos.Message{}, fmt.Errorf("%w: %s", ErrUnreachable, status.Convert(err).Message())
	}
	// Same rule as inproc: a reply crossing a fault that appeared mid-flight
	// is dropped.
	if err := g.faults.Check(string(to), string(g.self)); err != nil {
		return paxos.Message{}, err
	}
	return reply, nil
}

func (g *GRPC) conn(to paxos.NodeID) (*grpc.ClientConn, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if c, ok := g.conns[to]; ok {
		return c, nil
	}
	addr, ok := g.peers[to]
	if !ok {
		return nil, fmt.Errorf("no address for replica %s", to)
	}
	c, err := grpc.NewClient(addr, DialOptions()...)
	if err != nil {
		return nil, err
	}
	g.conns[to] = c
	return c, nil
}

// Close releases client connections.
func (g *GRPC) Close() {
	g.mu.Lock()
	defer g.mu.Unlock()
	for id, c := range g.conns {
		_ = c.Close()
		delete(g.conns, id)
	}
}
