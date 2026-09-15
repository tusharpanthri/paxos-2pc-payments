// Command cluster runs the sharded payments ledger and the WebSocket gateway the
// browser control plane drives.
//
//	go run ./cmd/cluster                    # inproc: one process, deployable
//	go run ./cmd/cluster -transport=grpc    # one OS process per replica, over gRPC
//
// then point frontend/index.html at ws://localhost:8080/ws.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"paxos-2pc-kvstore/internal/cluster"
	"paxos-2pc-kvstore/internal/gateway"
	"paxos-2pc-kvstore/internal/logstream"
)

func main() {
	if err := run(); err != nil {
		slog.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		addr      = flag.String("addr", "", "gateway listen address (default :$PORT, or :8080)")
		shards    = flag.Int("shards", envInt("SHARDS", 3), "number of shards (env SHARDS)")
		nodes     = flag.Int("nodes", envInt("NODES", 3), "replicas per shard (env NODES)")
		transport = flag.String("transport", envStr("TRANSPORT", "inproc"), "inter-node transport: inproc|grpc (env TRANSPORT)")
		pace      = flag.Duration("pace", 110*time.Millisecond, "delay between log frames, presentation only (0 disables)")
		sessions  = flag.Int("max-sessions", 32, "maximum concurrent connections")
		maxKeys   = flag.Int("max-keys", 256, "maximum distinct accounts per shard")

		// grpc mode
		role     = flag.String("role", "gateway", "grpc mode: gateway|node")
		basePort = flag.Int("base-port", 7100, "grpc mode: first port for spawned node processes")
		attach   = flag.String("attach", "", "grpc mode: s0n0=host:port,... to attach to node processes started by hand instead of spawning them")
		nodeID   = flag.String("id", "", "node role: this replica's id, e.g. s0n1")
		listen   = flag.String("listen", "", "node role: host:port to serve on")
		peers    = flag.String("peers", "", "node role: id=host:port for every replica in this shard, itself included")

		parentPipe = flag.Bool("exit-with-parent", false, "node role: exit when stdin closes (set by the spawning gateway)")
	)
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg := cluster.Config{Shards: *shards, NodesPerShard: *nodes, MaxKeysPerShard: *maxKeys}
	if err := cfg.Validate(); err != nil {
		return err
	}

	opts := gateway.Options{Pace: *pace, MaxSessions: *sessions, Logger: logger}

	switch *transport {
	case "inproc":
		opts.NewCluster = func() (*cluster.Engine, error) { return cluster.NewInproc(cfg) }

	case "grpc":
		if *role == "node" {
			peerMap, err := parseAddrs(*peers)
			if err != nil {
				return err
			}
			if *parentPipe {
				// Spawned by a gateway: exit when it does. Its end of our stdin
				// closes when it dies, however it dies, which a signal-based
				// approach cannot promise on Windows.
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				go func() {
					_, _ = io.Copy(io.Discard, os.Stdin)
					cancel()
				}()
			}
			logger.Info("node serving", "id", *nodeID, "listen", *listen)
			return cluster.ServeNode(ctx, cluster.NodeOptions{ID: *nodeID, Listen: *listen, Peers: peerMap, MaxKeys: *maxKeys})
		}

		addrs, err := parseAddrs(*attach)
		if err != nil {
			return err
		}
		if *attach == "" {
			addrs, err = spawnNodes(ctx, cfg, *basePort, *maxKeys, logger)
			if err != nil {
				return err
			}
		}
		engine, err := cluster.NewRemote(cfg, addrs)
		if err != nil {
			return err
		}
		defer engine.Close()
		// The replicas are long-lived processes, so they elect once here and
		// every connection shares them.
		time.Sleep(300 * time.Millisecond) // let the child listeners come up
		engine.Do(logstream.Discard, func() { engine.Bootstrap(ctx) })
		opts.Shared = engine

	default:
		return fmt.Errorf("unknown transport %q, want inproc or grpc", *transport)
	}

	gw, err := gateway.New(opts)
	if err != nil {
		return err
	}

	listenAddr := resolveAddr(*addr)
	logger.Info("control plane up",
		"addr", listenAddr, "transport", *transport,
		"shards", cfg.Shards, "nodes_per_shard", cfg.NodesPerShard, "quorum", cfg.Quorum())
	logger.Info("connect the frontend", "url", "ws://localhost"+listenAddr+"/ws")

	if err := gw.Serve(ctx, listenAddr); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// childPipes keeps the write end of every child's stdin open; see spawnNodes.
var childPipes []io.WriteCloser

// spawnNodes starts one child process per replica, each a copy of this binary
// in the node role, and returns their addresses. Children die with ctx.
func spawnNodes(ctx context.Context, cfg cluster.Config, basePort, maxKeys int, logger *slog.Logger) (map[string]string, error) {
	self, err := os.Executable()
	if err != nil {
		return nil, err
	}
	addrs := map[string]string{}
	for s := 0; s < cfg.Shards; s++ {
		var shardPeers []string
		for i := 0; i < cfg.NodesPerShard; i++ {
			name := cluster.NodeName(s, i)
			addrs[name] = fmt.Sprintf("127.0.0.1:%d", basePort+s*cfg.NodesPerShard+i)
			shardPeers = append(shardPeers, name+"="+addrs[name])
		}
		for i := 0; i < cfg.NodesPerShard; i++ {
			name := cluster.NodeName(s, i)
			cmd := exec.CommandContext(ctx, self,
				"-transport=grpc", "-role=node",
				"-id="+name, "-listen="+addrs[name],
				"-peers="+strings.Join(shardPeers, ","),
				"-max-keys="+strconv.Itoa(maxKeys),
				"-exit-with-parent")
			cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
			pipe, err := cmd.StdinPipe()
			if err != nil {
				return nil, err
			}
			// Held for the life of this process. Dropping the reference would let
			// a GC finalizer close the pipe, and the child would exit.
			childPipes = append(childPipes, pipe)
			if err := cmd.Start(); err != nil {
				return nil, fmt.Errorf("start %s: %w", name, err)
			}
			logger.Info("spawned node process", "id", name, "pid", cmd.Process.Pid, "addr", addrs[name])
			go func() { _ = cmd.Wait() }()
		}
	}
	return addrs, nil
}

func parseAddrs(raw string) (map[string]string, error) {
	out := map[string]string{}
	if strings.TrimSpace(raw) == "" {
		return out, nil
	}
	for _, part := range strings.Split(raw, ",") {
		id, addr, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok || id == "" || addr == "" {
			return nil, fmt.Errorf("bad address %q, want id=host:port", part)
		}
		out[id] = addr
	}
	return out, nil
}

// resolveAddr honours -addr, then $PORT, then :8080. Render and Fly inject PORT.
func resolveAddr(addr string) string {
	if addr != "" {
		return addr
	}
	if port := os.Getenv("PORT"); port != "" {
		return ":" + port
	}
	return ":8080"
}

func envStr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(key)); err == nil {
		return v
	}
	return def
}
