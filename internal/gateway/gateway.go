// Package gateway exposes a cluster to a browser over WebSocket. It is the only
// package that knows a browser exists: the cluster emits structured events
// through logstream, and the gateway turns those into wire frames.
//
// The wire contract is described in docs/control-plane.md.
package gateway

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"

	"paxos-2pc-kvstore/internal/cluster"
)

// Options configure the gateway. Exactly one of NewCluster and Shared is set.
type Options struct {
	// NewCluster builds a private cluster for each connection (inproc mode).
	NewCluster func() (*cluster.Engine, error)
	// Shared is one cluster every connection drives (grpc mode, where the
	// replicas are long-lived processes).
	Shared *cluster.Engine
	// Pace is the delay between log frames. Presentation only; 0 disables it.
	Pace time.Duration
	// MaxSessions bounds concurrent connections. 0 uses the default.
	MaxSessions int
	// Logger receives server diagnostics, not cluster events.
	Logger *slog.Logger
}

// Gateway serves /ws and /healthz.
type Gateway struct {
	opts     Options
	logger   *slog.Logger
	sessions atomic.Int64
}

// New returns a Gateway.
func New(opts Options) (*Gateway, error) {
	if (opts.NewCluster == nil) == (opts.Shared == nil) {
		return nil, errors.New("gateway: set exactly one of NewCluster and Shared")
	}
	if opts.MaxSessions <= 0 {
		opts.MaxSessions = maxSessions
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	return &Gateway{opts: opts, logger: opts.Logger}, nil
}

// Handler returns the HTTP routes.
func (g *Gateway) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", g.handleHealth)
	mux.HandleFunc("GET /ws", g.handleWS)
	mux.HandleFunc("GET /", g.handleRoot)
	return mux
}

func (g *Gateway) handleHealth(w http.ResponseWriter, _ *http.Request) {
	// The static frontend polls this cross-origin to wake a sleeping free-tier
	// instance before opening the socket, so it has to be able to read the reply.
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = w.Write([]byte("ok"))
}

// handleRoot answers a bare visit to the backend URL with a short pointer.
func (g *Gateway) handleRoot(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = w.Write([]byte("paxos-2pc payments control plane\n\n" +
		"this is the backend. connect a websocket to /ws\n" +
		"health check: /healthz\n"))
}

func (g *Gateway) handleWS(w http.ResponseWriter, r *http.Request) {
	if n := g.sessions.Add(1); int(n) > g.opts.MaxSessions {
		g.sessions.Add(-1)
		http.Error(w, "too many active sessions, try again shortly", http.StatusServiceUnavailable)
		return
	}
	defer g.sessions.Add(-1)

	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		// The frontend is static-hosted on another origin, and this endpoint
		// exposes a throwaway in-memory cluster with no user data and no
		// credentials, so any origin is accepted deliberately.
		InsecureSkipVerify: true,
		CompressionMode:    websocket.CompressionDisabled,
	})
	if err != nil {
		g.logger.Warn("websocket handshake failed", "err", err, "remote", r.RemoteAddr)
		return
	}
	conn.SetReadLimit(maxFrameBytes)
	defer func() { _ = conn.CloseNow() }()

	engine, owned := g.opts.Shared, false
	if engine == nil {
		engine, err = g.opts.NewCluster()
		if err != nil {
			g.logger.Error("building session cluster", "err", err)
			_ = conn.Close(websocket.StatusInternalError, "could not build cluster")
			return
		}
		owned = true
	}

	sess := newSession(conn, engine, owned, g.opts.Pace, g.logger)
	g.logger.Info("session opened", "remote", r.RemoteAddr, "active", g.sessions.Load())
	if err := sess.run(r.Context()); err != nil {
		g.logger.Warn("session ended with error", "err", err)
	}
	g.logger.Info("session closed", "remote", r.RemoteAddr, "dropped_frames", sess.dropped.Load())
	_ = conn.Close(websocket.StatusNormalClosure, "")
}

// Serve runs an HTTP server on addr until ctx is cancelled.
func (g *Gateway) Serve(ctx context.Context, addr string) error {
	srv := &http.Server{
		Addr:              addr,
		Handler:           g.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
		// No WriteTimeout: a WebSocket session is long-lived by design. Idle
		// sessions are bounded by idleTimeout in the read loop instead.
	}

	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe() }()

	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return srv.Shutdown(shutdownCtx)
	}
}
