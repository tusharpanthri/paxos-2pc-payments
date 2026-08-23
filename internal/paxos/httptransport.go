package paxos

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// Path is the endpoint replicas exchange protocol messages on.
const Path = "/paxos"

// HTTPTransport carries protocol messages between replicas as JSON over
// mutual TLS.
//
// Consensus traffic deliberately does not share the gRPC surface the clients
// and the gateway use. Keeping it on its own listener means the replication
// channel cannot be reached through the public API at all, and it keeps the
// message type an internal detail rather than part of a published .proto.
// Both sides still present certificates signed by the same private CA, so a
// replica will not talk to anything the CA has not vouched for.
type HTTPTransport struct {
	peers  map[NodeID]string // replica id -> host:port
	client *http.Client
}

// NewHTTPTransport builds a transport that can reach the given peers.
func NewHTTPTransport(peers map[NodeID]string, tlsCfg *tls.Config, timeout time.Duration) *HTTPTransport {
	addrs := make(map[NodeID]string, len(peers))
	for id, addr := range peers {
		addrs[id] = addr
	}
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	return &HTTPTransport{
		peers: addrs,
		client: &http.Client{
			Timeout: timeout,
			Transport: &http.Transport{
				TLSClientConfig:     tlsCfg,
				MaxIdleConnsPerHost: 8, // consensus is chatty; reuse connections
				IdleConnTimeout:     90 * time.Second,
			},
		},
	}
}

// Send delivers one message and waits for the reply.
func (t *HTTPTransport) Send(ctx context.Context, to NodeID, msg Message) (Message, error) {
	addr, ok := t.peers[to]
	if !ok {
		return Message{}, fmt.Errorf("paxos: no address known for replica %s", to)
	}

	body, err := json.Marshal(msg)
	if err != nil {
		return Message{}, fmt.Errorf("paxos: encode %s: %w", msg.Kind, err)
	}

	url := fmt.Sprintf("https://%s%s", addr, Path)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return Message{}, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := t.client.Do(req)
	if err != nil {
		// Unreachable is normal, not exceptional: a quorum protocol expects
		// to lose peers and simply proceeds without their vote.
		return Message{}, fmt.Errorf("paxos: send %s to %s: %w", msg.Kind, to, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return Message{}, fmt.Errorf("paxos: %s returned %s", to, resp.Status)
	}

	var reply Message
	if err := json.NewDecoder(resp.Body).Decode(&reply); err != nil {
		return Message{}, fmt.Errorf("paxos: decode reply from %s: %w", to, err)
	}
	return reply, nil
}

// Handler serves the protocol endpoint for one replica. Mount it on an
// https listener configured with ServerTLS so that only certificate holders
// can reach it.
func Handler(n *Node) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc(Path, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST only", http.StatusMethodNotAllowed)
			return
		}

		var msg Message
		if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
			http.Error(w, "bad message", http.StatusBadRequest)
			return
		}

		reply := n.Handle(r.Context(), msg)

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(reply); err != nil {
			return // the peer will treat the failure as unreachable and retry
		}
	})
	return mux
}
