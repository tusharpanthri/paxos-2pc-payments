package gateway

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"paxos-2pc-kvstore/internal/cluster"
	"paxos-2pc-kvstore/internal/logstream"
)

type frame struct {
	Type  string `json:"type"`
	Level string `json:"level"`
	Tag   string `json:"tag"`
	Msg   string `json:"msg"`
}

type client struct {
	t    *testing.T
	conn *websocket.Conn
}

func dial(t *testing.T) *client {
	t.Helper()
	gw, err := New(Options{
		NewCluster: func() (*cluster.Engine, error) {
			return cluster.NewInproc(cluster.Config{Shards: 3, NodesPerShard: 3, MaxKeysPerShard: 64})
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(gw.Handler())
	t.Cleanup(srv.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(srv.URL, "http")+"/ws", &websocket.DialOptions{
		HTTPHeader: http.Header{"Origin": []string{"https://someone-else.example"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.CloseNow() })
	c := &client{t: t, conn: conn}
	if _, res := c.until(); !strings.HasPrefix(res, "OK cluster up, 3/3") {
		t.Fatalf("greeting ended with %q", res)
	}
	return c
}

// until reads frames through the next result, checking each against the
// protocol, and returns the logs and the result message.
func (c *client) until() ([]frame, string) {
	c.t.Helper()
	var logs []frame
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		_, data, err := c.conn.Read(ctx)
		cancel()
		if err != nil {
			c.t.Fatalf("read: %v", err)
		}
		var f frame
		if err := json.Unmarshal(data, &f); err != nil {
			c.t.Fatalf("bad frame %s: %v", data, err)
		}
		switch f.Type {
		case "log":
			if !logstream.Valid(logstream.Level(f.Level)) {
				c.t.Fatalf("level %q is outside the protocol's closed set", f.Level)
			}
			logs = append(logs, f)
		case "result":
			return logs, f.Msg
		default:
			c.t.Fatalf("unknown frame type %q", f.Type)
		}
	}
}

func (c *client) run(cmd string) ([]frame, string) {
	c.t.Helper()
	payload, _ := json.Marshal(map[string]string{"cmd": cmd})
	if err := c.conn.Write(context.Background(), websocket.MessageText, payload); err != nil {
		c.t.Fatal(err)
	}
	return c.until()
}

func TestHealthz(t *testing.T) {
	gw, _ := New(Options{NewCluster: func() (*cluster.Engine, error) { return nil, nil }})
	rec := httptest.NewRecorder()
	gw.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("healthz = %d", rec.Code)
	}
}

func TestProtocolEndToEnd(t *testing.T) {
	c := dial(t)

	logs, res := c.run("put tushar 100")
	if res != "OK tushar=100 on s2" {
		t.Fatalf("put result %q", res)
	}
	joined := ""
	for _, l := range logs {
		joined += l.Level + " " + l.Tag + " " + l.Msg + "\n"
	}
	for _, want := range []string{"paxos [s2] ACCEPT slot=1", "paxos [s2] COMMIT slot=1"} {
		if !strings.Contains(joined, want) {
			t.Errorf("missing %q in\n%s", want, joined)
		}
	}

	c.run("put varun 10")
	if _, res := c.run("transfer tushar varun 500"); !strings.HasPrefix(res, "ABORT ") {
		t.Fatalf("overdraft result %q", res)
	}
	if _, res := c.run("transfer tushar varun 40"); res != "OK moved 40 from tushar to varun" {
		t.Fatalf("transfer result %q", res)
	}
	if _, res := c.run("get varun"); res != "OK varun=50" {
		t.Fatalf("get result %q", res)
	}

	cases := map[string]string{
		"frobnicate":                 "ERROR unknown command",
		"put onlykey":                "ERROR usage",
		"kill s9n9":                  "ERROR unknown node",
		"partition s0n0":             "ERROR partition needs",
		"clear":                      "",
		"kill s2n0":                  "OK s2n0 is down",
		"kill s2n1":                  "OK s2n1 is down",
		"put tushar 1":               "ABORT s2 cannot reach a majority",
		"revive s2n0":                "OK s2n0 is up",
		"partition s1n0 | s1n1 s1n2": "OK network split into 2 groups",
		"heal":                       "OK partitions removed",
		"status":                     "OK 3 shards, 3 writable",
	}
	order := []string{"frobnicate", "put onlykey", "kill s9n9", "partition s0n0", "clear", "kill s2n0", "kill s2n1", "put tushar 1", "revive s2n0", "partition s1n0 | s1n1 s1n2", "heal", "status"}
	for _, cmd := range order {
		if _, res := c.run(cmd); !strings.HasPrefix(res, cases[cmd]) {
			t.Errorf("%s -> %q, want prefix %q", cmd, res, cases[cmd])
		}
	}

	// A malformed frame still gets exactly one result.
	_ = c.conn.Write(context.Background(), websocket.MessageText, []byte("not json"))
	if _, res := c.until(); !strings.HasPrefix(res, "ERROR") {
		t.Fatalf("malformed frame -> %q", res)
	}
}
