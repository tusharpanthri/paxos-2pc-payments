# Browser control plane

How `cmd/cluster` works, the wire protocol the frontend depends on, and the
decisions behind both. Running and deploying it is covered in the
[README](../README.md#browser-control-plane).

## Wire protocol

One WebSocket at `GET /ws`. Any origin is accepted: the frontend is hosted on
a different origin, and the endpoint exposes only a throwaway in-memory
cluster. `GET /healthz` returns `200 ok`.

**Client → server**, one JSON object per command, the raw line as typed:

```json
{"cmd": "transfer tushar varun 25"}
```

**Server → client**, zero or more log and state frames, then **exactly one**
result frame:

```json
{"type":"log","level":"twopc","tag":"[2PC]","msg":"decision COMMIT tx1 (2/2 yes)"}
{"type":"log","level":"paxos","tag":"[s2]","msg":"ACCEPT slot=1 ballot=1.s2n0 s2n0 -> s2n1","from":"s2n0","to":"s2n1","kind":"accept"}
{"type":"state","shards":[{"shard":2,"leader":"s2n1","term":2,"quorum":2,"nodes":[{"id":"s2n0","alive":false,"role":"follower","applied":7}]}],"partitions":[],"accounts":[{"key":"tushar","shard":2,"balance":75}]}
{"type":"result","msg":"OK moved 25 from tushar to varun"}
```

- `from`, `to` and `kind` appear on a log frame only when it describes a
  message between two participants. Endpoints are replica ids, a shard name
  (`s0`) for a 2PC participant, or `2pc` for the coordinator. `kind` is one of
  `prepare`, `promise`, `accept`, `accepted`, `reject`, `drop` (Paxos) or
  `prepare`, `vote-yes`, `vote-no`, `commit`, `abort` (2PC). The frontend's map
  animates these; the terminal ignores them.
- A `state` frame is a full snapshot built from `Engine.Status`, and the map
  is redrawn from it each time. One is sent after the startup elections and
  before every command's result. Others are sent mid-command when a role
  changes or a fault is applied (`kill`, `revive`, `partition`, `heal`), so the
  map moves in step with the log. Mid-command snapshots can be dropped under
  backpressure like log lines, but the one before the result never is.
  `locked_by` is omitted when the account is not locked.

- `level` is one of a closed set, colour-coded by the frontend:
  `paxos`, `twopc`, `leader`, `net`, `error`, `client`, `info`
  ([`internal/logstream`](../internal/logstream/logstream.go)). The gateway
  downgrades anything else to `info` rather than send an unknown level.
- `tag` is a short origin marker: `[s0]`…, `[2PC]`, `[net]`, `[you]`, `[gw]`,
  `[log]`.
- The result `msg` is plain text, prefixed `OK `, `ABORT ` (an expected
  outcome: no quorum, insufficient funds, locked account) or `ERROR `
  (malformed or unknown command, unknown node, a 2PC left in doubt). Blank
  lines and the client-side commands `clear` and `demo` get an empty result.
- On connect, the server streams the startup elections unprompted and ends
  them with a result frame, so the prompt stays disabled until every shard
  has a leader.
- Commands on one connection run strictly one at a time. Log lines are
  spaced by `-pace` (default 110ms) in the writer, at the edge; nothing in
  consensus sleeps.
- Limits: 10 commands/s (burst 20), 4 KiB frames, 10 minutes idle, 32
  concurrent sessions, 256 accounts per shard.

## What a command does

| Command | Engine path |
|---|---|
| `put k v` | one Multi-Paxos round on `shard(k)`: `ACCEPT` to peers, `ACCEPTED` back, `COMMIT` once a majority holds it |
| `get k` | the read is itself proposed through the log, so it is linearizable: it sees every write acknowledged before it, even across a leader change |
| `transfer a b n` | same shard: one round of a `transfer` command. Different shards: [`twopc`](../internal/twopc/twopc.go): `BEGIN`, `PREPARE` on each shard (each a Paxos round; reaching quorum *is* that shard's YES), `decision`, then `COMMIT`/`ABORT` rounds |
| `kill n` | fault table marks `n` dead; if it led, a survivor in the majority campaigns immediately |
| `revive n` | clears the mark; the replica rejoins with its log and catches up from the leader |
| `partition g1 \| g2 …` | drop every message between groups; replicas named in no group form one more group together |
| `heal` | clear partitions; leaderless shards elect |
| `status` / `datastore` | operator views read from replicas' applied state (no Paxos round) |

Shard placement is unsalted FNV-1a mod shard count, so it is stable across
runs. The tour depends on it: `tushar` and `ram` land on s2, `varun` on s1.

**PREPARE on a write.** Multi-Paxos runs phase 1 once per leadership, not once
per command. So a `put` does not show fresh `PREPARE`/`PROMISE` messages.
Instead it says which ballot the leader already holds a majority's promise
under. Real `PREPARE`/`PROMISE` traffic appears whenever a leader is elected:
at startup, after `kill`, `partition` and `heal`. Printing a fake phase 1 on
every write would misstate the protocol the engine runs.

## Where the log lines come from

Nothing in `internal/paxos` knows it is being watched. Both transports report
each completed `Send` (request, reply, or the fault that dropped it) to an
observer. The engine formats `prepare` and `accept` exchanges and skips
heartbeats, catch-up fetches and probes, which fire several times a second. A
watcher polls replica roles to announce elections and step-downs. In gRPC mode
the traces buffer inside each node process, and the gateway drains them over
the control service before writing a command's result frame.

## Changes made to `internal/paxos`

The control plane exercises failure paths the bank deployment rarely hits, and
it surfaced real problems. All existing tests still pass.

- **Catch-up adopts the leader's entries.** A follower used to learn commits
  by marking slots committed and applying whatever it held locally. A replica
  can hold a value that was accepted but never chosen, such as an old leader
  cut off in a minority, so after healing it applied the wrong command and
  diverged. Followers now fetch the leader's entries for newly committed
  slots and overwrite any that differ before applying.
  (`TestMinorityLeaderStepsDownAndConverges` reproduces this.)
- **A failed write steps the leader down.** A write that missed quorum left a
  hole in the leader's log. The apply loop runs contiguously, so every later
  write committed and then waited forever behind that hole. The leader now
  steps down, and the next election's recovery pass fills the slot.
- **Check-quorum.** A leader that hears from no majority for an election
  timeout steps down. Without this, a leader isolated by a partition kept
  claiming leadership indefinitely.
- **Pre-vote probe.** A replica pings its peers before bumping its ballot, and
  campaigns only if a majority answers. This stops a dead or partitioned
  replica from inflating its ballot every timeout and unseating a healthy
  leader when it rejoins. Receiving a probe resets the receiver's election
  timer, as a `PREPARE` would. Without that, the probe's round trip opened a
  window for two candidates to duel, and a caller that retried a write
  rejected mid-duel applied it twice.
- **Additions:** `NewMemLog` (an in-memory `Log`, for per-connection
  clusters), and `Node.Campaign`, `Node.Promised` and `Node.Log`.

## Limitations

- **The 2PC coordinator is not replicated.** Its decision lives in the gateway
  process. That is harmless here, because the whole cluster shares that
  process. In gRPC mode, a gateway crash between the decision and the commits
  would leave locks held, as the bank gateway's README describes.
- **Retries are not idempotent across leader changes.** If a write's round is
  cut short by a new election, it can still be chosen, and the engine reports
  it as failed. The operator retrying it is at-least-once. The payments
  gateway avoids this with transaction ids; the demo's `put` does not.
- **State is memory only**, which is fine for a demo and deliberate: every
  connection starts clean.
