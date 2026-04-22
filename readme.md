# kansi: A ZooKeeper Implementation in Go

A from-scratch implementation of the ZooKeeper coordination service, based on the Hunt et al. paper ("ZooKeeper: Wait-free coordination for Internet-scale systems").

This is a learning project. Correctness and clarity over performance.

## Scope

In scope:

- Data tree (in-memory znode hierarchy)
- Transaction log (write-ahead log, fsync'd)
- Fuzzy snapshots (non-blocking periodic snapshots)
- Request processor (request → idempotent transaction)
- Zab (leader election + broadcast + recovery)
- Sessions + ephemerals
- Watches
- Client-server protocol (TCP, async, FIFO)
- Leader/follower role machinery

Out of scope:

- ACLs
- Observer nodes
- Byzantine fault tolerance
- Wire-compatibility with real ZooKeeper clients
- Multi-ensemble partitioning
- Request throttling

## Architecture

```
              ┌──────────────────────────────┐
              │         Client Library        │
              └──────────────┬───────────────┘
                             │ TCP, async, FIFO
              ┌──────────────┴───────────────┐
              │       Client-Server Proto     │
              └──────────────┬───────────────┘
                             │
        ┌────────────────────┴────────────────────┐
        │                                          │
    [read]                                    [write]
        │                                          │
        ▼                                          ▼
  Local Replica                            Request Processor
  (data tree)                                      │
        ▲                                          ▼
        │                                        Zab
        │                                          │
        └────────── Replicated DB ◄────── atomic broadcast
                         ▲
                         │
                   TxnLog + Snapshots
```

## Build Order

Each stage produces a testable artifact. Don't move on until the current stage passes its own tests.

### Stage 1 — Data Tree

Single-process, in-memory, no persistence.

- `Znode` struct: `data []byte`, `version int64`, `cversion int64`, `aversion int64`, `ctime`, `mtime`, `ephemeralOwner SessionID`, `children map[string]*Znode`
- Path parsing + validation (`/a/b/c`)
- Operations: `Create`, `Delete`, `Exists`, `GetData`, `SetData`, `GetChildren`
- Version-checked conditional updates (reject on version mismatch)
- Sequential znode support (parent tracks a counter; suffix appended to name)

Tests: basic CRUD, version conflicts, sequential naming, tree traversal.

### Stage 2 — Transaction Log + Fuzzy Snapshots

Single-node durability.

- Append-only txn log, one record per committed transaction
- Record format: `{zxid, txnType, payload, checksum}`
- `fsync` before ack (the paper is explicit about this)
- Snapshot = depth-first scan writing znodes to disk, non-blocking (DFS happens while writes continue)
- Snapshot format: stream of `{path, data, metadata}` records with a header
- Recovery: load latest snapshot → replay txn log entries with `zxid >= snapshot.startZxid`
- Idempotency check at replay: skip if `txn.version <= znode.version`

Tests: kill -9 mid-write and verify recovery, recovery after fuzzy snapshot interleaved with writes, truncated log handling.

### Stage 3 — Request Processor

Transforms client requests into idempotent transactions.

- Input: client request (e.g. `SetData{path, data, expectedVersion}`)
- Computes **future state** (accounts for in-flight txns not yet applied)
- Output: transaction (`SetDataTXN{path, data, newVersion, mtime}`) or `ErrorTXN`
- Transactions are fully self-contained — no conditional logic at apply time
- Apply function is pure: `apply(tree, txn) → tree'`

Tests: version math under pipelined writes, error txn generation, replay determinism.

### Stage 4 — Single-Node Server + Client Protocol

A complete single-node ZooKeeper. Skip Zab for now — the server is its own "leader."

- TCP server, one goroutine per connection
- Length-prefixed framed protocol (custom, simple)
- Message types: `ConnectRequest`, `CreateRequest`, `GetDataRequest`, etc., each with a response
- Every response carries the current `zxid`
- Per-connection FIFO: requests processed in arrival order; async responses
- **Sessions**: `sessionID`, `timeout`, `lastHeartbeat`; heartbeat at `s/3`, server expires at `s`
- **Ephemeral znodes**: deleted when owning session expires
- **Watches**: map of `(path, eventType) → []sessionID`, fired on matching write, one-shot

Tests: full CRUD over the wire, session timeout drops ephemerals, watch fires exactly once, reconnect with `lastZxid` validation.

### Stage 5 — Zab Broadcast (static leader)

Get quorum-based broadcast working with a hardcoded leader. Election comes later.

- `zxid = (epoch << 32) | counter`
- Leader proposes: sends `Proposal{zxid, txn}` to all followers
- Followers: write to txn log (fsync), send `Ack{zxid}`
- Leader commits on majority acks, sends `Commit{zxid}`
- Followers apply on commit in zxid order
- FIFO delivery over TCP per leader→follower connection

Tests: 3-node cluster commits writes, follower crash doesn't stall quorum, follower rejoin catches up via log.

### Stage 6 — Leader Election (Fast Leader Election)

- Each server broadcasts its vote: `Vote{proposedLeader, proposedZxid, epoch}`
- Receiving a vote with a higher `(epoch, zxid, serverID)` tuple → update own vote and rebroadcast
- Converge when a majority agree on the same vote
- Trigger: on startup, or when leader heartbeat times out

Tests: election converges in clean startup, re-election on leader kill, split-vote resolves deterministically.

### Stage 7 — Zab Recovery (Discovery + Synchronization)

- **Discovery**: new leader collects `lastZxid` from a quorum of followers
- **Synchronization**: leader sends `DIFF` (log tail), `SNAP` (full snapshot), or `TRUNC` (roll back divergent suffix) to bring each follower in sync
- Epoch bump on new leader — all new proposals use new epoch
- Previous-leader proposals delivered before new-leader proposals

Tests: leader dies with uncommitted proposal at some followers, new leader reconciles; follower with divergent tail gets TRUNC'd correctly.

### Stage 8 — Session Migration

- Client reconnects to a different server with its `sessionID` and `lastZxid`
- New server blocks the reconnect until its local state has `zxid >= client.lastZxid`
- Session continues; watches that were server-local are lost (paper semantics)

Tests: kill follower the client is on, client reconnects elsewhere, session survives.

## Project Layout

```
kansi/
├── cmd/
│   ├── kansi-server/     # server binary
│   └── kansi-client/     # CLI client
├── internal/
│   ├── tree/             # Stage 1: data tree
│   ├── txnlog/           # Stage 2: WAL
│   ├── snapshot/         # Stage 2: fuzzy snapshots
│   ├── processor/        # Stage 3: request processor
│   ├── proto/            # wire protocol
│   ├── session/          # sessions + ephemerals
│   ├── watch/            # watch manager
│   ├── server/           # Stage 4: single-node server
│   ├── zab/              # Stage 5-7: broadcast, election, recovery
│   └── client/           # client library
├── testutil/             # test harness (cluster spin-up, fault injection)
└── README.md
```

## Key Invariants

These must hold at every stage. If a test shows one violated, stop and fix before moving on.

1. **Transaction log is truth.** In-memory state must be reconstructable from snapshot + log replay.
2. **Transactions are idempotent.** Replaying a committed txn produces the same result.
3. **zxid is monotonic.** Strictly increasing, never reused.
4. **fsync before ack.** No write is acknowledged until durable.
5. **FIFO per client.** Responses to a single client in submission order.
6. **Zab total order.** All replicas apply committed txns in the same zxid order.
7. **Linearizable writes.** Quorum-committed writes never revert.

## Testing Strategy

- **Unit tests** per package.
- **Integration tests** spin up in-process clusters (goroutine per node, in-memory transport for speed).
- **Fault injection**: crash a node mid-write, partition leader from quorum, drop/reorder messages at the transport layer.
- **Linearizability check**: record all client ops with timestamps, post-hoc verify a valid serial order exists. Use porcupine-style checker.

## Non-Goals

- Performance tuning (no benchmarks, no optimization passes)
- Production-ready code (no metrics, no TLS, minimal config)
- Full real-ZK compatibility
- Cross-language clients

## References

- Hunt, Konar, Junqueira, Reed — "ZooKeeper: Wait-free coordination for Internet-scale systems" (USENIX ATC 2010): https://www.usenix.org/legacy/event/atc10/tech/full_papers/Hunt.pdf
- Junqueira, Reed, Serafini — "Zab: High-performance broadcast for primary-backup systems" (DSN 2011): https://marcoserafini.github.io/papers/zab.pdf
- Reed, Junqueira — "A simple totally ordered broadcast protocol" (LADIS 2008): https://diyhpl.us/~bryan/papers2/distributed/distributed-systems/zab.totally-ordered-broadcast-protocol.2008.pdf
- Apache ZooKeeper source (reference only, don't copy): https://github.com/apache/zookeeper