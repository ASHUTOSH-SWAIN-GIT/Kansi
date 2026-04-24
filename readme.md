# kansi

A small ZooKeeper-style coordination service in Go. From-scratch implementation of the Hunt et al. paper, scoped down to the parts that make ZooKeeper *ZooKeeper*: a replicated, watchable, hierarchical store.

Learning project. Correctness over performance.

## What's in

- Hierarchical znode tree
- Write-ahead log (fsync per record)
- Fuzzy snapshots + log replay recovery
- Sessions, ephemeral znodes, one-shot watches
- TCP client/server protocol
- Zab broadcast across multiple nodes (static leader)

## What's out

- Leader election — leader is configured, no failover
- Zab recovery (DIFF/SNAP/TRUNC)
- Session migration across nodes
- ACLs, observer nodes, real-ZK wire compat

## Layout

```
cmd/
├── kansi-server/     # node binary
└── kansi-client/     # CLI
internal/
├── tree/             # znode hierarchy
├── txn/              # transaction types + pure Apply
├── txnlog/           # WAL
├── snapshot/         # DFS snapshot
├── recovery/         # snapshot + log replay
├── processor/        # request → concrete txn
├── proto/            # wire format
├── session/          # sessions + ephemerals
├── watch/            # one-shot watches
├── server/           # TCP server, ties everything together
├── client/           # client library
└── zab/              # broadcast, static leader
```

## Run it

```bash
# single node
go run ./cmd/kansi-server -addr :9000 -data ./data

# in another shell
go run ./cmd/kansi-client -addr :9000 create /foo hello
go run ./cmd/kansi-client -addr :9000 get /foo
go run ./cmd/kansi-client -addr :9000 set /foo world
go run ./cmd/kansi-client -addr :9000 ls /
```

For a 3-node replicated cluster, see the test setup in [`internal/zab/zab_test.go`](internal/zab/zab_test.go).

## How writes flow

```
client → server → processor → committer → tree.Apply → fire watches → respond
```

The `committer` is the swap point:
- Standalone: append to WAL, fsync, return.
- Leader: zab broadcast, wait for quorum, return.
- Follower: reject (writes go to the leader).

Followers receive replicated txns through the zab follower goroutine, which applies them to the local tree.

## Key invariants

1. Transaction log is truth — state is reconstructable from log replay.
2. Transactions are idempotent — replay is safe.
3. zxid is strictly monotonic, never reused.
4. fsync before ack — no write is acknowledged until durable.
5. FIFO per client — responses arrive in submission order.
6. Total order across replicas — every node applies committed txns in the same zxid order.

## Tests

```bash
go test ./...
```

End-to-end coverage:
- CRUD over the wire
- Watches fire exactly once
- Ephemerals dropped when sessions expire
- Crash-mid-write log recovery
- 3-node replication, follower catch-up via history replay

## Dev

Pre-commit hooks (gofmt + golangci-lint) via [lefthook](https://lefthook.dev):

```bash
brew install lefthook golangci-lint
lefthook install
```

## References

- Hunt, Konar, Junqueira, Reed — *ZooKeeper: Wait-free coordination for Internet-scale systems* (USENIX ATC 2010)
- Junqueira, Reed, Serafini — *Zab: High-performance broadcast for primary-backup systems* (DSN 2011)
