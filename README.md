# Dynamo-Lite (Java) — Distributed, Durable, Tunable KV Store

A small-but-serious, Dynamo-inspired key–value store written in Java 21. It shards and replicates data across nodes, keeps writes durable with a write-ahead log, exposes **tunable consistency** (N/R/W), detects conflicts using **vector clocks** (per-key causal metadata), and heals divergence via **read-repair**, **hinted handoff**, and optional **anti-entropy** (Merkle trees). There’s no heavy UI—just a tiny HTTP surface for humans and a gRPC surface for node↔node replication.

---

## Why this exists (problem & goals)

* **High availability:** continue serving reads/writes when some replicas are down.
* **Clear guarantees:** make consistency and durability explicit and measurable.
* **Education + credibility:** demonstrate real distributed-systems tradeoffs in clean Java with production hygiene (observability, crash safety, fault injection).

---

## Guarantees (what you can rely on)

* **No lost acknowledged writes.** A write returns success **only after** its WAL record is flushed to disk (fsync) on enough replicas to satisfy **W**.
* **Tunable consistency.** You choose **N** (replication factor), **R** (read quorum), **W** (write quorum).

    * **Read-your-writes** holds in steady state when **R + W > N**.
* **Eventual convergence.** When the network is healthy long enough, all replicas of a key agree. Divergence is detected via vector clocks and healed by read-repair + anti-entropy.
* **At-least-once application; idempotent writes.** Each write carries an **opId** (UUID). Replays/retries don’t double-apply.

### Non-goals (by design)

* No multi-key transactions, secondary indexes, or cross-key constraints.
* No global linearizability or strict serializability.
* Not a replacement for consensus systems (Raft/Paxos) when you need single-writer total order.

---

## High-level architecture

* **Client HTTP API** (human-friendly) for `PUT/GET/DELETE /kv/{key}`. Values are bytes (Base64 in JSON).
* **Inter-node gRPC** (binary, efficient) for replication (`ReplicatePut`, `ReplicaGet`, etc.).
* **Coordinator path:** any node can accept a client request. It hashes the key onto a **consistent-hash ring** to pick **N** replicas, fans out writes, and waits for **W** acks.
* **Conflict detection:** per-key **vector clocks** (map `nodeId → counter`). We detect whether one version dominates another or if they’re **concurrent** (true conflict).
* **Conflict resolution (policy):** if versions are concurrent, the system returns **siblings** (both versions) plus a resolved value using a **pluggable policy** (default demo: **Last-Writer-Wins** by coordinator wall clock; documented caveat about clock skew).
* **Healing:**

    * **Read-repair:** on reads, push the dominating version to stale replicas.
    * **Hinted handoff:** if a replica is down during a write, another node queues a “hint” to deliver later.
    * **Anti-entropy (optional):** background reconciliation with **Merkle trees** (tree of hashes; compare roots → drill only differing branches).

### Storage (per node)

* **In-memory map** for fast reads.
* **Write-ahead log (WAL):** append record → **fsync** → only then ack.

    * Framing: small header (magic, version, length, **CRC32**) + payload.
    * On recovery: replay until first bad/truncated record (CRC protects tail).
* **Snapshots:** periodic full dump to bound recovery time; replay WAL only after last snapshot.
* **Idempotence:** per-write **opId** prevents double effects on retries/replay.

> **Term decoder:**
> **Vector clock** — per-key counters that encode causal history: who updated how many times. Lets us tell “newer vs concurrent” without synchronized clocks.
> **Consistent hashing** — stable key→node mapping that minimizes data movement when membership changes.
> **Merkle tree** — tree of hashes summarizing a set; comparing trees syncs only differences, not everything.

---

## API

### HTTP (human-friendly)

#### PUT `/kv/{key}`

Body:

```json
{ "valueBase64": "SGVsbG8=", "nodeId": "node-a" }
```

Response:

```json
{
  "ok": true,
  "vectorClock": {"node-a": 3, "node-c": 1},
  "lwwMillis": 1728000000000
}
```

#### GET `/kv/{key}`

Single winner:

```json
{
  "found": true,
  "valueBase64": "SGVsbG8=",
  "vectorClock": {"node-a": 3}
}
```

Concurrent versions (siblings):

```json
{
  "found": true,
  "siblings": [
    { "valueBase64": "djE=", "vectorClock": {"node-a": 3} },
    { "valueBase64": "djI=", "vectorClock": {"node-b": 1, "node-a": 2} }
  ],
  "resolved": { "valueBase64": "djI=", "resolvedBy": "LWW" }
}
```

#### DELETE `/kv/{key}`

Body:

```json
{ "nodeId": "node-a" }
```

Response:

```json
{ "ok": true, "tombstone": true, "vectorClock": {"node-a": 4} }
```

### gRPC (node↔node)

* `ReplicatePut(KeyRecord)` / `ReplicateDelete(KeyRecord)`
* `ReplicaGet(Key) -> Values[]`
  Payload mirrors `VersionedValue` (bytes, tombstone, vector clock, lwwMillis, opId).

---

## Configuration & deployment

* **Static cluster config** (default): `cluster.yaml`

```yaml
clusterId: "dynlite-local"
replicationFactor: 3
vnodesPerNode: 128
nodes:
  - id: "node-a"
    host: "127.0.0.1"
    httpPort: 8081
    grpcPort: 9091
    dataDir: "./data/node-a"
  - id: "node-b"
    host: "127.0.0.1"
    httpPort: 8082
    grpcPort: 9092
    dataDir: "./data/node-b"
  - id: "node-c"
    host: "127.0.0.1"
    httpPort: 8083
    grpcPort: 9093
    dataDir: "./data/node-c"
```

* **Membership:** static by default (simple and robust). Optional gossip can be enabled for auto-discovery (probabilistic push/pull heartbeats with suspicion timers).

* **Start locally (example):**

```bash
./gradlew :server:run --args="--node-id=node-a --config=cluster.yaml"
./gradlew :server:run --args="--node-id=node-b --config=cluster.yaml"
./gradlew :server:run --args="--node-id=node-c --config=cluster.yaml"
```

* **Docker Compose** (trimmed):

```yaml
services:
  node-a:
    image: dynamo-lite:latest
    command: ["--node-id=node-a","--config=/etc/dynlite/cluster.yaml"]
    ports: ["8081:8080","9091:9090"]
    volumes: ["./data/node-a:/var/lib/dynlite"]
  # node-b, node-c similar…
```

---

## How requests flow (with reasons)

### Write (PUT)

1. Coordinator hashes key → selects **N** owners on ring.
2. Bumps its entry in the **vector clock** (this is the new causal event).
3. Sends to all N; each replica **appends to WAL, fsyncs, applies to memory**; ack.
4. Coordinator returns success when **W** acks received.
5. If a replica was down, coordinator records a **hint** for later delivery (hinted handoff).

**Why:** fsync before ack gives durability; W acks give tunable write consistency; hints preserve eventual full replication.

### Read (GET)

1. Coordinator queries **R** replicas (often all N, return after R).
2. Compares **vector clocks**:

    * If one **dominates**, return it (and **read-repair** laggards).
    * If **concurrent**, return **siblings** and a resolved value (e.g., LWW).
3. Optional: **hedged reads** to cut tail latency (query extra replica if slow).

**Why:** separation of **detection** (vector clocks) from **policy** (resolver) is the right model; read-repair heals hot keys opportunistically.

### Recovery (after crash)

1. Load latest **snapshot** if present.
2. **Replay WAL** records in order; stop at first truncated/bad-CRC record (tail).
3. Apply only first-time **opId**s (idempotence).
4. Start serving; rotate WAL, continue snapshots.

**Why:** snapshot bounds recovery time; CRC ensures prefix-safety; opId protects from duplicates.

---

## Observability & ops

* **Metrics** (Micrometer): `put.latency`, `get.latency`, `wal.fsync.ms`, `handoff.queue.depth`, `repair.bytes`, `merkle.round.ms`. Export to Prometheus.
* **Logs**: structured JSON; every request carries `traceId`, `keyHashPrefix`, and summarized vector clock.
* **Health**: `/admin/health` (HTTP), `Admin/Health` (gRPC).
* **Ring**: `/admin/ring` shows vnode layout and ownership.
* **Chaos tools**: scripts for killing processes, injecting packet loss/delay, and simulating clock skew.

---

## Security (minimal but sane for a demo)

* **Auth token** (bearer) on HTTP write paths; read paths optional.
* **mTLS** optional on gRPC for inter-node auth.
* **ACL stubs** available to restrict key prefixes, if needed.

---

## Performance notes

* Writes are **sequential** (append-heavy), snapshots are off the hot path.
* Vnodes smooth shard load; add/remove a node moves only its arcs (bounded data motion).
* Tail latency dominates perceived performance—consider **hedged reads** and reasonable timeouts.

> **No magic numbers promised:** actual p50/p95/p99 depend on disk, fsync, CPU, GC, and network. Produce your own table with the included bench harness; don’t trust single runs.

---

## Limitations (be honest)

* **LWW policy** can drop a concurrent write under clock skew. This is acceptable for a demo; swap in a domain-aware resolver or CRDT where it matters.
* **Deletes use tombstones.** Until tombstone GC runs safely, storage grows.
* **Static membership** by default (simpler). Gossip is optional and intentionally conservative.
* **Single-key scope.** No cross-key transactions or secondary indexes.
* **One partitioned dataset.** No multi-tenant resource isolation.

---

## Roadmap (sensible next steps)

* **Pluggable resolvers** (e.g., register/add-wins sets) for specific keyspaces.
* **Adaptive anti-entropy** (prioritize cold/lagging shards first).
* **Hedged reads + retry budgets** for better tail behavior.
* **Range scans** (best-effort, eventually consistent) for small analytics.
* **Tiered storage** (RocksDB) behind the existing storage interface.
* **Gossip-driven membership** on by default, with clear suspicion SLAs.

---

## Demo script (copy/paste)

```bash
# 1) Start three nodes (different terminals)
./gradlew :server:run --args="--node-id=node-a --config=cluster.yaml"
./gradlew :server:run --args="--node-id=node-b --config=cluster.yaml"
./gradlew :server:run --args="--node-id=node-c --config=cluster.yaml"

# 2) Put and get
curl -X PUT localhost:8081/kv/user:1 -d '{"valueBase64":"aGVsbG8=","nodeId":"node-a"}' -H 'Content-Type: application/json'
curl localhost:8082/kv/user:1

# 3) Kill node-b, keep writing at W=2
# (simulate by Ctrl-C in its terminal, or docker stop)
curl -X PUT localhost:8081/kv/user:1 -d '{"valueBase64":"d29ybGQ=","nodeId":"node-a"}' -H 'Content-Type: application/json'

# 4) Bring node-b back, watch hinted handoff drain, then GET
curl localhost:8083/admin/health
curl localhost:8083/kv/user:1

# 5) Force a conflict (write from node-a and node-c concurrently), then GET to see siblings
```

---

## FAQ

**Why vector clocks instead of timestamps?**
Timestamps need synchronized clocks to detect “newer.” Vector clocks detect **causality** from data. We only use time to pick a **display winner** when updates are truly **concurrent**.

**Why not Raft?**
Raft gives a single total order (CP). Dynamo-style quorum is about **availability** with tunable consistency (AP). Different tools for different jobs.

**What happens if a replica is down for a long time?**
New writes still succeed (as long as W acks are met). The down node’s misses accumulate as **hints** and are replayed on return; **anti-entropy** cleans up cold keys that didn’t get read-repaired.

---

## Glossary (one-liners)

* **Vector clock:** per-key map `nodeId→counter` encoding causal history.
* **Dominates / concurrent:** clock A dominates B if A≥B element-wise and A≠B; otherwise they may be **concurrent** (independent updates).
* **Consistent hashing:** ring of tokens mapping keys to the next N owners clockwise; **vnodes** = multiple ring positions per physical node for load smoothing.
* **Quorum (R/W/N):** how many replicas must respond for reads/writes; **R+W>N** → read-your-writes (steady state).
* **WAL + fsync:** append log record and flush to disk **before** acknowledging a write.
* **Read-repair:** fix stale replicas during reads.
* **Hinted handoff:** buffer writes for down replicas and replay when they return.
* **Merkle tree:** tree of hashes to synchronize only the differences between replicas.

---

## License & contributions

MIT-style license. PRs welcome—especially for additional resolvers, RocksDB backend, hedged reads, and better chaos tooling. Please include tests and a short design note explaining tradeoffs.

---

**Bottom line:** Dynamo-Lite is a compact, readable, fault-tolerant KV store that demonstrates real distributed-systems thinking: explicit guarantees, principled conflict handling, durable writes, and measured tradeoffs. Use it to learn, to prototype ideas, or to showcase engineering rigor.
