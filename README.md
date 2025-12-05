# **🚀 DynamoLite++**

*A tiny-but-mighty distributed key-value store inspired by DynamoDB — built from scratch in Java + a full Python benchmarking harness.*

This project recreates the *core* ideas behind Amazon Dynamo/DynamoDB (vector clocks, consistent hashing, replication, Merkle anti-entropy, WAL durability) and then adds two modern twists:

### ⚡ **SLO-Driven Adaptive Consistency (SAC)**

Think of it as *latency-aware quorum selection*. Clients give a deadline → the system tries to pick the fastest replica set.

### 🔧 **Repair-Aware Anti-Entropy (RAAE)**

Merkle-tree background repair, but smarter: it prioritizes hot keys, higher-divergence leaves, and old mismatches.

And yes — it comes with a full Python 3.12 load generator that runs YCSB-style tests, chaos tests, anti-entropy visualizations, and SLO experiments… automatically.

---

# 🌟 Highlights

### 🏗️ **Full Dynamo-style KV Store (Java 21)**

* Vector clocks + sibling sets
* WAL + fsync durability (no lost writes)
* Deterministic snapshots
* gRPC replication
* Consistent hashing ring
* Merkle-tree anti-entropy snapshots
* Undertow HTTP API

### 🧪 **Python Experiment Framework**

* YCSB Workloads A/B/C (read-heavy, write-heavy, mixed)
* Latency/throughput plots (Plotly)
* Chaos testing: kill nodes, see who survives 😈
* Anti-entropy convergence graphs
* Staleness testing (RAW consistency)
* One command:

```bash
poetry run python -m src.runner --experiment all
```

### 🎯 **Why I Built This**

I wanted to understand **real distributed storage internals** beyond textbooks — clocks, replication, repair, concurrency bugs, divergence, tail latency.
This project forced me to implement those ideas *end-to-end*, from WAL → API → replication → experiments.

---

# 📁 Project Structure

```
.
├── server/                     # Java 21 distributed KV store
│   ├── src/main/java/io/dynlite/...
│   └── build.gradle.kts
│
├── dynlite-experiments/        # Python experiments
│   ├── src/
│   │   ├── client.py
│   │   ├── config.py
│   │   ├── experiments/
│   │   │   ├── functional.py
│   │   │   ├── chaos.py
│   │   │   ├── perf_ycsb.py
│   │   │   ├── sac.py
│   │   │   └── anti_entropy.py
│   │   └── runner.py
│   └── results/ (plots + logs auto-generated)
```

---

# ▶️ Running the Cluster

### **1. Build + Start the Java nodes**

```bash
cd server
./gradlew clean build

# Start nodes on 8080, 8081, 8082 (3 terminals)
java -jar build/libs/server.jar --port=8080
java -jar build/libs/server.jar --port=8081
java -jar build/libs/server.jar --port=8082
```

Each node exposes:

* `GET/PUT/DELETE /kv/<key>`
* `/debug/siblings/<key>`
* `/admin/anti-entropy/merkle-snapshot`

---

# 🧪 Running Experiments

### **Run everything (recommended):**

```bash
cd dynlite-experiments
poetry install
poetry run python -m src.runner --experiment all
```

This runs:

* ✔️ Functional correctness tests
* ✔️ Chaos tests
* ✔️ YCSB workloads
* ✔️ SLO latency profiling
* ✔️ Staleness experiments
* ✔️ Anti-entropy convergence
* ✔️ Generates **all plots** in `results/` automatically

---

# 📊 Example Outputs (Auto-Generated)

### 🔥 Throughput Scaling

Workload A/B/C under concurrency 4 → 256.

### 🌪️ Chaos Latency

Killing a node mid-load → system stays alive.

### 🌱 Anti-Entropy

All nodes converge to the same Merkle root.

### 🎯 SLO Latency

Tight vs relaxed deadlines.

### 🧊 Staleness

Read-after-write delay vs stale-read probability.

All plots are saved as PNG for easy embedding in reports.

---

# ⚠️ Assumptions & Limitations

This is a **teaching + experimental** implementation, not a production system.

* Running on localhost → no real network partitions.
* SAC control loop not fully implemented (only measurement hooks).
* RAAE prioritization implemented, but *actual repair RPCs are not*.
* Quorum logic simplified for project scope.
* Cluster membership is static.
* No LSM tree; values live in-memory + WAL.

These were intentional to keep focus on core Dynamo semantics, versioning, anti-entropy logic, and experimentation.

---

# 🔮 Future Extensions

Planned improvements (post-course):

* Full SAC algorithm (dynamic replica selection + hedged reads)
* Actual RAAE-driven repair calls
* Tunable R/W quorums per request
* Real network fault injection (tc/netem profiles)
* Background compaction + LSM storage
* Dynamic cluster membership
* Prometheus metrics + Grafana dashboards

---

# 🔗 Repo Links

* **Java KV Store:**
  [https://github.com/NefariousNiru/dynamo-lite](https://github.com/NefariousNiru/dynamo-lite)

* **Python Experiment Harness:**
  (same repo or submodule depending on your layout)

---

# 🙌 Credits

Built by **Nirupom Bose Roy** and **Riya Bangia**
for **CSCI 8790 – Distributed Systems**, University of Georgia.

Shoutout to:

* Amazon Dynamo (2007)
* DynamoDB paper + documentation
* Riak (Basho)
* Cassandra

---