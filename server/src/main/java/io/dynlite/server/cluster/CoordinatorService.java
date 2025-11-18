// file: server/src/main/java/io/dynlite/server/cluster/CoordinatorService.java
package io.dynlite.server.cluster;

import io.dynlite.core.VectorClock;

import java.util.*;

/**
 * Cluster-aware coordinator sitting above per-node KvService.
 *
 * Responsibilities:
 *  - For a key, compute its replica set via consistent hashing (RingRouting).
 *  - Fan out PUT/DELETE to N replicas using NodeClient, enforcing write quorum W.
 *  - On GET, read from R replicas, reconcile clocks and optionally perform read-repair.
 *  - Track basic consistency metrics (stale replicas, siblings).
 *
 * This matches the project proposal's layered design:
 *  Storage engine (DurableStore + KvService) <- CoordinatorService <- HTTP API.
 */
public class CoordinatorService {

    private final ClusterConfig cluster;
    private final RingRouting routing;
    private final Map<String, NodeClient> clients;
    private final String localNodeId;

    private final int N; // replication factor
    private final int R; // read quorum
    private final int W; // write quorum

    public CoordinatorService(
            ClusterConfig cluster,
            RingRouting routing,
            Map<String, NodeClient> clients
    ) {
        this.cluster = Objects.requireNonNull(cluster, "cluster");
        this.routing = Objects.requireNonNull(routing, "routing");
        this.clients = Map.copyOf(Objects.requireNonNull(clients, "clients"));
        this.localNodeId = cluster.localNodeId();

        this.N = cluster.replicationFactor();
        this.R = cluster.readQuorum();
        this.W = cluster.writeQuorum();

        if (R <= 0 || W <= 0 || N <= 0) {
            throw new IllegalArgumentException("N/R/W must be > 0");
        }
        if (R > N || W > N) {
            throw new IllegalArgumentException("Must satisfy R <= N and W <= N");
        }

        // Defensive: ensure we have a client for every cluster node we know about.
        for (ClusterConfig.Node n : cluster.nodes()) {
            if (!this.clients.containsKey(n.nodeId())) {
                throw new IllegalArgumentException(
                        "Missing NodeClient for nodeId=" + n.nodeId()
                );
            }
        }
    }

    // ---------- public API used by WebServer ----------

    /**
     * Cluster-wide PUT for a key with write quorum enforcement.
     *
     * Steps:
     *  - Compute replica set via hash ring.
     *  - For each replica nodeId:
     *      nodeClient.put(nodeId, key, valueBase64, coordNodeIdOrLocal()).
     *  - Count successful writes.
     *  - If successes >= W:
     *      - Aggregate lwwMillis = max over successful replicas.
     *      - Aggregate vectorClock = element-wise max over successful replicas.
     *    Else:
     *      - Throw IllegalStateException to signal write quorum failure.
     */
    public Result put(String key, String valueBase64, String coordNodeId) {
        String writerId = effectiveWriterId(coordNodeId);
        List<String> replicas = routing.replicaSetForKey(key);

        long maxLww = Long.MIN_VALUE;
        Map<String, Integer> mergedClock = new HashMap<>();
        int successes = 0;
        Exception lastFailure = null;

        for (String nodeId : replicas) {
            NodeClient client = clients.get(nodeId);
            try {
                NodeClient.PutResult r = client.put(nodeId, key, valueBase64, writerId);
                successes++;
                maxLww = Math.max(maxLww, r.lwwMillis());
                mergeClock(mergedClock, r.vectorClock());
            } catch (IllegalArgumentException e) {
                throw e;
            } catch (Exception e) {
                lastFailure = e;
            }
        }

        if (successes < W) {
            throw new IllegalStateException("write quorum failed for key=" + key +
                    " successes=" + successes + " W=" + W, lastFailure);
        }

        if (maxLww == Long.MIN_VALUE) {
            // Should not happen unless replica set is empty, which ClusterConfig prevents.
            maxLww = System.currentTimeMillis();
        }

        return new Result(false, maxLww, mergedClock);
    }

    /**
     * Cluster-wide DELETE (tombstone) for a key with write quorum enforcement.
     *
     * Same pattern as PUT but with tombstone=true at the storage layer.
     */
    public Result delete(String key, String coordNodeId) {
        String writerId = effectiveWriterId(coordNodeId);
        List<String> replicas = routing.replicaSetForKey(key);

        long maxLww = Long.MIN_VALUE;
        Map<String, Integer> mergedClock = new HashMap<>();
        int successes = 0;
        Exception lastFailure = null;

        for (String nodeId : replicas) {
            NodeClient client = clients.get(nodeId);
            try {
                NodeClient.PutResult r = client.delete(nodeId, key, writerId);
                successes++;
                maxLww = Math.max(maxLww, r.lwwMillis());
                mergeClock(mergedClock, r.vectorClock());
            } catch (IllegalArgumentException e) {
                throw e;
            } catch (Exception e) {
                lastFailure = e;
            }
        }

        if (successes < W) {
            throw new IllegalStateException("delete quorum failed for key=" + key +
                    " successes=" + successes + " W=" + W, lastFailure);
        }

        if (maxLww == Long.MIN_VALUE) {
            maxLww = System.currentTimeMillis();
        }

        return new Result(true, maxLww, mergedClock);
    }

    /**
     * Cluster-wide GET for a key with read quorum enforcement and read-repair.
     *
     * Behavior:
     *  - If R == 1: read from first replica (matches old behavior, no read-repair).
     *  - If R > 1:
     *      - Read from the first min(R, N) replicas.
     *      - Discard failures (exceptions) for quorum counting.
     *      - If successes == 0: return found=false.
     *      - If successes < R: throw IllegalStateException (read quorum failure).
     *      - Reconcile versions using vector clocks:
     *          * Determine maximal set (siblings).
     *          * Choose a single "winner" to return (deterministic).
     *          * For stale replicas (dominated by winner), issue read-repair writes.
     *      - Record consistency metrics: stale replicas, siblings, below-quorum.
     */
    public Read get(String key) {
        List<String> replicas = routing.replicaSetForKey(key);

        if (R == 1) {
            // Old behavior: primary-only read.
            String primary = replicas.getFirst();
            NodeClient client = clients.get(primary);

            NodeClient.ReadResult r = client.get(primary, key);
            if (!r.found()) {
                ConsistencyMetrics.recordRead(false, 0, false, false, false);
                return new Read(false, null, Map.of());
            }
            ConsistencyMetrics.recordRead(true, 1, false, false, false);
            return new Read(true, r.valueBase64(), r.vectorClock());
        }

        // R > 1: quorum read with reconciliation.
        int numToQuery = Math.min(R, replicas.size());
        List<ReplicaVersion> versions = new ArrayList<>(numToQuery);
        int successes = 0;
        boolean anyFound = false;
        boolean belowQuorum = false;

        for (int i = 0; i < numToQuery; i++) {
            String nodeId = replicas.get(i);
            NodeClient client = clients.get(nodeId);
            try {
                NodeClient.ReadResult r = client.get(nodeId, key);
                if (r.found()) {
                    anyFound = true;
                    successes++;
                    versions.add(new ReplicaVersion(
                            nodeId,
                            r.valueBase64(),
                            new VectorClock(r.vectorClock())
                    ));
                } else {
                    successes++;
                    // found=false, but we still count this as a successful contact
                    versions.add(new ReplicaVersion(
                            nodeId,
                            null,
                            new VectorClock(Map.of())
                    ));
                }
            } catch (IllegalArgumentException e) {
                throw e;
            } catch (Exception e) {
                // treat as failure to reach this replica
                System.out.println("Internal Error: " + e.toString());
            }
        }

        if (!anyFound) {
            // No replica had a value; not found is consistent, even if some failed.
            ConsistencyMetrics.recordRead(false, successes, false, false, successes < R);
            if (successes < R) {
                throw new IllegalStateException("read quorum failed for key=" + key +
                        " successes=" + successes + " R=" + R);
            }
            return new Read(false, null, Map.of());
        }

        if (successes < R) {
            belowQuorum = true;
            throw new IllegalStateException("read quorum failed for key=" + key +
                    " successes=" + successes + " R=" + R);
        }

        // Filter out replicas where value is null and clock is empty (pure "no value" views).
        List<ReplicaVersion> live = new ArrayList<>();
        for (ReplicaVersion rv : versions) {
            if (rv.valueBase64 != null) {
                live.add(rv);
            }
        }

        // Compute maximal set under vector-clock partial order (siblings).
        List<ReplicaVersion> maximals = maximalVersions(live);

        boolean hasSiblings = maximals.size() > 1;

        // Choose winner deterministically:
        //   - if 1 maximal -> that.
        //   - if >1 -> pick lexicographically smallest nodeId.
        ReplicaVersion winner = chooseWinner(maximals);

        // Read-repair: for each replica with stale version, push winner's value.
        int staleReplicas = 0;
        for (ReplicaVersion rv : live) {
            if (isStale(rv.clock, winner.clock)) {
                staleReplicas++;
                try {
                    NodeClient client = clients.get(rv.nodeId);
                    // Use localNodeId as coord writer for repair writes.
                    client.put(rv.nodeId, key, winner.valueBase64, localNodeId);
                } catch (Exception e) {
                    // best-effort repair; ignore failures for now
                }
            }
        }

        ConsistencyMetrics.recordRead(
                true,
                successes,
                staleReplicas > 0,
                hasSiblings,
                belowQuorum
        );

        return new Read(true, winner.valueBase64, winner.clock.entries());
    }

    // ---------- view models returned to WebServer ----------

    public record Result(
            boolean tombstone,
            long lwwMillis,
            Map<String, Integer> clock
    ) {}

    public record Read(
            boolean found,
            String base64,
            Map<String, Integer> clock
    ) {}

    // ---------- helpers ----------

    /**
     * Decide which id to bump in vector clocks for this write.
     *
     * If client provided coordNodeId, we propagate that; otherwise we use the
     * local node id (coordinator as writer).
     */
    private String effectiveWriterId(String coordNodeId) {
        if (coordNodeId == null || coordNodeId.isBlank()) {
            return localNodeId;
        }
        return coordNodeId;
    }

    private static void mergeClock(Map<String, Integer> acc, Map<String, Integer> other) {
        for (Map.Entry<String, Integer> e : other.entrySet()) {
            String id = e.getKey();
            int cnt = e.getValue();
            acc.merge(id, cnt, Math::max);
        }
    }

    // --- vector-clock based reconciliation helpers ---

    private record ReplicaVersion(
            String nodeId,
            String valueBase64,
            VectorClock clock
    ) {}

    /**
     * Return all maximal versions under the vector-clock partial order.
     */
    private static List<ReplicaVersion> maximalVersions(List<ReplicaVersion> versions) {
        List<ReplicaVersion> maximals = new ArrayList<>();
        outer:
        for (int i = 0; i < versions.size(); i++) {
            ReplicaVersion vi = versions.get(i);
            for (int j = 0; j < versions.size(); j++) {
                if (i == j) continue;
                ReplicaVersion vj = versions.get(j);
                var order = vj.clock.compare(vi.clock);
                if (order == io.dynlite.core.CausalOrder.LEFT_DOMINATES_RIGHT) {
                    // vi is dominated by vj; skip vi
                    continue outer;
                }
            }
            maximals.add(vi);
        }
        return maximals;
    }

    /**
     * Pick a deterministic winner from a set of maximal versions.
     * If there is a single maximal, return it.
     * If there are multiple (siblings), pick the one with lexicographically smallest nodeId.
     */
    private static ReplicaVersion chooseWinner(List<ReplicaVersion> maximals) {
        if (maximals.isEmpty()) {
            throw new IllegalStateException("no maximal versions to choose from");
        }
        if (maximals.size() == 1) return maximals.getFirst();
        return maximals.stream()
                .min(Comparator.comparing(ReplicaVersion::nodeId))
                .orElseThrow();
    }

    /**
     * A replica is "stale" if its clock is strictly dominated by the winner's clock.
     */
    private static boolean isStale(VectorClock candidate, VectorClock winner) {
        return winner.compare(candidate) == io.dynlite.core.CausalOrder.LEFT_DOMINATES_RIGHT;
    }
}
