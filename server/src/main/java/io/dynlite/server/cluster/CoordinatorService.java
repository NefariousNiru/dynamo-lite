// file: server/src/main/java/io/dynlite/server/cluster/CoordinatorService.java
package io.dynlite.server.cluster;

import io.dynlite.core.VectorClock;
import io.dynlite.server.slo.*;

import java.util.*;
import java.util.concurrent.*;

/**
 * Cluster-aware coordinator sitting above per-node KvService.
 *
 * Responsibilities:
 *  - For a key, compute its replica set via consistent hashing (RingRouting).
 *  - Fan out PUT/DELETE to replicas using NodeClient, enforcing write quorum.
 *  - On GET, read from replicas, reconcile clocks and optionally perform read-repair.
 *  - Track basic consistency metrics (stale replicas, siblings).
 *
 * SAC extensions:
 *  - AdaptiveQuorumPlanner:
 *      * chooses effective R/W and replica ordering.
 *  - ReplicaLatencyTracker:
 *      * per-replica latency EWMA + p95 for hedged reads.
 *  - StalenessBudgetTracker:
 *      * rolling window of budgeted reads, enforcing bound B.
 *  - SloMetrics:
 *      * SLO hit/miss and stale-read rates for safe vs budgeted reads.
 */
public class CoordinatorService {

    private final ClusterConfig cluster;
    private final RingRouting routing;
    private final Map<String, NodeClient> clients;
    private final String localNodeId;

    private final int N;      // replication factor
    private final int baseR;  // baseline read quorum
    private final int baseW;  // baseline write quorum

    private final AdaptiveQuorumPlanner quorumPlanner;
    private final ReplicaLatencyTracker latencyTracker;
    private final StalenessBudgetTracker stalenessBudget;
    private final SloMetrics sloMetrics;

    // Executor for hedged reads on the R == 1 path.
    private final ExecutorService hedgeExecutor;

    /**
     * Backwards-compatible constructor: builds SAC primitives internally.
     */
    public CoordinatorService(
            ClusterConfig cluster,
            RingRouting routing,
            Map<String, NodeClient> clients
    ) {
        this(
                cluster,
                routing,
                clients,
                // shared latency tracker across planner + coordinator
                new AdaptiveQuorumPlanner(
                        cluster,
                        new ReplicaLatencyTracker(0.3, 256)
                ),
                new ReplicaLatencyTracker(0.3, 256),
                new StalenessBudgetTracker(1024),
                new SloMetrics()
        );
    }

    /**
     * Constructor for tests: caller provides latency/budget/metrics, we derive planner.
     */
    public CoordinatorService(
            ClusterConfig cluster,
            RingRouting routing,
            Map<String, NodeClient> clients,
            ReplicaLatencyTracker latencyTracker,
            StalenessBudgetTracker stalenessBudget,
            SloMetrics sloMetrics
    ) {
        this(
                cluster,
                routing,
                clients,
                new AdaptiveQuorumPlanner(cluster, latencyTracker),
                latencyTracker,
                stalenessBudget,
                sloMetrics
        );
    }

    /**
     * Full constructor with explicit AdaptiveQuorumPlanner.
     *
     * This is what Main.java is calling:
     *
     *   new CoordinatorService(clusterConfig, routing, clients,
     *                          quorumPlanner, latencyTracker,
     *                          stalenessBudget, sloMetrics)
     */
    public CoordinatorService(
            ClusterConfig cluster,
            RingRouting routing,
            Map<String, NodeClient> clients,
            AdaptiveQuorumPlanner quorumPlanner,
            ReplicaLatencyTracker latencyTracker,
            StalenessBudgetTracker stalenessBudget,
            SloMetrics sloMetrics
    ) {
        this.cluster = Objects.requireNonNull(cluster, "cluster");
        this.routing = Objects.requireNonNull(routing, "routing");
        this.clients = Map.copyOf(Objects.requireNonNull(clients, "clients"));
        this.localNodeId = cluster.localNodeId();

        this.quorumPlanner = Objects.requireNonNull(quorumPlanner, "quorumPlanner");
        this.latencyTracker = Objects.requireNonNull(latencyTracker, "latencyTracker");
        this.stalenessBudget = Objects.requireNonNull(stalenessBudget, "stalenessBudget");
        this.sloMetrics = Objects.requireNonNull(sloMetrics, "sloMetrics");

        this.N = cluster.replicationFactor();
        this.baseR = cluster.readQuorum();
        this.baseW = cluster.writeQuorum();

        if (baseR <= 0 || baseW <= 0 || N <= 0) {
            throw new IllegalArgumentException("N/R/W must be > 0");
        }
        if (baseR > N || baseW > N) {
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

        this.hedgeExecutor = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "hedge-read-worker");
            t.setDaemon(true);
            return t;
        });
    }

    // ---------- public API used by WebServer ----------

    // ----- PUT -----

    /**
     * Backwards-compatible PUT (no SLO hints).
     *
     * Uses AdaptiveQuorumPlanner to choose:
     *  - write quorum W,
     *  - replica ordering.
     */
    public Result put(String key, String valueBase64, String coordNodeId, String opId) {
        String writerId = effectiveWriterId(coordNodeId);

        List<String> ringReplicas = routing.replicaSetForKey(key);
        AdaptiveQuorumPlanner.WritePlan plan =
                quorumPlanner.planWrite(ringReplicas);

        List<String> replicas = plan.replicas();
        int effectiveW = plan.W();

        long maxLww = Long.MIN_VALUE;
        Map<String, Integer> mergedClock = new HashMap<>();
        int successes = 0;
        Exception lastFailure = null;

        for (String nodeId : replicas) {
            NodeClient client = clients.get(nodeId);
            long start = System.nanoTime();
            try {
                NodeClient.PutResult r = client.put(nodeId, key, valueBase64, writerId, opId);
                successes++;
                maxLww = Math.max(maxLww, r.lwwMillis());
                mergeClock(mergedClock, r.vectorClock());
            } catch (IllegalArgumentException e) {
                throw e;
            } catch (Exception e) {
                lastFailure = e;
            } finally {
                recordLatencySample(nodeId, start);
            }
        }

        if (successes < effectiveW) {
            throw new IllegalStateException("write quorum failed for key=" + key +
                    " successes=" + successes + " W=" + effectiveW, lastFailure);
        }

        if (maxLww == Long.MIN_VALUE) {
            maxLww = System.currentTimeMillis();
        }

        return new Result(false, maxLww, mergedClock);
    }

    // ----- DELETE -----

    public Result delete(String key, String coordNodeId, String opId) {
        String writerId = effectiveWriterId(coordNodeId);

        List<String> ringReplicas = routing.replicaSetForKey(key);
        AdaptiveQuorumPlanner.WritePlan plan =
                quorumPlanner.planWrite(ringReplicas);

        List<String> replicas = plan.replicas();
        int effectiveW = plan.W();

        long maxLww = Long.MIN_VALUE;
        Map<String, Integer> mergedClock = new HashMap<>();
        int successes = 0;
        Exception lastFailure = null;

        for (String nodeId : replicas) {
            NodeClient client = clients.get(nodeId);
            long start = System.nanoTime();
            try {
                NodeClient.PutResult r = client.delete(nodeId, key, writerId, opId);
                successes++;
                maxLww = Math.max(maxLww, r.lwwMillis());
                mergeClock(mergedClock, r.vectorClock());
            } catch (IllegalArgumentException e) {
                throw e;
            } catch (Exception e) {
                lastFailure = e;
            } finally {
                recordLatencySample(nodeId, start);
            }
        }

        if (successes < effectiveW) {
            throw new IllegalStateException("delete quorum failed for key=" + key +
                    " successes=" + successes + " W=" + effectiveW, lastFailure);
        }

        if (maxLww == Long.MIN_VALUE) {
            maxLww = System.currentTimeMillis();
        }

        return new Result(true, maxLww, mergedClock);
    }

    // ----- GET -----

    /**
     * Backwards-compatible GET with no explicit SLO hint.
     */
    public Read get(String key) {
        return get(key, ConsistencyHint.none());
    }

    /**
     * SLO-aware GET:
     *  - uses ConsistencyHint to decide whether caller "wants" relaxed consistency,
     *  - AdaptiveQuorumPlanner chooses effective R and replica ordering,
     *  - enforces staleness budget B over a rolling window,
     *  - does hedged reads when effective R == 1 using per-replica p95 latency,
     *  - tracks SLO hit/miss and stale-read rates for safe vs budgeted reads.
     */
    public Read get(String key, ConsistencyHint hint) {
        long startNano = System.nanoTime();

        // Decide whether this request will actually be treated as "budgeted".
        boolean callerBudgeted = hint.allowStaleness();
        boolean usedBudget = callerBudgeted;
        ConsistencyHint effectiveHint = hint;

        if (callerBudgeted) {
            double maxB = hint.maxBudgetedFraction();
            if (!stalenessBudget.withinBudget(maxB)) {
                // Budget exhausted - upgrade this read back to safe.
                usedBudget = false;
                effectiveHint = hint.asSafeRead();
            }
        }

        List<String> ringReplicas = routing.replicaSetForKey(key);
        AdaptiveQuorumPlanner.ReadPlan plan =
                quorumPlanner.planRead(ringReplicas, effectiveHint);

        List<String> replicas = plan.replicas();
        int effectiveR = plan.R();

        if (effectiveR <= 0 || replicas.isEmpty()) {
            // No replicas to query; treat as miss.
            recordSloAndBudget(effectiveHint, usedBudget, false, startNano);
            return new Read(false, null, Map.of());
        }

        if (effectiveR == 1) {
            // Hedged read path.
            Read result = hedgedReadOne(key, replicas);
            // With R == 1 we cannot reliably detect staleness from multiple replicas.
            recordSloAndBudget(effectiveHint, usedBudget, false, startNano);
            return result;
        }

        // R > 1: quorum read with reconciliation and read-repair.
        int numToQuery = Math.min(effectiveR, replicas.size());
        List<ReplicaVersion> versions = new ArrayList<>(numToQuery);
        int successes = 0;
        boolean anyFound = false;
        boolean belowQuorum = false;

        for (int i = 0; i < numToQuery; i++) {
            String nodeId = replicas.get(i);
            NodeClient client = clients.get(nodeId);
            long start = System.nanoTime();
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
                    versions.add(new ReplicaVersion(
                            nodeId,
                            null,
                            new VectorClock(Map.of())
                    ));
                }
            } catch (IllegalArgumentException e) {
                throw e;
            } catch (Exception e) {
                System.out.println("Internal Error: " + e);
            } finally {
                recordLatencySample(nodeId, start);
            }
        }

        if (!anyFound) {
            // No replica had a value; not found is consistent, even if some failed.
            ConsistencyMetrics.recordRead(false, successes, false, false, successes < effectiveR);
            if (successes < effectiveR) {
                belowQuorum = true;
                // Treat as failure; do not record SLO hit/miss for this request.
                throw new IllegalStateException("read quorum failed for key=" + key +
                        " successes=" + successes + " R=" + effectiveR);
            }
            recordSloAndBudget(effectiveHint, usedBudget, false, startNano);
            return new Read(false, null, Map.of());
        }

        if (successes < effectiveR) {
            belowQuorum = true;
            throw new IllegalStateException("read quorum failed for key=" + key +
                    " successes=" + successes + " R=" + effectiveR);
        }

        // Filter out pure "no value" views.
        List<ReplicaVersion> live = new ArrayList<>();
        for (ReplicaVersion rv : versions) {
            if (rv.valueBase64 != null) {
                live.add(rv);
            }
        }

        // Compute maximal versions under vector-clock partial order.
        List<ReplicaVersion> maximals = maximalVersions(live);
        boolean hasSiblings = maximals.size() > 1;

        // Deterministic winner.
        ReplicaVersion winner = chooseWinner(maximals);

        // Read-repair stale replicas.
        int staleReplicas = 0;
        for (ReplicaVersion rv : live) {
            if (isStale(rv.clock, winner.clock)) {
                staleReplicas++;
                try {
                    NodeClient client = clients.get(rv.nodeId);
                    long startRepair = System.nanoTime();
                    try {
                        client.put(rv.nodeId, key, winner.valueBase64, localNodeId, null);
                    } finally {
                        recordLatencySample(rv.nodeId, startRepair);
                    }
                } catch (Exception e) {
                    // best-effort repair
                }
            }
        }

        boolean staleObserved = (staleReplicas > 0) || hasSiblings || belowQuorum;

        ConsistencyMetrics.recordRead(
                true,
                successes,
                staleReplicas > 0,
                hasSiblings,
                belowQuorum
        );

        recordSloAndBudget(effectiveHint, usedBudget, staleObserved, startNano);
        return new Read(true, winner.valueBase64, winner.clock.entries());
    }

    // ---------- view models ----------

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

    private String effectiveWriterId(String coordNodeId) {
        if (coordNodeId == null || coordNodeId.isBlank()) {
            return localNodeId;
        }
        return coordNodeId;
    }

    private void recordLatencySample(String nodeId, long startNano) {
        long end = System.nanoTime();
        double millis = (end - startNano) / 1_000_000.0;
        latencyTracker.recordSample(nodeId, millis);
    }

    private void recordSloAndBudget(
            ConsistencyHint hint,
            boolean usedBudget,
            boolean staleObserved,
            long startNano
    ) {
        double elapsedMs = (System.nanoTime() - startNano) / 1_000_000.0;
        sloMetrics.recordLatencyOutcome(hint, elapsedMs);
        sloMetrics.recordReadOutcome(usedBudget, staleObserved);
        stalenessBudget.recordRead(usedBudget);
    }

    // --- hedged read path (R == 1) ---

    private record ReplicaRead(
            String nodeId,
            boolean found,
            String valueBase64,
            Map<String, Integer> clock
    ) {}

    /**
     * Hedged read for R == 1:
     *  - primary = first replica in planner-ordered list,
     *  - if primary is slower than its predicted p95, fire a hedge to the next replica,
     *  - return the first completed result.
     */
    private Read hedgedReadOne(String key, List<String> replicas) {
        if (replicas.isEmpty()) {
            return new Read(false, null, Map.of());
        }

        String primaryId = replicas.getFirst();
        NodeClient primaryClient = clients.get(primaryId);

        double p95 = latencyTracker.estimateP95Millis(primaryId);
        boolean canHedge = replicas.size() > 1 && !Double.isNaN(p95) && p95 > 0.0;

        if (!canHedge) {
            // No useful stats or no hedge target; do a simple single-replica read.
            ReplicaRead rr = timedRead(primaryClient, primaryId, key);
            if (!rr.found) {
                ConsistencyMetrics.recordRead(false, 1, false, false, false);
                return new Read(false, null, Map.of());
            }
            ConsistencyMetrics.recordRead(true, 1, false, false, false);
            return new Read(true, rr.valueBase64, rr.clock);
        }

        String hedgeId = replicas.get(1);
        NodeClient hedgeClient = clients.get(hedgeId);

        CompletableFuture<ReplicaRead> primaryFuture =
                CompletableFuture.supplyAsync(() -> timedRead(primaryClient, primaryId, key), hedgeExecutor);

        try {
            // Try to get primary within its predicted p95.
            ReplicaRead rr = primaryFuture.get((long) p95, TimeUnit.MILLISECONDS);
            if (!rr.found) {
                ConsistencyMetrics.recordRead(false, 1, false, false, false);
                return new Read(false, null, Map.of());
            }
            ConsistencyMetrics.recordRead(true, 1, false, false, false);
            return new Read(true, rr.valueBase64, rr.clock);
        } catch (TimeoutException te) {
            // Primary is slower than p95 - fire hedge and race.
            CompletableFuture<ReplicaRead> hedgeFuture =
                    CompletableFuture.supplyAsync(() -> timedRead(hedgeClient, hedgeId, key), hedgeExecutor);

            CompletableFuture<ReplicaRead> winner =
                    primaryFuture.applyToEither(hedgeFuture, r -> r);

            ReplicaRead rr = winner.join();
            if (!rr.found) {
                ConsistencyMetrics.recordRead(false, 1, false, false, false);
                return new Read(false, null, Map.of());
            }
            ConsistencyMetrics.recordRead(true, 1, false, false, false);
            return new Read(true, rr.valueBase64, rr.clock);
        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException("hedged read failed for key=" + key, e);
        }
    }

    private ReplicaRead timedRead(NodeClient client, String nodeId, String key) {
        long start = System.nanoTime();
        try {
            NodeClient.ReadResult r = client.get(nodeId, key);
            return new ReplicaRead(nodeId, r.found(), r.valueBase64(), r.vectorClock());
        } finally {
            recordLatencySample(nodeId, start);
        }
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
                    continue outer;
                }
            }
            maximals.add(vi);
        }
        return maximals;
    }

    private static ReplicaVersion chooseWinner(List<ReplicaVersion> maximals) {
        if (maximals.isEmpty()) {
            throw new IllegalStateException("no maximal versions to choose from");
        }
        if (maximals.size() == 1) return maximals.getFirst();
        return maximals.stream()
                .min(Comparator.comparing(ReplicaVersion::nodeId))
                .orElseThrow();
    }

    private static boolean isStale(VectorClock candidate, VectorClock winner) {
        return winner.compare(candidate) == io.dynlite.core.CausalOrder.LEFT_DOMINATES_RIGHT;
    }
}
