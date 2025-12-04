// file: server/src/main/java/io/dynlite/server/slo/AdaptiveQuorumPlanner.java
package io.dynlite.server.slo;

import io.dynlite.server.cluster.ClusterConfig;

import java.util.List;
import java.util.Objects;

/**
 * SLO-driven quorum planner.
 *
 * Responsibilities:
 *  - Take the ring-ordered replica list for a key.
 *  - Choose an effective read quorum R and write quorum W.
 *  - Optionally reorder replicas (e.g., fastest first via latency EWMA).
 *
 * Current prototype:
 *  - Uses baseline R/W from ClusterConfig.
 *  - For reads:
 *      * sorts replicas by EWMA latency ascending (fastest first),
 *      * keeps the same R.
 *  - For writes:
 *      * keeps ring order and baseline W.
 *
 * This is enough to plug into CoordinatorService and leave room
 * for future "true" adaptive logic (deadline-aware R/W).
 */
public final class AdaptiveQuorumPlanner {

    /**
     * Read quorum plan.
     *
     * @param replicas ordered list of replica nodeIds to query.
     * @param R        read quorum to enforce (0..replicas.size()).
     */
    public record ReadPlan(List<String> replicas, int R) {}

    /**
     * Write quorum plan.
     *
     * @param replicas ordered list of replica nodeIds to target.
     * @param W        write quorum to enforce (0..replicas.size()).
     */
    public record WritePlan(List<String> replicas, int W) {}

    private final ClusterConfig cluster;
    private final ReplicaLatencyTracker latencyTracker;

    public AdaptiveQuorumPlanner(ClusterConfig cluster,
                                 ReplicaLatencyTracker latencyTracker) {
        this.cluster = Objects.requireNonNull(cluster, "cluster");
        this.latencyTracker = Objects.requireNonNull(latencyTracker, "latencyTracker");
    }

    /**
     * Decide a read plan for this request.
     *
     * Inputs:
     *  - ringReplicas: ring-ordered replica set for the key.
     *  - hint: per-request consistency / deadline hint.
     *
     * Current behavior:
     *  - baseline R = cluster.readQuorum().
     *  - replicas are sorted by EWMA latency (fastest first).
     *  - R is clipped to replicas.size().
     */
    public ReadPlan planRead(List<String> ringReplicas, ConsistencyHint hint) {
        if (ringReplicas == null || ringReplicas.isEmpty()) {
            return new ReadPlan(List.of(), 0);
        }

        int baseR = cluster.readQuorum();
        int effectiveR = Math.min(baseR, ringReplicas.size());

        // Simple: fastest replicas first by EWMA latency.
        List<String> ordered = ringReplicas.stream()
                .sorted((a, b) -> {
                    double la = latencyOrInfinity(a);
                    double lb = latencyOrInfinity(b);
                    return Double.compare(la, lb);
                })
                .toList();

        return new ReadPlan(ordered, effectiveR);
    }

    /**
     * Decide a write plan.
     *
     * Inputs:
     *  - ringReplicas: ring-ordered replica set for the key.
     *
     * Current behavior:
     *  - baseline W = cluster.writeQuorum().
     *  - preserves ring order.
     */
    public WritePlan planWrite(List<String> ringReplicas) {
        if (ringReplicas == null || ringReplicas.isEmpty()) {
            return new WritePlan(List.of(), 0);
        }
        int baseW = cluster.writeQuorum();
        int effectiveW = Math.min(baseW, ringReplicas.size());
        return new WritePlan(List.copyOf(ringReplicas), effectiveW);
    }

    private double latencyOrInfinity(String nodeId) {
        double ewma = latencyTracker.estimateEwmaMillis(nodeId);
        if (Double.isNaN(ewma) || ewma <= 0.0) {
            // No data yet: push this replica to the tail.
            return Double.POSITIVE_INFINITY;
        }
        return ewma;
    }
}
