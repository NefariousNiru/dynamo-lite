// file: src/main/java/io/dynlite/cluster/CoordinatorService.java
package io.dynlite.cluster;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Cluster-aware coordinator sitting above per-node KvService.
 *
 * Responsibilities:
 *  - For a key, compute its replica set via consistent hashing (RingRouting).
 *  - Fan out PUT/DELETE to N replicas using NodeClient.
 *  - Aggregate vector clocks and lwwMillis from replica responses.
 *  - For now, GET reads from the first replica (R=1), matching single-node behavior.
 *
 * This matches the project proposal's layered design:
 *  Storage engine (DurableStore + KvService) <- CoordinatorService <- HTTP API.
 */
public final class CoordinatorService {

    private final ClusterConfig cluster;
    private final RingRouting routing;
    private final Map<String, NodeClient> clients;
    private final String localNodeId;

    public CoordinatorService(
            ClusterConfig cluster,
            RingRouting routing,
            Map<String, NodeClient> clients
    ) {
        this.cluster = Objects.requireNonNull(cluster, "cluster");
        this.routing = Objects.requireNonNull(routing, "routing");
        this.clients = Map.copyOf(Objects.requireNonNull(clients, "clients"));
        this.localNodeId = cluster.localNodeId();

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
     * Cluster-wide PUT for a key.
     *
     * Steps:
     *  - Compute replica set via hash ring.
     *  - For each replica nodeId:
     *      nodeClient.put(nodeId, key, valueBase64, coordNodeIdOrLocal()).
     *  - Aggregate lwwMillis = max over replicas.
     *  - Aggregate vectorClock = element-wise max over replicas.
     *
     * For now we assume all replicas succeed and W=N, which is fine in a
     * single-node configuration (N=1, W=1).
     */
    public Result put(String key, String valueBase64, String coordNodeId) {
        String writerId = effectiveWriterId(coordNodeId);
        List<String> replicas = routing.replicaSetForKey(key);

        long maxLww = Long.MIN_VALUE;
        Map<String, Integer> mergedClock = new HashMap<>();

        for (String nodeId : replicas) {
            NodeClient client = clients.get(nodeId);
            NodeClient.PutResult r = client.put(nodeId, key, valueBase64, writerId);

            maxLww = Math.max(maxLww, r.lwwMillis());
            mergeClock(mergedClock, r.vectorClock());
        }

        if (maxLww == Long.MIN_VALUE) {
            // Should not happen unless replica set is empty, which ClusterConfig prevents.
            maxLww = System.currentTimeMillis();
        }

        return new Result(false, maxLww, mergedClock);
    }

    /**
     * Cluster-wide DELETE (tombstone) for a key.
     *
     * Same pattern as PUT but with tombstone=true at the storage layer.
     */
    public Result delete(String key, String coordNodeId) {
        String writerId = effectiveWriterId(coordNodeId);
        List<String> replicas = routing.replicaSetForKey(key);

        long maxLww = Long.MIN_VALUE;
        Map<String, Integer> mergedClock = new HashMap<>();

        for (String nodeId : replicas) {
            NodeClient client = clients.get(nodeId);
            NodeClient.PutResult r = client.delete(nodeId, key, writerId);

            maxLww = Math.max(maxLww, r.lwwMillis());
            mergeClock(mergedClock, r.vectorClock());
        }

        if (maxLww == Long.MIN_VALUE) {
            maxLww = System.currentTimeMillis();
        }

        return new Result(true, maxLww, mergedClock);
    }

    /**
     * Cluster-wide GET for a key.
     *
     * For now:
     *  - Read from the first replica in the replica set (R=1).
     *  - Delegate to that node's KvService via NodeClient.
     *
     * This matches your current single-node behavior exactly.
     * Later, for multi-node + R>1, we can:
     *  - read from multiple replicas,
     *  - reconcile diverging clocks,
     *  - optionally write back repairs.
     */
    public Read get(String key) {
        List<String> replicas = routing.replicaSetForKey(key);
        String primary = replicas.getFirst();
        NodeClient client = clients.get(primary);

        NodeClient.ReadResult r = client.get(primary, key);
        if (!r.found()) {
            return new Read(false, null, Map.of());
        }
        return new Read(true, r.valueBase64(), r.vectorClock());
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
}
