// file: src/test/java/io/dynlite/server/cluster/CoordinatorServiceMultiNodeSpec.java
package io.dynlite.server.cluster;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * CoordinatorService behavior in a multi-node cluster.
 *
 * Focus:
 *  - fan-out PUT to all replicas,
 *  - aggregate lwwMillis as max,
 *  - aggregate vector clocks as elementwise max.
 *
 * We use a scripted NodeClient that returns pre-defined results,
 * so we can assert CoordinatorService's aggregation logic precisely.
 */
class CoordinatorServiceMultiNodeSpec {

    @Test
    void put_merges_lww_and_vector_clocks_across_replicas() {
        // --- arrange: 2-node cluster with N=2 ---
        ClusterConfig.Node n1 = new ClusterConfig.Node("node-a", "a.example", 8081);
        ClusterConfig.Node n2 = new ClusterConfig.Node("node-b", "b.example", 8082);
        ClusterConfig cluster = new ClusterConfig(
                "node-a",
                List.of(n1, n2),
                /*N*/ 2,
                /*R*/ 1,
                /*W*/ 1,   // W not used yet, but kept for consistency
                /*vnodes*/ 32
        );
        RingRouting routing = new RingRouting(cluster);

        // Scripted NodeClient:
        //  - For each nodeId, we predefine the PutResult to return.
        Map<String, NodeClient.PutResult> scriptedPuts = Map.of(
                "node-a", new NodeClient.PutResult(
                        false,
                        /*lwwMillis*/ 1000L,
                        Map.of("node-a", 2)
                ),
                "node-b", new NodeClient.PutResult(
                        false,
                        /*lwwMillis*/ 1500L,
                        Map.of("node-b", 3)
                )
        );

        ScriptedNodeClient client = new ScriptedNodeClient(scriptedPuts);
        CoordinatorService coord = new CoordinatorService(
                cluster,
                routing,
                Map.of(
                        "node-a", client,
                        "node-b", client
                )
        );

        String key = "user:99";
        String valueB64 = "Zm9v"; // "foo"

        // --- act ---
        CoordinatorService.Result res = coord.put(key, valueB64, /*coordId*/ "coordinator-1", null);

        // --- assert: lwwMillis is max over replicas ---
        assertEquals(1500L, res.lwwMillis(), "lwwMillis must be max over replica responses");

        // --- assert: vector clock is elementwise max over replicas ---
        Map<String, Integer> clock = res.clock();
        assertEquals(2, clock.getOrDefault("node-a", 0));
        assertEquals(3, clock.getOrDefault("node-b", 0));
        assertEquals(2, clock.size(), "no extra entries expected");
    }

    /**
     * NodeClient that returns pre-defined PutResult per nodeId,
     * and a trivial ReadResult (not used in this test).
     */
    private static final class ScriptedNodeClient implements NodeClient {
        private final Map<String, PutResult> puts;

        ScriptedNodeClient(Map<String, PutResult> puts) {
            this.puts = puts;
        }

        @Override
        public PutResult put(String nodeId, String key, String valueB64, String coordId, String opId) {
            PutResult r = puts.get(nodeId);
            if (r == null) {
                throw new IllegalArgumentException("No scripted PutResult for nodeId=" + nodeId);
            }
            return r;
        }

        @Override
        public PutResult delete(String nodeId, String key, String coordId, String opId) {
            throw new UnsupportedOperationException("delete not used in this test");
        }

        @Override
        public ReadResult get(String nodeId, String key) {
            // For this test we don't care about reads.
            return new ReadResult(false, null, Map.of());
        }
    }
}