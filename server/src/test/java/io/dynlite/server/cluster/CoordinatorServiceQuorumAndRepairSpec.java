// file: server/src/test/java/io/dynlite/server/cluster/CoordinatorServiceQuorumAndRepairSpec.java
package io.dynlite.server.cluster;

import io.dynlite.core.CausalOrder;
import io.dynlite.core.VectorClock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Specs for:
 *  - Write quorum enforcement (W).
 *  - Read quorum enforcement (R).
 *  - Read-repair with vector clocks.
 *  - Consistency metrics (stale replicas, siblings).
 */
class CoordinatorServiceQuorumAndRepairSpec {

    @BeforeEach
    void resetMetrics() {
        ConsistencyMetrics.reset();
    }

    // ---------- helpers ----------

    private ClusterConfig threeNodeCluster(int N, int R, int W) {
        ClusterConfig.Node a = new ClusterConfig.Node("node-a", "localhost", 9001);
        ClusterConfig.Node b = new ClusterConfig.Node("node-b", "localhost", 9002);
        ClusterConfig.Node c = new ClusterConfig.Node("node-c", "localhost", 9003);
        return new ClusterConfig(
                "node-a",
                List.of(a, b, c),
                N,
                R,
                W,
                128 // vnodes
        );
    }

    /** Simple NodeClient stub with configurable behavior per instance. */
    private static final class StubNodeClient implements NodeClient {
        final String nodeId;

        boolean failPut;
        boolean failDelete;
        boolean failGet;

        NodeClient.PutResult putResult =
                new NodeClient.PutResult(false, 0L, Map.of());
        NodeClient.ReadResult readResult =
                new NodeClient.ReadResult(false, null, Map.of());

        int putCalls;
        int deleteCalls;
        int getCalls;

        StubNodeClient(String nodeId) {
            this.nodeId = nodeId;
        }

        @Override
        public PutResult put(String nodeId, String key, String valueBase64, String coordNodeId, String opId) {
            if (failPut) {
                throw new RuntimeException("put failure for " + this.nodeId);
            }
            putCalls++;
            return putResult;
        }

        @Override
        public PutResult delete(String nodeId, String key, String coordNodeId, String opId) {
            if (failDelete) {
                throw new RuntimeException("delete failure for " + this.nodeId);
            }
            deleteCalls++;
            return putResult;
        }

        @Override
        public ReadResult get(String nodeId, String key) {
            if (failGet) {
                throw new RuntimeException("get failure for " + this.nodeId);
            }
            getCalls++;
            return readResult;
        }
    }

    // ---------- tests ----------

    @Test
    void write_quorum_success_aggregates_clocks() {
        ClusterConfig cluster = threeNodeCluster(/*N*/3, /*R*/1, /*W*/2);
        RingRouting routing = new RingRouting(cluster);

        StubNodeClient a = new StubNodeClient("node-a");
        StubNodeClient b = new StubNodeClient("node-b");
        StubNodeClient c = new StubNodeClient("node-c");

        a.putResult = new NodeClient.PutResult(false, 1000L, Map.of("A", 1));
        b.putResult = new NodeClient.PutResult(false, 1100L, Map.of("A", 2));
        c.putResult = new NodeClient.PutResult(false, 1050L, Map.of("A", 1, "B", 1));

        CoordinatorService coord = new CoordinatorService(
                cluster,
                routing,
                Map.of(
                        "node-a", a,
                        "node-b", b,
                        "node-c", c
                )
        );

        CoordinatorService.Result res =
                coord.put("user:1", "dmFsdWU=", /*coordNodeId*/ null, null);

        // All three replicas succeeded; W=2 so quorum is met.
        assertEquals(3, a.putCalls + b.putCalls + c.putCalls);
        // lwwMillis is the max of all.
        assertEquals(1100L, res.lwwMillis());

        // Clock merged element-wise max: {A:2, B:1}
        assertEquals(2, res.clock().get("A"));
        assertEquals(1, res.clock().get("B"));
    }

    @Test
    void write_quorum_failure_throws_when_successes_lt_W() {
        ClusterConfig cluster = threeNodeCluster(/*N*/3, /*R*/1, /*W*/2);
        RingRouting routing = new RingRouting(cluster);

        StubNodeClient a = new StubNodeClient("node-a");
        StubNodeClient b = new StubNodeClient("node-b");
        StubNodeClient c = new StubNodeClient("node-c");

        // Only node-a will succeed; others throw on put.
        a.putResult = new NodeClient.PutResult(false, 1000L, Map.of("A", 1));
        b.failPut = true;
        c.failPut = true;

        CoordinatorService coord = new CoordinatorService(
                cluster,
                routing,
                Map.of(
                        "node-a", a,
                        "node-b", b,
                        "node-c", c
                )
        );

        IllegalStateException ex = assertThrows(
                IllegalStateException.class,
                () -> coord.put("user:2", "dmFsdWU=", null, null)
        );
        assertTrue(ex.getMessage().contains("write quorum failed"));
        assertEquals(1, a.putCalls);
        assertEquals(0, b.putCalls);
        assertEquals(0, c.putCalls);
    }

    @Test
    void read_quorum_with_stale_replica_triggers_read_repair_and_metrics() {
        // N=3, R=3, W=1 so GET will query all three replicas.
        ClusterConfig cluster = threeNodeCluster(/*N*/3, /*R*/3, /*W*/1);
        RingRouting routing = new RingRouting(cluster);

        StubNodeClient a = new StubNodeClient("node-a");
        StubNodeClient b = new StubNodeClient("node-b");
        StubNodeClient c = new StubNodeClient("node-c");

        // node-a has older clock: {N:1}, value="old"
        a.readResult = new NodeClient.ReadResult(
                true,
                "b2xk", // "old"
                Map.of("N", 1)
        );
        // node-b has newer clock: {N:2}, value="new"
        b.readResult = new NodeClient.ReadResult(
                true,
                "bmV3", // "new"
                Map.of("N", 2)
        );
        // node-c has no value (not found)
        c.readResult = new NodeClient.ReadResult(
                false,
                null,
                Map.of()
        );

        // putResult for repairs: we don't care about fields, just that put() is called.
        a.putResult = new NodeClient.PutResult(false, 2000L, Map.of("N", 2));
        b.putResult = new NodeClient.PutResult(false, 2000L, Map.of("N", 2));
        c.putResult = new NodeClient.PutResult(false, 2000L, Map.of("N", 2));

        CoordinatorService coord = new CoordinatorService(
                cluster,
                routing,
                Map.of(
                        "node-a", a,
                        "node-b", b,
                        "node-c", c
                )
        );

        CoordinatorService.Read r = coord.get("user:3");

        // Winner should be the newer version ("new").
        assertTrue(r.found());
        assertEquals("bmV3", r.base64());
        assertEquals(2, r.clock().get("N")); // winner's clock

        // node-a was stale relative to winner -> one repair put.
        assertEquals(1, a.putCalls);
        // node-b had the winner; should not be repaired.
        assertEquals(0, b.putCalls);

        // Metrics: one read, stale replicas > 0, no siblings.
        ConsistencyMetrics.Snapshot snap = ConsistencyMetrics.snapshot();
        assertEquals(1L, snap.totalReads());
        assertEquals(1L, snap.readsWithStaleReplicas());
        assertEquals(0L, snap.readsWithSiblings());
        // 3 replica contacts (a,b,c).
        assertEquals(3L, snap.totalReplicaContacts());
    }

    @Test
    void read_quorum_with_siblings_records_metrics() {
        // Two replicas with concurrent clocks -> siblings.
        ClusterConfig cluster = threeNodeCluster(/*N*/3, /*R*/3, /*W*/1);
        RingRouting routing = new RingRouting(cluster);

        StubNodeClient a = new StubNodeClient("node-a");
        StubNodeClient b = new StubNodeClient("node-b");
        StubNodeClient c = new StubNodeClient("node-c");

        // Concurrent clocks: {A:1} and {B:1}.
        a.readResult = new NodeClient.ReadResult(
                true,
                "djE=", // "v1"
                Map.of("A", 1)
        );
        b.readResult = new NodeClient.ReadResult(
                true,
                "djI=", // "v2"
                Map.of("B", 1)
        );
        c.readResult = new NodeClient.ReadResult(
                false,
                null,
                Map.of()
        );

        CoordinatorService coord = new CoordinatorService(
                cluster,
                routing,
                Map.of(
                        "node-a", a,
                        "node-b", b,
                        "node-c", c
                )
        );

        CoordinatorService.Read r = coord.get("user:4");

        // Winner is deterministic (lexicographically smallest nodeId among maximals).
        // In this setup, both a and b are maximal; winner is node-a.
        assertTrue(r.found());
        // Should match a's value "v1".
        assertEquals("djE=", r.base64());
        assertEquals(1, r.clock().getOrDefault("A", 0));
        assertEquals(0, r.clock().getOrDefault("B", 0));

        // Clocks are concurrent, so neither is strictly stale; no repairs.
        assertEquals(0, a.putCalls);
        assertEquals(0, b.putCalls);

        // Metrics: siblings>0, stale=0.
        ConsistencyMetrics.Snapshot snap = ConsistencyMetrics.snapshot();
        assertEquals(1L, snap.totalReads());
        assertEquals(0L, snap.readsWithStaleReplicas());
        assertEquals(1L, snap.readsWithSiblings());
        assertEquals(3L, snap.totalReplicaContacts());
    }
}
