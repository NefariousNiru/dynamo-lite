// file: src/test/java/io/dynlite/server/cluster/CoordinatorServiceSingleNodeSpec.java
package io.dynlite.server.cluster;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/**
 * CoordinatorService behavior in a single-node cluster (N=1).
 *
 * This matches your current deployment:
 *  - one node,
 *  - N=R=W=1,
 *  - coordinator is essentially a thin wrapper over the local node client.
 */
class CoordinatorServiceSingleNodeSpec {

    @Test
    void put_then_get_roundtrip_on_single_node() {
        // --- arrange: cluster with one node ---
        String nodeId = "node-a";
        ClusterConfig.Node n = new ClusterConfig.Node(nodeId, "localhost", 8080);
        ClusterConfig cluster = new ClusterConfig(
                nodeId,
                List.of(n),
                /*N*/ 1,
                /*R*/ 1,
                /*W*/ 1,
                /*vnodes*/ 16
        );
        RingRouting routing = new RingRouting(cluster);

        // Fake in-memory NodeClient that:
        //  - stores valueBase64 per key
        //  - increments a logical clock per write
        FakeSingleNodeClient client = new FakeSingleNodeClient(nodeId);
        CoordinatorService coord = new CoordinatorService(
                cluster,
                routing,
                Map.of(nodeId, client)
        );

        String key = "user:42";
        String valueB64 = "d29ybGQ="; // "world"
        String coordId = null;        // let coordinator default to nodeId

        // --- act: PUT via coordinator ---
        CoordinatorService.Result putRes = coord.put(key, valueB64, coordId);

        // --- assert: NodeClient saw the write and stored the value ---
        assertEquals(valueB64, client.storedValue(key), "client must store the value");
        assertFalse(putRes.tombstone());
        assertEquals(1, putRes.clock().getOrDefault(nodeId, 0), "clock for node-a should be 1");

        // --- act: GET via coordinator ---
        CoordinatorService.Read read = coord.get(key);

        // --- assert: roundtrip ---
        assertTrue(read.found());
        assertEquals(valueB64, read.base64());
        assertEquals(putRes.clock(), read.clock());
    }

    /**
     * Minimal in-memory single-node NodeClient for testing.
     * Each PUT:
     *  - overwrites the stored value for the key,
     *  - increments an internal logical counter as lwwMillis,
     *  - returns a simple vector clock { nodeId: counter }.
     */
    private static final class FakeSingleNodeClient implements NodeClient {
        private final String nodeId;
        private final AtomicLong clock = new AtomicLong();
        private final java.util.Map<String, String> kv = new java.util.concurrent.ConcurrentHashMap<>();

        FakeSingleNodeClient(String nodeId) {
            this.nodeId = nodeId;
        }

        @Override
        public PutResult put(String nodeId, String key, String valueB64, String coordId) {
            ensureLocal(nodeId);
            kv.put(key, valueB64);
            long ts = clock.incrementAndGet();
            return new PutResult(false, ts, Map.of(this.nodeId, (int) ts));
        }

        @Override
        public PutResult delete(String nodeId, String key, String coordId) {
            ensureLocal(nodeId);
            kv.remove(key);
            long ts = clock.incrementAndGet();
            return new PutResult(true, ts, Map.of(this.nodeId, (int) ts));
        }

        @Override
        public ReadResult get(String nodeId, String key) {
            ensureLocal(nodeId);
            String v = kv.get(key);
            if (v == null) {
                return new ReadResult(false, null, Map.of());
            }
            long ts = clock.get();
            return new ReadResult(true, v, Map.of(this.nodeId, (int) ts));
        }

        String storedValue(String key) {
            return kv.get(key);
        }

        private void ensureLocal(String target) {
            if (!this.nodeId.equals(target)) {
                throw new IllegalArgumentException("FakeSingleNodeClient only supports nodeId=" + nodeId);
            }
        }
    }
}
