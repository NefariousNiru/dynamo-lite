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