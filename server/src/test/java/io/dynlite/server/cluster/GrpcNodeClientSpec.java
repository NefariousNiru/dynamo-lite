// file: server/src/test/java/io/dynlite/server/cluster/GrpcNodeClientSpec.java
package io.dynlite.server.cluster;

import io.dynlite.server.KvService;
import io.dynlite.server.replica.GrpcKvReplicaService;
import io.dynlite.server.replica.KvReplicaGrpc;
import io.dynlite.server.replica.KvReplicaProto;
import io.dynlite.storage.KeyValueStore;
import io.dynlite.core.VersionedValue;
import io.dynlite.core.VectorClock;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * End-to-end spec for GrpcNodeClient:
 *  - In-process gRPC server exposing GrpcKvReplicaService + KvService.
 *  - GrpcNodeClient talks to that server and gets correct NodeClient.PutResult / ReadResult.
 */
class GrpcNodeClientSpec {

    @Test
    void put_get_delete_roundtrip_over_grpc() throws IOException {
        // Minimal in-memory KeyValueStore for KvService.
        KvService kv = getKvService();

        String name = InProcessServerBuilder.generateName();
        var server = InProcessServerBuilder
                .forName(name)
                .directExecutor()
                .addService(new GrpcKvReplicaService(kv))
                .build()
                .start();

        var channel = InProcessChannelBuilder
                .forName(name)
                .directExecutor()
                .build();

        GrpcNodeClient client = new GrpcNodeClient("inproc:" + name, channel);

        // --- PUT ---
        NodeClient.PutResult putRes =
                client.put("node-a", "user:42", "dmFsdWU=", "coord-1", null);

        assertFalse(putRes.tombstone());
        assertTrue(putRes.lwwMillis() > 0);
        assertFalse(putRes.vectorClock().isEmpty());

        // --- GET ---
        NodeClient.ReadResult readRes =
                client.get("node-a", "user:42");

        assertTrue(readRes.found());
        assertEquals("dmFsdWU=", readRes.valueBase64());
        assertEquals(putRes.vectorClock(), readRes.vectorClock());

        // --- DELETE ---
        NodeClient.PutResult delRes =
                client.delete("node-a", "user:42", "coord-1", null);

        assertTrue(delRes.tombstone());

        NodeClient.ReadResult afterDel =
                client.get("node-a", "user:42");

        assertFalse(afterDel.found());

        channel.shutdownNow();
        server.shutdownNow();
    }

    private static KvService getKvService() {
        KeyValueStore store = new KeyValueStore() {
            private VersionedValue current;

            @Override
            public void put(String key, VersionedValue value, String opId) {
                current = value;
            }

            @Override
            public VersionedValue get(String key) {
                return current;
            }

            @Override
            public List<VersionedValue> getSiblings(String key) {
                return current == null ? List.of() : List.of(current);
            }
        };

        KvService kv = new KvService(store, "node-a");
        return kv;
    }
}
