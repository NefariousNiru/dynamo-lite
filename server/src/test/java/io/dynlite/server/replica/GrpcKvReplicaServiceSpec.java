// file: server/src/test/java/io/dynlite/server/replica/GrpcKvReplicaServiceSpec.java
package io.dynlite.server.replica;

import io.dynlite.core.VersionedValue;
import io.dynlite.server.KvService;
import io.dynlite.storage.KeyValueStore;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Specs for GrpcKvReplicaService:
 *  - Maps PutReplica/DeleteReplica/GetReplica to KvService calls.
 *  - Maps IllegalArgumentException to INVALID_ARGUMENT.
 */
class GrpcKvReplicaServiceSpec {

    @Test
    void put_replica_calls_kv_service_and_returns_clock() throws IOException {
        // Stub KvService recording the last put.
        AtomicReference<KvService.Result> lastResult = new AtomicReference<>();

        // Dummy KeyValueStore; KvService methods are overridden so this won't be used.
        KvService kv = getKvService(lastResult);

        String name = InProcessServerBuilder.generateName();
        var server = InProcessServerBuilder
                .forName(name)
                .directExecutor()
                .addService(new GrpcKvReplicaService(kv))
                .build()
                .start();

        var channel = InProcessChannelBuilder.forName(name)
                .directExecutor()
                .build();

        KvReplicaGrpc.KvReplicaBlockingStub stub =
                KvReplicaGrpc.newBlockingStub(channel);

        KvReplicaProto.PutReplicaRequest req =
                KvReplicaProto.PutReplicaRequest.newBuilder()
                        .setKey("user:1")
                        .setValueBase64("dmFsdWU=")
                        .setCoordNodeId("node-a")
                        .build();

        KvReplicaProto.ReplicaWriteResponse resp = stub.putReplica(req);

        assertFalse(resp.getTombstone());
        assertEquals(1234L, resp.getLwwMillis());
        assertEquals(2, resp.getVectorClockMap().get("A").intValue());
        assertNotNull(lastResult.get());

        channel.shutdownNow();
        server.shutdownNow();
    }

    private static KvService getKvService(AtomicReference<KvService.Result> lastResult) {
        KeyValueStore dummyStore = new KeyValueStore() {
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

        KvService kv = new KvService(dummyStore, "node-test") {
            @Override
            public Result put(String key, String base64, String coordNodeId) {
                Result r = new Result(false, 1234L, Map.of("A", 2));
                lastResult.set(r);
                return r;
            }
        };
        return kv;
    }

    @Test
    void illegal_argument_maps_to_invalid_argument_status() throws IOException {
        KvService kv = new KvService(null, "node-test") {
            @Override
            public Result put(String key, String base64, String coordNodeId) {
                throw new IllegalArgumentException("bad key");
            }
        };

        String name = InProcessServerBuilder.generateName();
        var server = InProcessServerBuilder
                .forName(name)
                .directExecutor()
                .addService(new GrpcKvReplicaService(kv))
                .build()
                .start();

        var channel = InProcessChannelBuilder.forName(name)
                .directExecutor()
                .build();

        KvReplicaGrpc.KvReplicaBlockingStub stub =
                KvReplicaGrpc.newBlockingStub(channel);

        KvReplicaProto.PutReplicaRequest req =
                KvReplicaProto.PutReplicaRequest.newBuilder()
                        .setKey("")
                        .setValueBase64("")
                        .build();

        var ex = assertThrows(
                io.grpc.StatusRuntimeException.class,
                () -> stub.putReplica(req)
        );
        assertEquals(Status.INVALID_ARGUMENT.getCode(), ex.getStatus().getCode());
        assertTrue(ex.getStatus().getDescription().contains("bad key"));

        channel.shutdownNow();
        server.shutdownNow();
    }
}
