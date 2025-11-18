// file: server/src/main/java/io/dynlite/server/cluster/GrpcNodeClient.java
package io.dynlite.server.cluster;

import io.dynlite.server.replica.KvReplicaGrpc;
import io.dynlite.server.replica.KvReplicaProto;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * gRPC-based NodeClient implementation.
 *
 * One GrpcNodeClient instance represents a single remote node (host:port).
 * CoordinatorService keeps a Map<String, NodeClient> and passes nodeId into
 * each call, but this implementation only uses that nodeId for error messages.
 */
public final class GrpcNodeClient implements NodeClient {

    private final String target; // "host:port" or in-process name
    private final ManagedChannel channel;
    private final KvReplicaGrpc.KvReplicaBlockingStub stub;

    /**
     * Production constructor using host and port.
     */
    public GrpcNodeClient(String host, int port) {
        this.target = host + ":" + port;
        this.channel = ManagedChannelBuilder
                .forAddress(host, port)
                .usePlaintext() // internal traffic; terminate TLS at edge if needed
                .build();
        this.stub = KvReplicaGrpc.newBlockingStub(channel);
    }

    /**
     * Test-only constructor allowing a pre-built channel (e.g., in-process).
     */
    public GrpcNodeClient(String target, ManagedChannel channel) {
        this.target = target;
        this.channel = channel;
        this.stub = KvReplicaGrpc.newBlockingStub(channel);
    }

    @Override
    public PutResult put(String nodeId, String key, String valueBase64, String coordNodeId) {
        try {
            KvReplicaProto.PutReplicaRequest req =
                    KvReplicaProto.PutReplicaRequest.newBuilder()
                            .setKey(key)
                            .setValueBase64(valueBase64)
                            .setCoordNodeId(coordNodeId == null ? "" : coordNodeId)
                            .build();

            KvReplicaProto.ReplicaWriteResponse resp = stub.putReplica(req);

            return new PutResult(
                    resp.getTombstone(),
                    resp.getLwwMillis(),
                    Map.copyOf(resp.getVectorClockMap())
            );
        } catch (StatusRuntimeException sre) {
            // For now, let CoordinatorService handle exceptions uniformly.
            throw new RuntimeException("gRPC put to node " + nodeId + " (" + target + ") failed", sre);
        }
    }

    @Override
    public PutResult delete(String nodeId, String key, String coordNodeId) {
        try {
            KvReplicaProto.DeleteReplicaRequest req =
                    KvReplicaProto.DeleteReplicaRequest.newBuilder()
                            .setKey(key)
                            .setCoordNodeId(coordNodeId == null ? "" : coordNodeId)
                            .build();

            KvReplicaProto.ReplicaWriteResponse resp = stub.deleteReplica(req);

            return new PutResult(
                    resp.getTombstone(),
                    resp.getLwwMillis(),
                    Map.copyOf(resp.getVectorClockMap())
            );
        } catch (StatusRuntimeException sre) {
            throw new RuntimeException("gRPC delete to node " + nodeId + " (" + target + ") failed", sre);
        }
    }

    @Override
    public ReadResult get(String nodeId, String key) {
        try {
            KvReplicaProto.GetReplicaRequest req =
                    KvReplicaProto.GetReplicaRequest.newBuilder()
                            .setKey(key)
                            .build();

            KvReplicaProto.GetReplicaResponse resp = stub.getReplica(req);

            if (!resp.getFound()) {
                return new ReadResult(false, null, Map.of());
            }

            return new ReadResult(
                    true,
                    resp.getValueBase64(),
                    Map.copyOf(resp.getVectorClockMap())
            );
        } catch (StatusRuntimeException sre) {
            throw new RuntimeException("gRPC get to node " + nodeId + " (" + target + ") failed", sre);
        }
    }

    /**
     * Allow graceful shutdown for tests or node stop.
     */
    public void shutdown() {
        channel.shutdown();
        try {
            channel.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
    }
}
