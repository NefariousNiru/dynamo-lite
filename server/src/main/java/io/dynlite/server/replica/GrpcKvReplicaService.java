// file: server/src/main/java/io/dynlite/server/replica/GrpcKvReplicaService.java
package io.dynlite.server.replica;

import io.dynlite.server.KvService;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;

import java.util.Map;

/**
 * gRPC replica service that exposes a node's KvService to other nodes.
 *
 * This is the node-to-node replication API (internal), not the external HTTP API.
 *
 * Responsibilities:
 *  - Decode gRPC requests into KvService calls.
 *  - Map KvService.Result / KvService.Read to ReplicaWriteResponse / GetReplicaResponse.
 *  - Map IllegalArgumentException to INVALID_ARGUMENT, everything else to INTERNAL.
 */
public final class GrpcKvReplicaService extends KvReplicaGrpc.KvReplicaImplBase {

    private final KvService kv;

    public GrpcKvReplicaService(KvService kv) {
        this.kv = kv;
    }

    @Override
    public void putReplica(
            KvReplicaProto.PutReplicaRequest request,
            StreamObserver<KvReplicaProto.ReplicaWriteResponse> responseObserver
    ) {
        try {
            String key = request.getKey();
            String valueBase64 = request.getValueBase64();
            String coordNodeId = request.getCoordNodeId();

            KvService.Result r = kv.put(key, valueBase64, coordNodeId);

            KvReplicaProto.ReplicaWriteResponse resp =
                    KvReplicaProto.ReplicaWriteResponse.newBuilder()
                            .setTombstone(false)
                            .setLwwMillis(r.lwwMillis())
                            .putAllVectorClock(toIntMap(r.clock()))
                            .build();

            responseObserver.onNext(resp);
            responseObserver.onCompleted();
        } catch (IllegalArgumentException iae) {
            responseObserver.onError(
                    Status.INVALID_ARGUMENT
                            .withDescription(iae.getMessage())
                            .asException()
            );
        } catch (Exception e) {
            responseObserver.onError(
                    Status.INTERNAL
                            .withDescription(e.getMessage())
                            .asException()
            );
        }
    }

    @Override
    public void deleteReplica(
            KvReplicaProto.DeleteReplicaRequest request,
            StreamObserver<KvReplicaProto.ReplicaWriteResponse> responseObserver
    ) {
        try {
            String key = request.getKey();
            String coordNodeId = request.getCoordNodeId();

            KvService.Result r = kv.delete(key, coordNodeId);

            KvReplicaProto.ReplicaWriteResponse resp =
                    KvReplicaProto.ReplicaWriteResponse.newBuilder()
                            .setTombstone(true)
                            .setLwwMillis(r.lwwMillis())
                            .putAllVectorClock(toIntMap(r.clock()))
                            .build();

            responseObserver.onNext(resp);
            responseObserver.onCompleted();
        } catch (IllegalArgumentException iae) {
            responseObserver.onError(
                    Status.INVALID_ARGUMENT
                            .withDescription(iae.getMessage())
                            .asException()
            );
        } catch (Exception e) {
            responseObserver.onError(
                    Status.INTERNAL
                            .withDescription(e.getMessage())
                            .asException()
            );
        }
    }

    @Override
    public void getReplica(
            KvReplicaProto.GetReplicaRequest request,
            StreamObserver<KvReplicaProto.GetReplicaResponse> responseObserver
    ) {
        try {
            String key = request.getKey();
            KvService.Read r = kv.get(key);

            if (!r.found()) {
                KvReplicaProto.GetReplicaResponse resp =
                        KvReplicaProto.GetReplicaResponse.newBuilder()
                                .setFound(false)
                                .build();
                responseObserver.onNext(resp);
                responseObserver.onCompleted();
                return;
            }

            KvReplicaProto.GetReplicaResponse resp =
                    KvReplicaProto.GetReplicaResponse.newBuilder()
                            .setFound(true)
                            .setValueBase64(r.base64())
                            .putAllVectorClock(toIntMap(r.clock()))
                            .build();

            responseObserver.onNext(resp);
            responseObserver.onCompleted();
        } catch (IllegalArgumentException iae) {
            responseObserver.onError(
                    Status.INVALID_ARGUMENT
                            .withDescription(iae.getMessage())
                            .asException()
            );
        } catch (Exception e) {
            responseObserver.onError(
                    Status.INTERNAL
                            .withDescription(e.getMessage())
                            .asException()
            );
        }
    }

    private static Map<String, Integer> toIntMap(Map<String, Integer> in) {
        // KvService already uses Map<String,Integer>, so this is just identity.
        return in;
    }
}
