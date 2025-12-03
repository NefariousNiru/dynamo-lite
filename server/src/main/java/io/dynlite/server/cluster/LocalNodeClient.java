// file: src/main/java/io/dynlite/server/cluster/LocalNodeClient.java
package io.dynlite.server.cluster;

import io.dynlite.server.KvService;

import java.util.Objects;

/**
 * NodeClient implementation that talks to the local KvService directly
 * (no network).
 * <br>
 * Used when:
 *  - the coordinator is sending a request to itself,
 *  - or in tests where you don't want to spin up HTTP servers.
 */
public final class LocalNodeClient implements NodeClient {

    private final String localNodeId;
    private final KvService kv;

    public LocalNodeClient(String localNodeId, KvService kv) {
        this.localNodeId = Objects.requireNonNull(localNodeId, "localNodeId");
        this.kv = Objects.requireNonNull(kv, "kv");
    }

    @Override
    public PutResult put(String nodeId, String key, String valueB64, String coordId, String opId) {
        ensureLocal(nodeId);
        KvService.Result r = kv.put(key, valueB64, coordId, opId);
        return new PutResult(r.tombstone(), r.lwwMillis(), r.clock());
    }

    @Override
    public PutResult delete(String nodeId, String key, String coordId, String opId) {
        ensureLocal(nodeId);
        KvService.Result r = kv.delete(key, coordId, opId);
        return new PutResult(r.tombstone(), r.lwwMillis(), r.clock());
    }

    @Override
    public ReadResult get(String nodeId, String key) {
        ensureLocal(nodeId);
        KvService.Read r = kv.get(key);
        return new ReadResult(r.found(), r.base64(), r.clock());
    }

    private void ensureLocal(String targetNodeId) {
        if (!localNodeId.equals(targetNodeId)) {
            throw new IllegalArgumentException(
                    "LocalNodeClient can only be used for nodeId=%s (got %s)"
                            .formatted(localNodeId, targetNodeId)
            );
        }
    }
}