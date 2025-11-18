// file: src/main/java/io/dynlite/server/cluster/NodeClient.java
package io.dynlite.server.cluster;

import java.util.Map;

/**
 * Internal client for talking to a DynamoLite node (local or remote).
 *
 * This abstracts over the transport:
 *  - LocalNodeClient can directly invoke KvService methods in-process.
 *  - HttpNodeClient can use HTTP/JSON to talk to a remote node.
 *
 * All methods are synchronous for now; later you can make them async.
 */
public interface NodeClient {

    /**
     * Put a Base64-encoded value for a key on a specific node.
     * <br>
     * Semantics:
     *  - The node will treat this as a normal write with its local KvService logic,
     *    including vector clock bumping and durable persistence.
     *  - Implementation may translate this into a /kv/{key} HTTP call.
     *
     * @param nodeId    target nodeId
     * @param key       logical key
     * @param valueB64  Base64-encoded value
     * @param coordId   coordinator node id (for clock bumping); may be null
     * @return a PutResult describing tombstone status, lwwMillis, and clock seen by that node
     */
    PutResult put(String nodeId, String key, String valueB64, String coordId,String opId);

    /** Delete (tombstone) a key on a specific node. */
    PutResult delete(String nodeId, String key, String coordId, String opId);

    /**
     * Read a key from a specific node.
     *
     * @return ReadResult with found flag, Base64 value (if any), and vector clock.
     */
    ReadResult get(String nodeId, String key);

    // ----- DTOs returned by the client -----

    record PutResult(
            boolean tombstone,
            long lwwMillis,
            Map<String, Integer> vectorClock
    ) {}

    record ReadResult(
            boolean found,
            String valueBase64,
            Map<String, Integer> vectorClock
    ) {}
}
