// file: src/main/java/io/dynlite/storage/KeyValueStore.java
package io.dynlite.storage;

import io.dynlite.core.VersionedValue;

/**
 * Minimal synchronous KV interface used by the server layer.
 * <p>
 * Semantics:
 *  - put() must be durable before returning (WAL+fsync).
 *  - get() returns the latest applied version for the key, or null if absent.
 * <p>
 * Note:
 *  - For true Dynamo semantics, this will eventually evolve to support
 *    multiple siblings per key. For now it exposes only a single VersionedValue.
 */
public interface KeyValueStore {

    /**
     * Put or delete a key (tombstone=true) in a durable way.
     * Implementations:
     *  - append record to WAL and fsync,
     *  - apply to in-memory state,
     *  - possibly trigger snapshot and WAL rotation.
     */
    void put(String key, VersionedValue value, String opId);

    /**
     * Get current value or null if absent or tombstoned.
     */
    VersionedValue get(String key);
}
