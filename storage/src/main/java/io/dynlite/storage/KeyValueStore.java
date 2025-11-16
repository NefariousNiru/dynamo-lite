// file: src/main/java/io/dynlite/storage/KeyValueStore.java
package io.dynlite.storage;

import io.dynlite.core.VersionedValue;

import java.util.List;

/**
 * Minimal synchronous KV interface used by the server layer.
 * <p>
 * Semantics:
 *  - put() must be durable before returning (WAL+fsync).
 *  - get() returns the latest applied version for the key, or null if absent.
 *  - getSiblings(): return the full set of concurrent maximal versions (siblings).
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
     * Get the current value for a key, using an internal conflict resolution policy.
     * - If no versions exist, or all known versions are tombstones, returns null.
     * - If one or more live (non-tombstone) versions exist, a single winner is chosen
     *   using the store's ConflictResolver (currently Last-Writer-Wins by lwwMillis).
     */
    VersionedValue get(String key);

    /**
     * Return the current sibling set for a key: all maximal versions under the
     * vector-clock partial order.
     * - The list may be empty if the key has never been written.
     * - The list may contain only tombstones (logical delete, but kept for replication).
     * - The list is immutable; callers must not mutate its elements.
     */
    List<VersionedValue> getSiblings(String key);
}
