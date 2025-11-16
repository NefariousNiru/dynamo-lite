// file: src/main/java/io/dynlite/storage/Snapshotter.java
package io.dynlite.storage;

import io.dynlite.core.VersionedValue;

import java.util.List;
import java.util.Map;

/**
 * Snapshot abstraction to bound recovery time.
 * <p>
 * A snapshot is a full copy of the current in-memory map at some point in time.
 * On restart:
 *  - we load the latest snapshot, then
 *  - replay WAL records written after that snapshot.
 */
public interface Snapshotter {

    /**
     * Persist a full copy of the current map.
     *
     * @param current immutable snapshot of key -> Sibling list of VersionedValue
     * @return snapshot identifier (e.g., filename/path).
     */
    String writeSnapshot(Map<String, List<VersionedValue>> current);

    /** Load the latest snapshot if present. Returns a map to seed memory. */
    LoadedSnapshot loadLatest();

    /** Simple holder for snapshot id and its data */
    record LoadedSnapshot(String id, Map<String, List<VersionedValue>> data) {}
}
