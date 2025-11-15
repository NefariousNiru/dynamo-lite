// file: src/main/java/io/dynlite/storage/SnapshotPolicy.java
package io.dynlite.storage;

import io.dynlite.core.VersionedValue;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Snapshot policy that triggers a full snapshot after every N writes.
 * <p>
 * Simple but effective:
 *  - Bounds worst-case recovery time by limiting WAL replay length.
 *  - Does not consider file size or time - those can be added later.
 */

public final class SnapshotPolicy {
    private final int everyOps;
    private final AtomicInteger sinceLast = new AtomicInteger();

    public SnapshotPolicy(int everyOps) {
        if (everyOps <= 0) throw new IllegalArgumentException("everyOps must be > 0");
        this.everyOps = everyOps;
    }

    /** Call after each successful durable write. Snapshots when threshold is hit. */
    public void maybeSnapshot(Map<String, VersionedValue> mem, Snapshotter snaps) {
        if (sinceLast.incrementAndGet() >= everyOps) {
            snaps.writeSnapshot(mem);
            sinceLast.set(0);
        }
    }
}
