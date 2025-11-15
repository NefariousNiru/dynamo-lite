// file: src/main/java/io/dynlite/storage/OpIdDeduper.java
package io.dynlite.storage;

import java.time.Duration;

/**
 * Deduplicates operations by opId.
 * Rationale:
 *  - Clients may retry after timeouts, which can cause the same logical write
 *    to be appended more than once to the WAL or received from replication.
 *  - We only want to apply each opId's mutation to memory once.
 * <p>
 * Implementation is deliberately abstract. A production impl could use:
 *  - Caffeine cache with TTL,
 *  - ring buffer,
 *  - or Redis if centralized.
 */
public interface OpIdDeduper {
    /** Returns true if this opId was not seen before and is now recorded. */
    boolean firstTime(String opId);

    /** Configure retention window for remembering ids. */
    void setTtl(Duration ttl);
}
