// file: src/main/java/io/dynlite/core/VersionMerger.java
package io.dynlite.core;

import java.util.List;

/**
 * Pure function interface that merges competing versions of the same key
 * using vector clocks.
 * <p>
 * Typical behavior:
 *  - If there is a single maximal element under the vector-clock order,
 *    that version is the unique winner.
 *  - If there are multiple maximal elements, they are concurrent siblings
 *    and all must be preserved.
 * <p>
 * This interface is deliberately ignorant of:
 *  - storage layout (single vs multi-version per key),
 *  - network/protocol details, and
 *  - UI specific policies (such as "pick one sibling to display").
 */
public interface VersionMerger {

    /**
     * Merge a set of candidate versions for the same key.
     *
     * @param candidates non-empty list of versions that should all belong to the same key
     * @return a MergeResult indicating either:
     *         - a single Winner, or
     *         - a Siblings set of concurrent versions.
     */
    MergeResult merge(List<VersionedValue> candidates);

    /**
     * Result of a merge:
     *  - Winner: there is a unique maximal version under the vector-clock order.
     *  - Siblings: multiple concurrent maximal versions exist and must be retained.
     */
    sealed interface MergeResult permits MergeResult.Winner, MergeResult.Siblings {
        record Winner(VersionedValue value) implements MergeResult {}
        record Siblings(List<VersionedValue> values) implements MergeResult {}
    }
}
