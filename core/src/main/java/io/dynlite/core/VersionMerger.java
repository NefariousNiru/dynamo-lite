package io.dynlite.core;

import java.util.List;

/**
 * Pure merge of competing versions using vector clocks.
 * If a single maximal element exists, return it. Otherwise, return all maximal siblings.
 */
public interface VersionMerger {

    /** Merge by causality; if tied, defer to a resolver strategy to pick a demo winner. */
    MergeResult merge(List<VersionedValue> candidates);

    /** Merge result type: either a single winner or a set of siblings. */
    sealed interface MergeResult permits MergeResult.Winner, MergeResult.Siblings {
        record Winner(VersionedValue value) implements MergeResult {}
        record Siblings(List<VersionedValue> values) implements MergeResult {}
    }
}
