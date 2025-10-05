package io.dynlite.core;

import java.util.Comparator;
import java.util.List;

/** Pluggable policy to pick a single value from concurrent siblings for the UI/return path. */
public interface ConflictResolver {
    VersionedValue choose(List<VersionedValue> siblings);

    /** Default: Last-Writer-Wins by lwwMillis. */
    final class LastWriterWins implements ConflictResolver {
        @Override public VersionedValue choose(List<VersionedValue> siblings) {
            if (siblings == null || siblings.isEmpty())
                throw new IllegalArgumentException("siblings must not be empty");
            return siblings.stream().max(Comparator.comparingLong(VersionedValue::lwwMillis)).orElseThrow();
        }
    }
}
