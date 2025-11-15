// file: src/main/java/io/dynlite/core/ConflictResolver.java
package io.dynlite.core;

import java.util.Comparator;
import java.util.List;

/**
 * Policy for choosing a single value from a set of concurrent siblings.
 * <p>
 * This is only for "presentation" or external API convenience.
 * Correctness does not depend on which sibling is chosen, as long as
 * all siblings are preserved in storage and participate in future merges.
 */
public interface ConflictResolver {

    /**
     * Choose a single VersionedValue from a non-empty list of siblings.
     */
    VersionedValue choose(List<VersionedValue> siblings);

    /**
     * Default resolver that picks the value with the largest lwwMillis.
     * <p>
     * Important:
     *  - This is not used for causal correctness, only for tie-breaking
     *    when some API needs a single value to show to users.
     */
    final class LastWriterWins implements ConflictResolver {
        @Override public VersionedValue choose(List<VersionedValue> siblings) {
            if (siblings == null || siblings.isEmpty())
                throw new IllegalArgumentException("siblings must not be empty");
            return siblings.stream().max(Comparator.comparingLong(VersionedValue::lwwMillis)).orElseThrow();
        }
    }
}
