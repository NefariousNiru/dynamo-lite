// file: src/main/java/io/dynlite/core/DefaultVersionMerger.java
package io.dynlite.core;

import java.util.ArrayList;
import java.util.List;

/**
 * Default vector-clock based merge implementation.
 * <p>
 * Algorithm:
 *  - Given a set of candidate versions V:
 *    - Call a version vi "maximal" if there is no other version vj such that
 *      vj.clock dominates vi.clock.
 *    - Collect all maximal versions.
 * <p>
 *  - If there is exactly 1 maximal version:
 *      return Winner(vi).
 *    Else:
 *      return Siblings(list_of_maximals).
 * <p>
 * Notes:
 *  - This merge does not look at the value bytes themselves, only clocks.
 *  - This is deterministic given the same input list (order does not matter).
 */
public final class DefaultVersionMerger implements VersionMerger {
    @Override
    public MergeResult merge(List<VersionedValue> candidates) {
        if (candidates == null || candidates.isEmpty())
            throw new IllegalArgumentException("Candidates must not be empty");

        // We build the set of maximal elements under the vector-clock partial order.
        var maximals = new ArrayList<VersionedValue>(candidates.size());

        outer:
        for (int i = 0; i < candidates.size(); i++) {
            var vi = candidates.get(i);
            for (int j = 0; j < candidates.size(); j++) {
                if (i == j) continue;
                var vj = candidates.get(j);
                CausalOrder order = vj.clock().compare(vi.clock());

                // If some vj dominates vi, then vi is not maximal, and we can skip it.
                if (order == CausalOrder.LEFT_DOMINATES_RIGHT) continue outer;
            }
            // No other version dominates vi, so vi is maximal.
            maximals.add(vi);
        }
        if (maximals.size() == 1) return new MergeResult.Winner(maximals.getFirst());

        // Multiple maximal versions -> concurrent siblings.
        return new MergeResult.Siblings(List.copyOf(maximals));
    }
}
