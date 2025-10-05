package io.dynlite.core;

import java.util.ArrayList;
import java.util.List;

public final class DefaultVersionMerger implements VersionMerger {
    @Override
    public MergeResult merge(List<VersionedValue> candidates) {
        if (candidates == null || candidates.isEmpty())
            throw new IllegalArgumentException("Candidates must not be empty");

        var maximals = new ArrayList<VersionedValue>(candidates.size());

        outer:
        for (int i = 0; i < candidates.size(); i++) {
            var vi = candidates.get(i);
            for (int j = 0; j < candidates.size(); j++) {
                if (i == j) continue;
                var vj = candidates.get(j);
                var order = vj.clock().compare(vi.clock());
                if (order == CausalOrder.LEFT_DOMINATES_RIGHT) continue outer;
            }
            maximals.add(vi);
        }
        if (maximals.size() == 1) return new MergeResult.Winner(maximals.getFirst());
        return new MergeResult.Siblings(List.copyOf(maximals));
    }
}
