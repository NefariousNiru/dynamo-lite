// file: src/main/java/io/dynlite/core/VectorClock.java
package io.dynlite.core;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/**
 * Immutable vector clock: a mapping from nodeId -> counter.
 * <p>
 * This is the fundamental causal metadata used to:
 *  - determine if one version happened-before another, and
 *  - detect concurrent/conflicting updates.
 * <p>
 * Design:
 *  - Immutable: internal map is copied and wrapped as unmodifiable.
 *  - Value object: equals/hashCode based purely on contents.
 *  - Thread safe by construction (no internal mutation).
 */
public final class VectorClock {

    // Internal representation is an unmodifiable copy.
    private final Map<String, Integer> vv;

    /**
     * Create a new vector clock from the provided entries.
     * The input map is defensively copied and wrapped.
     */
    public VectorClock(Map<String, Integer> vv) {
        this.vv = Collections.unmodifiableMap(Map.copyOf(vv));
    }

    /** Empty clock. */
    public static VectorClock empty() { return new VectorClock(Map.of()); }

    /** Current entries (read-only view). */
    public Map<String, Integer> entries() { return vv; }

    /**
     * Return a new VectorClock where {@code nodeId}'s counter is incremented by 1.
     * If the node is not present yet, it is treated as 0 and becomes 1.
     */
    public VectorClock bump(String nodeId) {
        var m = new HashMap<>(vv);
        m.put(nodeId, m.getOrDefault(nodeId, 0) + 1);
        return new VectorClock(m);
    }

    /**
     * Compare this clock (A) to another clock (B) under the standard vector-clock order.
     * <p>
     * Missing entries are treated as 0. Intuition:
     *  - If A >= B elementwise and A != B, then A has "seen" all events B has
     *    plus at least one more, so A dominates B.
     *  - If both A > B somewhere and B > A somewhere, neither dominates, so
     *    the versions are concurrent.
     */
    public CausalOrder compare(VectorClock other) {
        boolean aGreater = false;
        boolean bGreater = false;

        var ids = new HashSet<String>(vv.keySet());
        ids.addAll(other.vv.keySet());

        for (var id: ids) {
            int a = vv.getOrDefault(id, 0);
            int b = other.vv.getOrDefault(id, 0);
            if (a > b) aGreater = true;
            if (a < b) bGreater = true;
            // Early exit: once both are strictly greater on some components, it is Concurrent
            if (aGreater && bGreater) return CausalOrder.CONCURRENT;
        }

        if (!aGreater && !bGreater) return CausalOrder.EQUAL;
        if (aGreater) return CausalOrder.LEFT_DOMINATES_RIGHT;
        return CausalOrder.RIGHT_DOMINATES_LEFT;
    }

    @Override public boolean equals(Object o){
        if (this == o) return true;
        if (!(o instanceof VectorClock vc)) return false;
        return vv.equals(vc.vv);
    }

    @Override public int hashCode(){ return vv.hashCode(); }

    @Override public String toString(){ return vv.toString(); }
}
