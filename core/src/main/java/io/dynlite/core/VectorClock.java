package io.dynlite.core;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/**
 * Immutable per-key causal metadata: nodeId -> counter.
 * Value Object: thread-safe, no hidden state, equals/hashCode by content.
 */
public final class VectorClock {
    private final Map<String, Integer> vv; // unmodifiable, defensive copy in ctor

    public VectorClock(Map<String, Integer> vv) {
        // defensive copy and wrap to enforce immutability
        this.vv = Collections.unmodifiableMap(Map.copyOf(vv));
    }

    /** Empty clock. */
    public static VectorClock empty() { return new VectorClock(Map.of()); }

    /** Current entries (read-only view). */
    public Map<String, Integer> entries() { return vv; }

    /**
     * Return a new clock with nodeId's counter incremented by 1.
     * If nodeId absent, it becomes 1.
     */
    public VectorClock bump(String nodeId) {
        var m = new HashMap<>(vv);
        m.put(nodeId, m.getOrDefault(nodeId, 0) + 1);
        return new VectorClock(m);
    }

    /**
     * Compare this clock (A) to another (B) under the vector-clock partial order.
     * Missing entries are treated as 0.
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
