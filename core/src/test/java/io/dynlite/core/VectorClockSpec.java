package io.dynlite.core;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Spec tests: we describe behavior in English, then assert it.
 */
class VectorClockSpec {

    @Test
    void dominance_when_all_counts_ge_and_one_strictly_greater() {
        // A has seen more from B than B has from itself → A dominates B
        var A = new VectorClock(Map.of("A", 1, "B", 2));
        var B = new VectorClock(Map.of("A", 1, "B", 1));

        assertEquals(CausalOrder.LEFT_DOMINATES_RIGHT, A.compare(B));
        assertEquals(CausalOrder.RIGHT_DOMINATES_LEFT, B.compare(A));
    }

    @Test
    void concurrency_when_neither_dominates() {
        // Independent updates: {A:3} vs {A:2,B:1} → neither contains the other
        var aOnly = new VectorClock(Map.of("A", 3));
        var mixed = new VectorClock(Map.of("A", 2, "B", 1));

        assertEquals(CausalOrder.CONCURRENT, aOnly.compare(mixed));
        assertEquals(CausalOrder.CONCURRENT, mixed.compare(aOnly));
    }

    @Test
    void merge_returns_single_winner_or_siblings() {
        var base = new VectorClock(Map.of("A", 2));

        var vA = new VersionedValue("vA".getBytes(), false, new VectorClock(Map.of("A", 3)), /*lww*/ 1000);
        var vB = new VersionedValue("vB".getBytes(), false, new VectorClock(Map.of("A", 2, "B", 1)), /*lww*/ 2000);

        var merger = new DefaultVersionMerger();

        // Concurrent case → siblings
        var res1 = merger.merge(List.of(vA, vB));
        assertTrue(res1 instanceof VersionMerger.MergeResult.Siblings);
        var sibs = ((VersionMerger.MergeResult.Siblings) res1).values();
        assertEquals(2, sibs.size());

        // Dominance case → single winner
        var older = new VersionedValue("old".getBytes(), false, base, 500);
        var newer = new VersionedValue("new".getBytes(), false, new VectorClock(Map.of("A", 3)), 600);
        var res2 = merger.merge(List.of(older, newer));
        assertTrue(res2 instanceof VersionMerger.MergeResult.Winner);
        assertArrayEquals("new".getBytes(), ((VersionMerger.MergeResult.Winner) res2).value().value());

        // Optional: if UI needs one value to display from siblings, use a resolver policy
        var lww = new ConflictResolver.LastWriterWins();
        var chosen = lww.choose(sibs);
        assertArrayEquals("vB".getBytes(), chosen.value()); // vB has larger lwwMillis
    }
}
