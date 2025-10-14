package io.dynlite.core;

import org.junit.jupiter.api.Test;

import java.util.*;


import static org.junit.jupiter.api.Assertions.*;

class HashRingSpec {

    private static List<HashRing.NodeSpec> nodes(String... ids) {
        return Arrays.stream(ids).map(HashRing.NodeSpec::new).toList();
    }

    @Test
    void determinism_same_inputs_same_owners() {
        var ring1 = HashRing.build(nodes("A","B","C"), 128);
        var ring2 = HashRing.build(nodes("A","B","C"), 128);

        var k = "user:42";
        assertEquals(ring1.ownersForKey(k, 3), ring2.ownersForKey(k, 3));
    }

    @Test
    void balance_first_owner_share_is_roughly_uniform() {
        var ring = HashRing.build(nodes("A","B","C"), 128);
        var counts = new HashMap<String,Integer>();
        counts.put("A",0); counts.put("B",0); counts.put("C",0);

        int N = 100_000;
        for (int i=0;i<N;i++) {
            String key = "k-" + i;
            String first = ring.ownersForKey(key, 1).get(0);
            counts.put(first, counts.get(first)+1);
        }
        // ~33% each, allow ±5%
        double pA = counts.get("A")/(double)N, pB = counts.get("B")/(double)N, pC = counts.get("C")/(double)N;
        assertTrue(Math.abs(pA - 1.0/3) < 0.05, "A share=" + pA);
        assertTrue(Math.abs(pB - 1.0/3) < 0.05, "B share=" + pB);
        assertTrue(Math.abs(pC - 1.0/3) < 0.05, "C share=" + pC);
    }

    @Test
    void movement_on_join_is_about_quarter() {
        var rOld = HashRing.build(nodes("A","B","C"), 128);
        var rNew = HashRing.build(nodes("A","B","C","D"), 128);

        int N = 100_000, moved = 0;
        for (int i=0;i<N;i++) {
            String key = "k-" + i;
            String oldOwner = rOld.ownersForKey(key, 1).get(0);
            String newOwner = rNew.ownersForKey(key, 1).get(0);
            if (!oldOwner.equals(newOwner)) moved++;
        }
        double frac = moved / (double) N;
        // ~25% ±5%
        assertTrue(frac > 0.20 && frac < 0.30, "moved=" + frac);
    }

    @Test
    void owners_are_distinct_physical_nodes() {
        var ring = HashRing.build(nodes("A","B","C"), 256);
        var owners = ring.ownersForKey("hot-key", 3);
        assertEquals(3, new HashSet<>(owners).size());
    }

    @Test
    void edge_cases() {
        var ring = HashRing.build(nodes("A","B"), 64);
        // Request more than available: cap at unique count
        assertEquals(2, ring.ownersForKey("x", 3).size());
        // Empty nodes -> IAE
        assertThrows(IllegalArgumentException.class, () -> HashRing.build(List.of(), 64));
    }
}
