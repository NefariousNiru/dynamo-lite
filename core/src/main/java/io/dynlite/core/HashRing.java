package io.dynlite.core;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;

/**
 * Consistent hashing ring with virtual nodes (vnodes).
 *
 * Responsibilities:
 *  - Given a set of physical nodes (identified by nodeId strings)
 *    and a vnode count, build a stable ring of 64-bit tokens.
 *  - For any key (string), return up to N distinct physical owners
 *    by walking clockwise around the ring.
 *
 * Properties:
 *  - Deterministic: same inputs (nodes + vnodes) produce the same ring.
 *  - Balanced: with enough vnodes (e.g. 128 or 256 per node), the key
 *    distribution is approximately uniform.
 *  - Minimal movement on membership change: on joining a new node, only
 *    about 1 / numNodes of keys move to it.
 */
public final class HashRing {

    /**
     * Logical node identity.
     * This must be stable across restarts, and unique per physical node
     * (for example "node-a", "node-b", or a UUID).
     */
    public record NodeSpec(String nodeId) {
        public NodeSpec {
            if (nodeId == null || nodeId.isBlank()) {
                throw new IllegalArgumentException("nodeId");
            }
        }
    }

    // Sorted array of tokens on the ring (unsigned ordering).
    private final long[] tokens;

    // Parallel array: tokens[i] is owned by owners[i] (physical nodeId).
    private final String[] owners;

    // Set of distinct nodeIds present in this ring.
    private final Set<String> nodeSet;

    private HashRing(long[] tokens, String[] owners, Set<String> nodeSet) {
        this.tokens = tokens;
        this.owners = owners;
        this.nodeSet = Set.copyOf(nodeSet);
    }

    /**
     * Build a consistent hash ring with the given nodes and vnode count.
     *
     * @param nodes  list of physical nodes
     * @param vnodes number of virtual nodes per physical node (e.g. 128)
     */
    public static HashRing build(List<NodeSpec> nodes, int vnodes) {
        if (nodes == null || nodes.isEmpty()) throw new IllegalArgumentException("nodes");
        if (vnodes <= 0) throw new IllegalArgumentException("vnodes");
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");

            int total = nodes.size() * vnodes;
            long[] toks = new long[total];
            String[] own = new String[total];

            int idx = 0;
            for (var n : nodes) {
                for (int i = 0; i < vnodes; i++) {
                    long t = hash64(md, n.nodeId() + "#" + i);
                    toks[idx] = t;
                    own[idx] = n.nodeId();
                    idx++;
                }
            }

            // sort by unsigned token
            Integer[] order = new Integer[total];
            for (int i = 0; i < total; i++) {
                order[i] = i;
            }
            Arrays.sort(order, (a, b) -> Long.compareUnsigned(toks[a], toks[b]));

            long[] st = new long[total];
            String[] so = new String[total];
            for (int i = 0; i < total; i++) {
                st[i] = toks[order[i]];
                so[i] = own[order[i]];
            }

            Set<String> set = new HashSet<>(nodes.size());
            for (var n : nodes) {
                set.add(n.nodeId());
            }

            return new HashRing(st, so, set);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Return up to N distinct physical owners for this key, walking
     * clockwise from the key's token on the ring.
     *
     * Example:
     *  - N=3, nodes = [A,B,C], replication factor 3.
     *  - For some key, you might get ["B", "C", "A"].
     * If N > number of distinct nodes, we cap the result to nodeSet.size().
     */
    public List<String> ownersForKey(String key, int N) {
        if (N <= 0) return List.of();
        int want = Math.min(N, nodeSet.size());
        long t = hash64Unchecked(key);
        int start = lowerBoundUnsigned(tokens, t);

        List<String> res = new ArrayList<>(want);
        Set<String> seen = new HashSet<>(want);

        // Walk around the ring at most tokens.length steps, wrapping around.
        for (int i = 0; i < tokens.length && res.size() < want; i++) {
            int idx = (start + i) % tokens.length;
            String owner = owners[idx];

            // Ensure we return distinct physical nodes, not duplicated vnodes.
            if (seen.add(owner)) {
                res.add(owner);
            }
        }
        return res;
    }

    /**
     * Compute the 64-bit token for a given key using the same hash
     * function as the ring.
     *
     * This is used by anti-entropy to map keys to token ranges for shards.
     */
    public static long tokenForKey(String key) {
        return hash64Unchecked(key);
    }

    // ---------- helpers ----------

    private static long hash64(MessageDigest md, String s) {
        md.reset();
        md.update(s.getBytes(StandardCharsets.UTF_8));
        byte[] h = md.digest();
        // take the first 8 bytes as unsigned 64-bit (big-endian for deterministic ordering)
        return ByteBuffer.wrap(h, 0, 8).order(ByteOrder.BIG_ENDIAN).getLong();
    }

    private static long hash64Unchecked(String s) {
        try {
            return hash64(MessageDigest.getInstance("SHA-256"), s);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Return the first index i such that tokens[i] >= t under unsigned comparison.
     * If t is larger than all tokens, we wrap and return 0.
     */
    private static int lowerBoundUnsigned(long[] arr, long t) {
        int lo = 0, hi = arr.length;
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            if (Long.compareUnsigned(arr[mid], t) < 0) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return lo == arr.length ? 0 : lo;
    }
}
