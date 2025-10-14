package io.dynlite.core;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;

/**
 * Consistent-hash ring with virtual nodes (vnodes). Deterministic: same inputs -> same owners.
 * Tokens are unsigned 64-bit values derived from SHA-256(nodeId#vnode) or SHA-256(key).
 */
public final class HashRing {

    /** Logical node identity. Must be stable across restarts. */
    public record NodeSpec(String nodeId) {
        public NodeSpec {
            if (nodeId == null || nodeId.isBlank()) throw new IllegalArgumentException("nodeId");
        }
    }

    private final long[] tokens;        // sorted ascending (unsigned compare)
    private final String[] owners;      // parallel array: physical nodeId per token
    private final Set<String> nodeSet;  // distinct physical nodes in this ring

    private HashRing(long[] tokens, String[] owners, Set<String> nodeSet) {
        this.tokens = tokens; this.owners = owners; this.nodeSet = Set.copyOf(nodeSet);
    }

    /** Build a ring with 'vnodes' positions per physical node. */
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
                    toks[idx] = t; own[idx] = n.nodeId(); idx++;
                }
            }
            // sort by unsigned token
            Integer[] order = new Integer[total];
            for (int i=0;i<total;i++) order[i]=i;
            Arrays.sort(order, (a,b) -> Long.compareUnsigned(toks[a], toks[b]));
            long[] st = new long[total]; String[] so = new String[total];
            for (int i=0;i<total;i++) { st[i] = toks[order[i]]; so[i] = own[order[i]]; }
            Set<String> set = new HashSet<>(nodes.size());
            for (var n : nodes) set.add(n.nodeId());
            return new HashRing(st, so, set);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /** Return up to N distinct physical owners for this key, clockwise from key's token. */
    public List<String> ownersForKey(String key, int N) {
        if (N <= 0) return List.of();
        int want = Math.min(N, nodeSet.size());
        long t = hash64Unchecked(key);
        int start = lowerBoundUnsigned(tokens, t);
        List<String> res = new ArrayList<>(want);
        Set<String> seen = new HashSet<>(want);
        for (int i = 0; i < tokens.length && res.size() < want; i++) {
            int idx = (start + i) % tokens.length;
            String owner = owners[idx];
            if (seen.add(owner)) res.add(owner);
        }
        return res;
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
        try { return hash64(MessageDigest.getInstance("SHA-256"), s); }
        catch (Exception e) { throw new RuntimeException(e); }
    }

    /** First index i where tokens[i] >= t under unsigned comparison; wrap to 0 if t > last. */
    private static int lowerBoundUnsigned(long[] arr, long t) {
        int lo = 0, hi = arr.length;
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            if (Long.compareUnsigned(arr[mid], t) < 0) lo = mid + 1; else hi = mid;
        }
        return lo == arr.length ? 0 : lo;
    }
}
