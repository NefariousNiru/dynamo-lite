// file: src/main/java/io/dynlite/server/shard/TokenRange.java
package io.dynlite.server.shard;

import java.util.Objects;

/**
 * Half-open interval [start, end) over the unsigned 64-bit token space.
 * The range may or may not wrap around:
 *  - Non-wrapping: start <= end, covers [start, end).
 *  - Wrapping:     start > end, covers [start, 2^64) U [0, end).
 * Uses unsigned comparison semantics (Long.compareUnsigned).
 */
public final class TokenRange {

    private final long startInclusive;
    private final long endExclusive;

    public TokenRange(long startInclusive, long endExclusive) {
        this.startInclusive = startInclusive;
        this.endExclusive = endExclusive;
    }

    public long startInclusive() {
        return startInclusive;
    }

    public long endExclusive() {
        return endExclusive;
    }

    /**
     * Return true if 'token' belongs to this range under unsigned comparison.
     */
    public boolean contains(long token) {
        int cmp = Long.compareUnsigned(startInclusive, endExclusive);
        if (cmp == 0) {
            // Full ring: [x, x) covers all tokens.
            return true;
        } else if (cmp < 0) {
            // Non-wrapping: start < end
            return Long.compareUnsigned(token, startInclusive) >= 0
                    && Long.compareUnsigned(token, endExclusive) < 0;
        } else {
            // Wrapping: [start, 2^64) U [0, end)
            return Long.compareUnsigned(token, startInclusive) >= 0
                    || Long.compareUnsigned(token, endExclusive) < 0;
        }
    }

    @Override
    public String toString() {
        return "TokenRange[" + toHex(startInclusive) + "," + toHex(endExclusive) + ")";
    }

    private static String toHex(long v) {
        return "0x" + Long.toUnsignedString(v, 16);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TokenRange other)) return false;
        return startInclusive == other.startInclusive
                && endExclusive == other.endExclusive;
    }

    @Override
    public int hashCode() {
        return Objects.hash(startInclusive, endExclusive);
    }
}
