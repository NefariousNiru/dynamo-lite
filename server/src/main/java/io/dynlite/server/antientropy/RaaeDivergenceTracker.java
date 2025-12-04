// file: server/src/main/java/io/dynlite/server/antientropy/RaaeDivergenceTracker.java
package io.dynlite.server.antientropy;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks how long a token has been "diverged" from at least one peer.
 *
 * Semantics:
 *  - The first time anti-entropy detects that a token differs between
 *    replicas, you call {@link #recordDivergence(long, long)}.
 *  - We store the FIRST time we saw divergence for that token.
 *  - Once anti-entropy confirms that replicas agree again, call
 *    {@link #clearConverged(long)} to drop the age.
 *
 * This lets RAAE prefer:
 *  - older divergences (stuck for a long time),
 *  - and combine them with hotness to prioritize visible problems.
 */
public final class RaaeDivergenceTracker {

    /**
     * token -> first divergence timestamp (ms since epoch).
     */
    private final ConcurrentHashMap<Long, Long> firstDivergedAtMillis = new ConcurrentHashMap<>();

    /**
     * Record that this token is divergent as of nowMillis.
     * If we already recorded an earlier divergence, we keep the oldest time.
     *
     * @param token     64-bit token for a key.
     * @param nowMillis current wall-clock time (ms since epoch).
     */
    public void recordDivergence(long token, long nowMillis) {
        firstDivergedAtMillis.compute(token, (t, existing) ->
                (existing == null || nowMillis < existing) ? nowMillis : existing
        );
    }

    /**
     * Called when we know that replicas for this token have converged
     * (e.g., after a successful repair round).
     *
     * This resets age to zero.
     */
    public void clearConverged(long token) {
        firstDivergedAtMillis.remove(token);
    }

    /**
     * Return how long (in milliseconds) this token has been divergent,
     * or 0 if we have never recorded divergence for it.
     */
    public long ageMillis(long token, long nowMillis) {
        Long first = firstDivergedAtMillis.get(token);
        if (first == null) return 0L;
        long age = nowMillis - first;
        return Math.max(0L, age);
    }

    /**
     * Lightweight snapshot for metrics / debugging.
     */
    public Map<Long, Long> snapshot() {
        return Map.copyOf(firstDivergedAtMillis);
    }
}
