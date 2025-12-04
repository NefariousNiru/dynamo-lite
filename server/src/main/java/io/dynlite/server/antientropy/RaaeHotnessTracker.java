// file: server/src/main/java/io/dynlite/server/antientropy/RaaeHotnessTracker.java
package io.dynlite.server.antientropy;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks how "hot" a key/token is using an EWMA of access frequency.
 *
 * This is intentionally lightweight and lock-free:
 *  - Keys are identified by their token (64-bit hash on the ring).
 *  - Each access (read/write/delete) calls {@link #recordAccess(long, long)}.
 *  - Hotness decays implicitly via EWMA rather than explicit background jobs.
 *
 * Usage:
 *  - On every foreground read/write:
 *        tracker.recordAccess(token, System.currentTimeMillis());
 *  - During anti-entropy scheduling:
 *        double h = tracker.getHotness(token);
 *        // combine with divergence age to get a repair priority score.
 */
public final class RaaeHotnessTracker {

    /**
     * Immutable snapshot of hotness for a single token.
     *
     * @param ewma              exponentially weighted moving average of access count.
     * @param lastAccessMillis  wall-clock time (ms since epoch) when we last saw this token.
     */
    public record Hotness(double ewma, long lastAccessMillis) {}

    private final ConcurrentHashMap<Long, Hotness> hotnessMap = new ConcurrentHashMap<>();
    private final double alpha;

    /**
     * Create a new tracker.
     *
     * @param alpha smoothing factor in (0, 1]:
     *              - closer to 1 -> reacts quickly to recent spikes, more volatile.
     *              - closer to 0 -> smoother, remembers older activity longer.
     */
    public RaaeHotnessTracker(double alpha) {
        if (!(alpha > 0.0 && alpha <= 1.0)) {
            throw new IllegalArgumentException("alpha must be in (0,1], got " + alpha);
        }
        this.alpha = alpha;
    }

    /**
     * Record a single access for a token at the given time.
     *
     * Each call increments the EWMA toward +1 and updates lastAccessMillis.
     * There is no explicit decay timer – EWMA decays as a function of how
     * often you keep calling recordAccess.
     *
     * @param token     64-bit hash for a key on the ring.
     * @param nowMillis current time in ms (usually System.currentTimeMillis()).
     */
    public void recordAccess(long token, long nowMillis) {
        hotnessMap.compute(token, (t, existing) -> {
            if (existing == null) {
                // First time we see this token; treat as a "burst" of 1.
                return new Hotness(1.0, nowMillis);
            }
            double newEwma = (1.0 - alpha) * existing.ewma() + alpha * 1.0;
            return new Hotness(newEwma, nowMillis);
        });
    }

    /**
     * Current hotness EWMA for this token (0.0 if never seen).
     */
    public double getHotness(long token) {
        Hotness h = hotnessMap.get(token);
        return (h == null) ? 0.0 : h.ewma();
    }

    /**
     * Last access time for this token, or -1 if we have never seen it.
     */
    public long getLastAccessMillis(long token) {
        Hotness h = hotnessMap.get(token);
        return (h == null) ? -1L : h.lastAccessMillis();
    }

    /**
     * Lightweight view for debugging / metrics.
     * Callers should treat the returned map as read-only.
     */
    public Map<Long, Hotness> snapshot() {
        return Map.copyOf(hotnessMap);
    }
}
