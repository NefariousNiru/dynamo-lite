// file: src/main/java/io/dynlite/storage/TtlOpIdDeduper.java
package io.dynlite.storage;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TTL-bounded OpId deduper.
 *
 * Semantics:
 *  - firstTime(opId):
 *      * returns true  if the opId has not been seen within the TTL window;
 *      * returns false if the opId is still within its TTL window.
 *  - setTtl(Duration): updates the retention window for newly recorded opIds.
 *
 * Implementation notes:
 *  - Backed by a ConcurrentHashMap<opId, expireAtMillis>.
 *  - Lazy cleanup: we only remove expired entries opportunistically on access.
 *    This keeps the implementation simple and avoids background threads.
 *  - TTL is approximate at large scales but sufficient for "dedupe recent retries".
 */
public final class TtlOpIdDeduper implements OpIdDeduper {

    /** Map of opId -> expiration time in epoch millis. */
    private final Map<String, Long> seen = new ConcurrentHashMap<>();

    /** Volatile so TTL updates are visible across threads. */
    private volatile long ttlMillis;

    /**
     * Create a deduper with the given initial TTL.
     */
    public TtlOpIdDeduper(Duration ttl) {
        setTtl(ttl);
    }

    @Override
    public boolean firstTime(String opId) {
        Objects.requireNonNull(opId, "opId");
        long now = System.currentTimeMillis();
        long ttl = this.ttlMillis;

        // Fast path: check existing record (if any).
        Long expireAt = seen.get(opId);
        if (expireAt != null && expireAt >= now) {
            // We have seen this opId within the TTL window.
            return false;
        }

        // Either not present or expired: record it as new.
        long newExpire = now + ttl;
        // Update the map. We don't care about the previous value here because:
        //  - If it was expired, we are "reviving" it with a new TTL.
        //  - If another thread races, dedupe semantics are still correct.
        seen.put(opId, newExpire);

        // Opportunistically clean up a little to avoid unbounded growth.
        // We only scan a small portion on each call to keep overhead low.
        maybeCleanup(now);

        return true;
    }

    @Override
    public void setTtl(Duration ttl) {
        Objects.requireNonNull(ttl, "ttl");
        if (ttl.isNegative() || ttl.isZero()) {
            throw new IllegalArgumentException("TTL must be positive, got: " + ttl);
        }
        this.ttlMillis = ttl.toMillis();
    }

    /**
     * Opportunistic cleanup of expired entries.
     *
     * Strategy:
     *  - Iterate over a subset of entries and remove ones whose expiration
     *    is strictly before 'now'.
     *  - This keeps memory from growing without bound under steady load
     *    without introducing background threads.
     */
    private void maybeCleanup(long now) {
        // Simple heuristic: iterate over a limited number of entries each call.
        // This prevents a single call from doing O(map.size()) work.
        int scanned = 0;
        int scanLimit = 64; // tune as needed

        for (var it = seen.entrySet().iterator(); it.hasNext() && scanned < scanLimit; scanned++) {
            var e = it.next();
            Long expireAt = e.getValue();
            if (expireAt != null && expireAt < now) {
                it.remove();
            }
        }
    }
}
