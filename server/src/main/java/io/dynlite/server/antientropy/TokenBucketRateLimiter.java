// file: server/src/main/java/io/dynlite/server/antientropy/TokenBucketRateLimiter.java
package io.dynlite.server.antientropy;

import io.dynlite.server.shard.ShardDescriptor;

import java.util.Objects;

/**
 * Global token-bucket limiter for anti-entropy repairs.
 *
 * Semantics:
 *   - capacityTokens: maximum burst size (upper bound for bucket contents).
 *   - refillTokensPerSecond: how quickly the bucket refills over time.
 *   - Each call to tryAcquireTokens(...) will:
 *       * lazily refill the bucket based on elapsed time,
 *       * allow up to min(requested, floor(availableTokens)) tokens,
 *       * subtract the returned amount from the bucket.
 *
 * This is a node-local limiter. It does NOT block; it simply returns 0 when
 * the bucket is empty. Callers should treat 0 as "skip this round".
 */
public final class TokenBucketRateLimiter implements AntiEntropyRateLimiter {

    private final long capacityTokens;
    private final double refillTokensPerSecond;

    // guarded by this
    private double availableTokens;
    private long lastRefillNanos;

    public TokenBucketRateLimiter(long capacityTokens, double refillTokensPerSecond) {
        if (capacityTokens <= 0) {
            throw new IllegalArgumentException("capacityTokens must be > 0");
        }
        if (refillTokensPerSecond <= 0.0) {
            throw new IllegalArgumentException("refillTokensPerSecond must be > 0");
        }
        this.capacityTokens = capacityTokens;
        this.refillTokensPerSecond = refillTokensPerSecond;
        this.availableTokens = capacityTokens;
        this.lastRefillNanos = System.nanoTime();
    }

    @Override
    public synchronized int tryAcquireTokens(ShardDescriptor shard, int requestedTokens) {
        Objects.requireNonNull(shard, "shard");
        if (requestedTokens <= 0) {
            return 0;
        }

        refill();

        int allowable = (int) Math.floor(availableTokens);
        if (allowable <= 0) {
            return 0;
        }

        int grant = Math.min(requestedTokens, allowable);
        availableTokens -= grant;
        return grant;
    }

    private void refill() {
        long now = System.nanoTime();
        long deltaNanos = now - lastRefillNanos;
        if (deltaNanos <= 0L) {
            return;
        }

        double deltaSeconds = deltaNanos / 1_000_000_000.0;
        double added = deltaSeconds * refillTokensPerSecond;
        if (added <= 0.0) {
            return;
        }

        availableTokens = Math.min(capacityTokens, availableTokens + added);
        lastRefillNanos = now;
    }
}
