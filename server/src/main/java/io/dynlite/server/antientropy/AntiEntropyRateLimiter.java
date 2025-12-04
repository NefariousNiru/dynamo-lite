// file: server/src/main/java/io/dynlite/server/antientropy/AntiEntropyRateLimiter.java
package io.dynlite.server.antientropy;

import io.dynlite.server.shard.ShardDescriptor;

/**
 * Simple interface for anti-entropy backpressure.
 *
 * Implementations are responsible for deciding how many "repair tokens"
 * (e.g., key-tokens) are allowed in the current time window for a given shard.
 *
 * The contract:
 *   - requested >= 0
 *   - return value is in [0, requested]
 *   - 0 means "do not repair anything this round" (caller should skip)
 */
public interface AntiEntropyRateLimiter {

    /**
     * Try to acquire a budget of repair tokens for the current round.
     *
     * @param shard          shard being repaired (for tagging / metrics).
     * @param requestedTokens how many token repairs the caller would like to perform.
     * @return how many tokens are actually allowed this round (0..requestedTokens).
     */
    int tryAcquireTokens(ShardDescriptor shard, int requestedTokens);
}
