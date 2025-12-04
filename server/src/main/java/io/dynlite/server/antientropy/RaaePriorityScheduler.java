// file: server/src/main/java/io/dynlite/server/antientropy/RaaePriorityScheduler.java
package io.dynlite.server.antientropy;

import io.dynlite.server.shard.ShardDescriptor;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Objects;

/**
 * Global priority queue of tokens ("leaves") across shards.
 *
 * This scheduler:
 *   - accepts scored tokens from RAAE (token -> score),
 *   - stores them in a max-heap,
 *   - on each drain() call, returns the highest-score tokens up to a
 *     global bandwidth cap.
 *
 * It does NOT directly perform repairs. Instead, it determines
 * which (shard, token) pairs should be repaired first, and the
 * caller (RaaeAwareRepairExecutor) uses that ordering together
 * with a node-level rate limiter.
 */
public final class RaaePriorityScheduler {

    /**
     * Single scheduled token entry.
     *
     * shard       - which shard this token belongs to.
     * token       - ring token.
     * score       - RAAE score (higher = more urgent).
     * createdAtMs - when this entry was inserted (for tie-breaking).
     */
    public static final class ScheduledToken {
        private final ShardDescriptor shard;
        private final long token;
        private final double score;
        private final long createdAtMs;

        public ScheduledToken(ShardDescriptor shard, long token, double score, long createdAtMs) {
            this.shard = Objects.requireNonNull(shard, "shard");
            this.token = token;
            this.score = score;
            this.createdAtMs = createdAtMs;
        }

        public ShardDescriptor shard() { return shard; }
        public long token() { return token; }
        public double score() { return score; }
        public long createdAtMs() { return createdAtMs; }
    }

    private final PriorityQueue<ScheduledToken> queue;
    private final int globalBandwidthCap;

    /**
     * @param globalBandwidthCap maximum number of tokens that will ever be
     *                           returned by a single drain() call.
     */
    public RaaePriorityScheduler(int globalBandwidthCap) {
        if (globalBandwidthCap <= 0) {
            throw new IllegalArgumentException("globalBandwidthCap must be > 0");
        }
        this.globalBandwidthCap = globalBandwidthCap;
        this.queue = new PriorityQueue<>(
                Comparator
                        .comparingDouble(ScheduledToken::score).reversed()
                        .thenComparingLong(ScheduledToken::createdAtMs)
        );
    }

    /**
     * Offer scored tokens for a shard.
     *
     * This does not apply any deduplication; repeated offers for the same
     * token will result in multiple entries. For the prototype this is fine,
     * since we expect bounded diffs and frequent convergence.
     */
    public synchronized void offerScores(
            ShardDescriptor shard,
            List<RaaeScorer.ScoredToken> scoredTokens,
            long nowMillis
    ) {
        if (scoredTokens == null || scoredTokens.isEmpty()) {
            return;
        }
        for (RaaeScorer.ScoredToken st : scoredTokens) {
            queue.add(new ScheduledToken(shard, st.token(), st.score(), nowMillis));
        }
    }

    /**
     * Drain tokens in global priority order.
     *
     * @param desired maximum number of tokens the caller wants to process.
     * @return list of up to min(desired, globalBandwidthCap, queue.size())
     *         scheduled tokens, ordered by descending score.
     */
    public synchronized List<ScheduledToken> drain(int desired) {
        if (desired <= 0 || queue.isEmpty()) {
            return List.of();
        }
        int limit = Math.min(desired, globalBandwidthCap);
        List<ScheduledToken> out = new ArrayList<>(limit);
        for (int i = 0; i < limit && !queue.isEmpty(); i++) {
            out.add(queue.poll());
        }
        return out;
    }

    /**
     * For tests / debugging.
     */
    public synchronized int pendingSize() {
        return queue.size();
    }
}
