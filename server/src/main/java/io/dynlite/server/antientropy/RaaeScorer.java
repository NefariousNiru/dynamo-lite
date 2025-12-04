// file: server/src/main/java/io/dynlite/server/antientropy/RaaeScorer.java
package io.dynlite.server.antientropy;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;

/**
 * Combines hotness and divergence age into a single repair priority score.
 *
 * Concept:
 *  - Hot keys matter more to users.
 *  - Old divergences are more dangerous (long-standing inconsistency).
 *  - We define a simple multiplicative score:
 *        score(token) = hotness(token) * ageMillis(token)
 *
 * This is intentionally simple for now:
 *  - Both inputs are always >= 0.
 *  - If either is 0, the score is 0 (no urgency).
 *  - Units do not matter, we only use scores for relative ordering.
 */
public final class RaaeScorer {

    private final RaaeHotnessTracker hotnessTracker;
    private final RaaeDivergenceTracker divergenceTracker;

    public record ScoredToken(long token, double score) {}

    public RaaeScorer(RaaeHotnessTracker hotnessTracker,
                      RaaeDivergenceTracker divergenceTracker) {
        this.hotnessTracker = hotnessTracker;
        this.divergenceTracker = divergenceTracker;
    }

    /**
     * Compute a score for a single token at time {@code nowMillis}.
     *
     * @return non-negative score; 0 means "no urgency" (cold or not divergent).
     */
    public double score(long token, long nowMillis) {
        double hot = hotnessTracker.getHotness(token);
        long ageMs = divergenceTracker.ageMillis(token, nowMillis);
        if (hot <= 0.0 || ageMs <= 0L) {
            return 0.0;
        }
        // Simple multiplicative combination. If this proves too skewed, we
        // can move to log-scaled age or a different weighting later.
        return hot * (double) ageMs;
    }

    /**
     * Given a set of candidate tokens, return a list sorted by descending score.
     * Tokens with zero score are included but end up at the tail.
     */
    public List<ScoredToken> rankTokens(Collection<Long> tokens, long nowMillis) {
        List<ScoredToken> out = new ArrayList<>(tokens.size());
        for (long token : tokens) {
            double s = score(token, nowMillis);
            out.add(new ScoredToken(token, s));
        }
        out.sort(Comparator.comparingDouble(ScoredToken::score).reversed());
        return out;
    }
}
