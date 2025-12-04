// file: server/src/main/java/io/dynlite/server/antientropy/RaaeAwareRepairExecutor.java
package io.dynlite.server.antientropy;

import io.dynlite.server.antientropy.AntiEntropySession.RepairExecutor;
import io.dynlite.server.shard.ShardDescriptor;

import java.time.Clock;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * RepairExecutor that implements:
 *   - FIFO baseline anti-entropy, and
 *   - RAAE (Repair-Aware Anti-Entropy) priority scheduling.
 *
 * Responsibilities:
 *   - track divergence age for tokens whenever Merkle diffs report them,
 *   - select which tokens should be repaired under a bandwidth cap,
 *   - apply backpressure via {@link AntiEntropyRateLimiter},
 *   - notify {@link AntiEntropyMetrics} so we can compare FIFO vs RAAE behavior.
 *
 * This class deliberately does NOT implement the actual data copy between nodes.
 * A higher-level component can:
 *   - map token -> key(s),
 *   - fetch siblings from local + remote,
 *   - resolve conflicts and write back via NodeClient/KvService.
 */
public final class RaaeAwareRepairExecutor implements RepairExecutor {

    private final AntiEntropyMode mode;
    private final int maxTokensPerRun;
    private final RaaeHotnessTracker hotnessTracker;
    private final RaaeDivergenceTracker divergenceTracker;
    private final RaaeScorer scorer;
    private final AntiEntropyRateLimiter rateLimiter;
    private final AntiEntropyMetrics metrics;
    private final RaaePriorityScheduler priorityScheduler;
    private final Clock clock;

    /**
     * Low-level constructor; prefer the static factories for clarity.
     */
    public RaaeAwareRepairExecutor(
            AntiEntropyMode mode,
            int maxTokensPerRun,
            RaaeHotnessTracker hotnessTracker,
            RaaeDivergenceTracker divergenceTracker,
            RaaeScorer scorer,
            AntiEntropyRateLimiter rateLimiter,
            AntiEntropyMetrics metrics,
            RaaePriorityScheduler priorityScheduler,
            Clock clock
    ) {
        this.mode = mode;
        this.maxTokensPerRun = maxTokensPerRun;
        this.hotnessTracker = hotnessTracker;
        this.divergenceTracker = divergenceTracker;
        this.scorer = scorer;
        this.rateLimiter = rateLimiter;
        this.metrics = metrics;
        this.priorityScheduler = priorityScheduler;
        this.clock = clock;
    }

    /**
     * Factory for a simple FIFO baseline (no hotness-aware prioritization).
     */
    public static RaaeAwareRepairExecutor fifoBaseline(
            int maxTokensPerRun,
            RaaeHotnessTracker hotnessTracker,
            RaaeDivergenceTracker divergenceTracker,
            AntiEntropyRateLimiter rateLimiter,
            AntiEntropyMetrics metrics,
            RaaePriorityScheduler priorityScheduler,
            Clock clock
    ) {
        var dummyScorer = new RaaeScorer(hotnessTracker, divergenceTracker);
        return new RaaeAwareRepairExecutor(
                AntiEntropyMode.FIFO,
                maxTokensPerRun,
                hotnessTracker,
                divergenceTracker,
                dummyScorer,
                rateLimiter,
                metrics,
                priorityScheduler,
                clock
        );
    }

    /**
     * Factory for full RAAE mode (hotness × age scoring).
     */
    public static RaaeAwareRepairExecutor raaePriority(
            int maxTokensPerRun,
            RaaeHotnessTracker hotnessTracker,
            RaaeDivergenceTracker divergenceTracker,
            RaaeScorer scorer,
            AntiEntropyRateLimiter rateLimiter,
            AntiEntropyMetrics metrics,
            RaaePriorityScheduler priorityScheduler,
            Clock clock
    ) {
        return new RaaeAwareRepairExecutor(
                AntiEntropyMode.RAAE,
                maxTokensPerRun,
                hotnessTracker,
                divergenceTracker,
                scorer,
                rateLimiter,
                metrics,
                priorityScheduler,
                clock
        );
    }

    /**
     * Core entrypoint called by {@link AntiEntropySession} when it finds differences.
     *
     * Steps:
     *  1) Merge pullTokens + pushTokens into a unified set of differing tokens.
     *  2) Record divergence for all of them (they differ from at least one peer).
     *  3) Respect backpressure via {@link AntiEntropyRateLimiter} (drop / shrink if overloaded).
     *  4) Choose up to maxTokensPerRun tokens to actually repair:
     *       - FIFO: original order,
     *       - RAAE: highest hotness×age score first via RaaeScorer/RaaePriorityScheduler.
     *  5) Record per-mode metrics (considered vs selected).
     *  6) Mark selected tokens as converged from the divergence-age perspective.
     *
     * Actual data movement is intentionally left to a higher level.
     */
    @Override
    public void executeRepairs(
            ShardDescriptor shard,
            List<Long> pullTokens,
            List<Long> pushTokens
    ) {
        long nowMillis = clock.millis();

        // 1) De-duplicate into a stable order.
        Set<Long> allTokens = new LinkedHashSet<>();
        allTokens.addAll(pullTokens);
        allTokens.addAll(pushTokens);

        if (allTokens.isEmpty()) {
            return;
        }

        // 2) Track divergence time for all differing tokens.
        for (long token : allTokens) {
            divergenceTracker.recordDivergence(token, nowMillis);
        }

        // 3) Backpressure: consult rate limiter for how many we are allowed to process.
        int requested = Math.min(maxTokensPerRun, allTokens.size());
        int budget = rateLimiter.tryAcquireTokens(shard, requested);
        if (budget <= 0) {
            // Node is under pressure; defer this batch.
            metrics.recordSession(mode, allTokens.size(), 0);
            return;
        }

        // 4) Choose which tokens to actually repair under the cap.
        List<Long> selected;
        switch (mode) {
            case FIFO -> selected = selectFifo(allTokens, budget);
            case RAAE -> selected = selectRaae(shard, allTokens, nowMillis, budget);
            default -> selected = List.of();
        }

        if (selected.isEmpty()) {
            metrics.recordSession(mode, allTokens.size(), 0);
            return;
        }

        // 5) Record metrics so we can compare FIFO vs RAAE behavior later.
        metrics.recordSession(mode, allTokens.size(), selected.size());

        // NOTE: this class is a scheduler, not the data plane.
        // If you want actual repairs, you can:
        //  - add a callback here, or
        //  - have a higher-level component read from RaaePriorityScheduler
        //    plus DivergenceTracker and drive NodeClient/KvService operations.

        // 6) Mark selected tokens as "converged" from divergence-age perspective.
        for (long token : selected) {
            divergenceTracker.clearConverged(token);
        }
    }

    // ---------- selection strategies ----------

    /**
     * FIFO baseline: respect the original order from the Merkle diff,
     * up to the rate-limited budget.
     */
    private List<Long> selectFifo(Set<Long> allTokens, int budget) {
        List<Long> out = new ArrayList<>(Math.min(budget, allTokens.size()));
        int count = 0;
        for (long token : allTokens) {
            out.add(token);
            count++;
            if (count >= budget) {
                break;
            }
        }
        return out;
    }

    /**
     * RAAE selection:
     *  - score tokens using hotness × divergence age,
     *  - feed scores into the global priority scheduler,
     *  - drain up to min(budget, maxTokensPerRun, globalBandwidthCap).
     */
    private List<Long> selectRaae(
            ShardDescriptor shard,
            Set<Long> allTokens,
            long nowMillis,
            int budget
    ) {
        // Rank locally by score.
        var scored = scorer.rankTokens(allTokens, nowMillis);

        // Offer into the global PQ so we can prioritize across shards.
        priorityScheduler.offerScores(shard, scored, nowMillis);

        // Drain in priority order under the node-level budget + global cap.
        var scheduled = priorityScheduler.drain(Math.min(budget, maxTokensPerRun));

        List<Long> out = new ArrayList<>(scheduled.size());
        for (var st : scheduled) {
            out.add(st.token());
        }
        return out;
    }
}
