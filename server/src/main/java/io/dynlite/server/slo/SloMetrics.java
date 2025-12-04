// file: server/src/main/java/io/dynlite/server/slo/SloMetrics.java
package io.dynlite.server.slo;

import java.util.concurrent.atomic.AtomicLong;

/**
 * In-memory SLO metrics.
 *
 * Tracks:
 *  - sloHits / sloMisses:
 *      requests with deadlines that finished within/outside their SLO.
 *  - safeReads / safeStaleReads:
 *      strict RYW reads vs how many of those saw potential staleness.
 *  - budgetedReads / budgetedStaleReads:
 *      relaxed reads vs how many of those saw potential staleness.
 */
public final class SloMetrics {

    private final AtomicLong sloHits = new AtomicLong();
    private final AtomicLong sloMisses = new AtomicLong();

    private final AtomicLong safeReads = new AtomicLong();
    private final AtomicLong safeStaleReads = new AtomicLong();

    private final AtomicLong budgetedReads = new AtomicLong();
    private final AtomicLong budgetedStaleReads = new AtomicLong();

    public void recordLatencyOutcome(ConsistencyHint hint, double elapsedMs) {
        var dlOpt = hint.deadlineMillis();
        if (dlOpt.isEmpty()) {
            return;
        }
        double deadline = dlOpt.getAsDouble();
        if (elapsedMs <= deadline) {
            sloHits.incrementAndGet();
        } else {
            sloMisses.incrementAndGet();
        }
    }

    public void recordReadOutcome(boolean budgeted, boolean staleObserved) {
        if (budgeted) {
            budgetedReads.incrementAndGet();
            if (staleObserved) {
                budgetedStaleReads.incrementAndGet();
            }
        } else {
            safeReads.incrementAndGet();
            if (staleObserved) {
                safeStaleReads.incrementAndGet();
            }
        }
    }

    public long sloHits()               { return sloHits.get(); }
    public long sloMisses()             { return sloMisses.get(); }
    public long safeReads()             { return safeReads.get(); }
    public long safeStaleReads()        { return safeStaleReads.get(); }
    public long budgetedReads()         { return budgetedReads.get(); }
    public long budgetedStaleReads()    { return budgetedStaleReads.get(); }

    @Override
    public String toString() {
        return "SloMetrics{" +
                "sloHits=" + sloHits.get() +
                ", sloMisses=" + sloMisses.get() +
                ", safeReads=" + safeReads.get() +
                ", safeStaleReads=" + safeStaleReads.get() +
                ", budgetedReads=" + budgetedReads.get() +
                ", budgetedStaleReads=" + budgetedStaleReads.get() +
                '}';
    }
}
