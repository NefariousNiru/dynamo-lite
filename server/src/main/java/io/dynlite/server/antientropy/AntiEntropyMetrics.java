// file: server/src/main/java/io/dynlite/server/antientropy/AntiEntropyMetrics.java
package io.dynlite.server.antientropy;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Simple in-memory counters for comparing FIFO vs RAAE anti-entropy.
 *
 * This is intentionally minimal and JVM-local:
 *  - No export / registry yet (can be wired to Micrometer, Prometheus, etc. later).
 *  - Thread-safe via AtomicLong.
 *
 * Tracked metrics (per mode):
 *  - tokensConsidered: how many tokens were seen as "differing" in AE sessions.
 *  - tokensSelected:   how many tokens were actually chosen for repair under
 *                      bandwidth constraints / scheduling policy.
 *  - sessions:         how many AE sessions were run.
 */
public final class AntiEntropyMetrics {

    private final AtomicLong fifoTokensConsidered = new AtomicLong();
    private final AtomicLong fifoTokensSelected   = new AtomicLong();
    private final AtomicLong fifoSessions         = new AtomicLong();

    private final AtomicLong raaeTokensConsidered = new AtomicLong();
    private final AtomicLong raaeTokensSelected   = new AtomicLong();
    private final AtomicLong raaeSessions         = new AtomicLong();

    public void recordSession(AntiEntropyMode mode, int considered, int selected) {
        if (considered < 0 || selected < 0) {
            return;
        }
        switch (mode) {
            case FIFO -> {
                fifoSessions.incrementAndGet();
                fifoTokensConsidered.addAndGet(considered);
                fifoTokensSelected.addAndGet(selected);
            }
            case RAAE -> {
                raaeSessions.incrementAndGet();
                raaeTokensConsidered.addAndGet(considered);
                raaeTokensSelected.addAndGet(selected);
            }
        }
    }

    // --- Read-only views for debugging / future export ---

    public long fifoTokensConsidered() { return fifoTokensConsidered.get(); }
    public long fifoTokensSelected()   { return fifoTokensSelected.get(); }
    public long fifoSessions()         { return fifoSessions.get(); }

    public long raaeTokensConsidered() { return raaeTokensConsidered.get(); }
    public long raaeTokensSelected()   { return raaeTokensSelected.get(); }
    public long raaeSessions()         { return raaeSessions.get(); }

    @Override
    public String toString() {
        return "AntiEntropyMetrics{" +
                "fifoSessions=" + fifoSessions.get() +
                ", fifoTokensConsidered=" + fifoTokensConsidered.get() +
                ", fifoTokensSelected=" + fifoTokensSelected.get() +
                ", raaeSessions=" + raaeSessions.get() +
                ", raaeTokensConsidered=" + raaeTokensConsidered.get() +
                ", raaeTokensSelected=" + raaeTokensSelected.get() +
                '}';
    }
}
