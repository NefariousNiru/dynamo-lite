// file: server/src/test/java/io/dynlite/server/slo/ReplicaLatencyTrackerTest.java
package io.dynlite.server.slo;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ReplicaLatencyTrackerTest {

    @Test
    void quantilesIncreaseWithHigherLatencies() {
        ReplicaLatencyTracker tracker = new ReplicaLatencyTracker(0.3, 256);

        String replica = "node-1";

        // Simulate a mix of latencies.
        double[] samplesMs = {5, 10, 15, 20, 25, 30, 40, 50, 60, 80, 100};
        for (double s : samplesMs) {
            tracker.recordSample(replica, s);
        }

        double p95 = tracker.estimateP95Millis(replica);
        double p99 = tracker.estimateP99Millis(replica);

        assertFalse(Double.isNaN(p95), "p95 should be defined after a few samples");
        assertFalse(Double.isNaN(p99), "p99 should be defined after a few samples");

        // Very loose sanity bounds: we just care that they land in a plausible range.
        assertTrue(p95 >= 40 && p95 <= 120, "p95 should sit near the tail");
        assertTrue(p99 >= p95, "p99 should be >= p95");
    }

    @Test
    void unknownReplicaHasNoStats() {
        ReplicaLatencyTracker tracker = new ReplicaLatencyTracker(0.3, 256);
        assertTrue(Double.isNaN(tracker.estimateP95Millis("missing")));
        assertTrue(Double.isNaN(tracker.estimateP99Millis("missing")));
    }
}
