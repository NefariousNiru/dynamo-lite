// file: server/src/test/java/io/dynlite/server/slo/StalenessBudgetTrackerTest.java
package io.dynlite.server.slo;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class StalenessBudgetTrackerTest {

    @Test
    void withinBudget_respectsThresholdInFixedWindow() {
        // Window of 10 reads.
        StalenessBudgetTracker tracker = new StalenessBudgetTracker(10);

        // First 5 reads use budget, next 5 are safe -> 5/10 = 0.5 budgeted.
        for (int i = 0; i < 5; i++) {
            tracker.recordRead(true);
        }
        for (int i = 0; i < 5; i++) {
            tracker.recordRead(false);
        }

        assertTrue(tracker.withinBudget(0.6), "0.5 <= 0.6 should be within budget");
        assertFalse(tracker.withinBudget(0.4), "0.5 > 0.4 should exceed budget");
    }

    @Test
    void slidingWindowDropsOldReads() {
        // Window of 4 reads.
        StalenessBudgetTracker tracker = new StalenessBudgetTracker(4);

        // Start with 4 budgeted reads -> fraction = 1.0
        for (int i = 0; i < 4; i++) {
            tracker.recordRead(true);
        }
        assertFalse(tracker.withinBudget(0.5), "all budgeted -> exceed 0.5");

        // Add safe reads to push old ones out of window.
        tracker.recordRead(false); // window now last 4, includes 3 budgeted, 1 safe -> 3/4 = 0.75
        tracker.recordRead(false); // now 2 budgeted, 2 safe -> 0.5
        assertTrue(tracker.withinBudget(0.5), "0.5 should be within 0.5 threshold");
        assertFalse(tracker.withinBudget(0.4), "0.5 should exceed 0.4 threshold");
    }
}
