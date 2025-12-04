// file: server/src/main/java/io/dynlite/server/slo/StalenessBudgetTracker.java
package io.dynlite.server.slo;

/**
 * Rolling-window tracker for the fraction of reads executed in "budgeted"
 * (relaxed consistency) mode.
 *
 * Implementation:
 *  - fixed-size circular buffer of booleans (budgeted or not),
 *  - tracks how many of the last N reads were budgeted,
 *  - exposes currentFraction() and withinBudget(B).
 */
public final class StalenessBudgetTracker {

    private final boolean[] window;
    private final int capacity;

    // guarded by this
    private int size;
    private int index;
    private int budgetedCount;

    /**
     * @param capacity number of reads to track in the rolling window.
     */
    public StalenessBudgetTracker(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be > 0");
        }
        this.window = new boolean[capacity];
        this.capacity = capacity;
    }

    /**
     * Record a single read as budgeted or not.
     *
     * budgeted == true means this read was allowed to violate RYW.
     */
    public synchronized void recordRead(boolean budgeted) {
        if (size < capacity) {
            window[index] = budgeted;
            if (budgeted) {
                budgetedCount++;
            }
            index = (index + 1) % capacity;
            size++;
        } else {
            boolean old = window[index];
            if (old) {
                budgetedCount--;
            }
            window[index] = budgeted;
            if (budgeted) {
                budgetedCount++;
            }
            index = (index + 1) % capacity;
        }
    }

    /**
     * Current fraction of budgeted reads over the window.
     */
    public synchronized double currentFraction() {
        if (size == 0) {
            return 0.0;
        }
        return (double) budgetedCount / (double) size;
    }

    /**
     * Check if current fraction is within B.
     */
    public synchronized boolean withinBudget(double maxBudgetFraction) {
        if (maxBudgetFraction < 0.0 || maxBudgetFraction > 1.0) {
            throw new IllegalArgumentException("maxBudgetFraction must be in [0,1], got " + maxBudgetFraction);
        }
        return currentFraction() <= maxBudgetFraction;
    }
}
