// file: server/src/main/java/io/dynlite/server/slo/ConsistencyHint.java
package io.dynlite.server.slo;

import java.util.Objects;
import java.util.OptionalDouble;

/**
 * Per-request consistency and latency hint supplied by the caller.
 *
 * Semantics:
 *  - deadlineMillis: optional latency SLO for this request.
 *  - allowStaleness: if true, caller is willing to violate read-your-writes
 *                    on some reads in exchange for lower latency.
 *  - maxBudgetedFraction: upper bound B on the long-run fraction of reads
 *                         that are allowed to be "budgeted".
 *
 * This is advisory only. CoordinatorService enforces the staleness budget by
 * occasionally upgrading "budgeted" reads back to "safe" when we are over B.
 */
public final class ConsistencyHint {

    private final Double deadlineMillis;      // null => no explicit SLO
    private final boolean allowStaleness;
    private final double maxBudgetedFraction;

    public ConsistencyHint(Double deadlineMillis,
                            boolean allowStaleness,
                            double maxBudgetedFraction) {
        if (maxBudgetedFraction < 0.0 || maxBudgetedFraction > 1.0) {
            throw new IllegalArgumentException("maxBudgetedFraction must be in [0,1], got " + maxBudgetedFraction);
        }
        this.deadlineMillis = deadlineMillis;
        this.allowStaleness = allowStaleness;
        this.maxBudgetedFraction = maxBudgetedFraction;
    }

    /**
     * Default hint: no deadline, strict RYW, no budgeted reads.
     */
    public static ConsistencyHint none() {
        return new ConsistencyHint(null, false, 0.0);
    }

    /**
     * Hint with a deadline but strict RYW.
     */
    public static ConsistencyHint withDeadlineMillis(double deadlineMillis) {
        if (deadlineMillis <= 0.0) {
            throw new IllegalArgumentException("deadlineMillis must be > 0");
        }
        return new ConsistencyHint(deadlineMillis, false, 0.0);
    }

    /**
     * Full custom hint.
     */
    public static ConsistencyHint of(Double deadlineMillis,
                                     boolean allowStaleness,
                                     double maxBudgetedFraction) {
        return new ConsistencyHint(deadlineMillis, allowStaleness, maxBudgetedFraction);
    }

    public ConsistencyHint withDeadlineMillis(OptionalDouble maybeDeadline) {
        Double dl = maybeDeadline.isPresent() ? maybeDeadline.getAsDouble() : null;
        return new ConsistencyHint(dl, this.allowStaleness, this.maxBudgetedFraction);
    }

    public ConsistencyHint withAllowStaleness(boolean allowStaleness) {
        return new ConsistencyHint(this.deadlineMillis, allowStaleness, this.maxBudgetedFraction);
    }

    public ConsistencyHint withMaxBudgetedFraction(double maxBudgetedFraction) {
        return new ConsistencyHint(this.deadlineMillis, this.allowStaleness, maxBudgetedFraction);
    }

    /**
     * Same deadline/B, but forces strict RYW (no relax).
     */
    public ConsistencyHint asSafeRead() {
        return new ConsistencyHint(this.deadlineMillis, false, this.maxBudgetedFraction);
    }

    public boolean hasDeadline() {
        return deadlineMillis != null;
    }

    public OptionalDouble deadlineMillis() {
        return deadlineMillis == null
                ? OptionalDouble.empty()
                : OptionalDouble.of(deadlineMillis);
    }

    public boolean allowStaleness() {
        return allowStaleness;
    }

    public double maxBudgetedFraction() {
        return maxBudgetedFraction;
    }

    @Override
    public String toString() {
        return "ConsistencyHint{" +
                "deadlineMillis=" + deadlineMillis +
                ", allowStaleness=" + allowStaleness +
                ", maxBudgetedFraction=" + maxBudgetedFraction +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ConsistencyHint that)) return false;
        return allowStaleness == that.allowStaleness
                && Double.compare(that.maxBudgetedFraction, maxBudgetedFraction) == 0
                && Objects.equals(deadlineMillis, that.deadlineMillis);
    }

    @Override
    public int hashCode() {
        return Objects.hash(deadlineMillis, allowStaleness, maxBudgetedFraction);
    }
}
