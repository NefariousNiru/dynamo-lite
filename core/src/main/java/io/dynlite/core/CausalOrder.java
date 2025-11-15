// file: src/main/java/io/dynlite/core/CausalOrder.java
package io.dynlite.core;

/**
 * Partial order between two vector clocks.
 * <p>
 * Interpretation for A.compare(B):
 *  - EQUAL:               A and B have identical counters.
 *  - LEFT_DOMINATES_RIGHT: A has seen at least as many events as B
 *                          for every node, and strictly more for at least one.
 *  - RIGHT_DOMINATES_LEFT: symmetric to LEFT_DOMINATES_RIGHT.
 *  - CONCURRENT:          neither dominates the other (conflicting updates).
 */
public enum CausalOrder {
    EQUAL, LEFT_DOMINATES_RIGHT, RIGHT_DOMINATES_LEFT, CONCURRENT;

    /**
     * Return the "perspective" if we swap the left/right arguments.
     * Useful in tests to assert symmetry.
     */
    public CausalOrder swap() {
        return switch (this) {
            case LEFT_DOMINATES_RIGHT -> RIGHT_DOMINATES_LEFT;
            case RIGHT_DOMINATES_LEFT -> LEFT_DOMINATES_RIGHT;
            default -> this;
        };
    }
}
