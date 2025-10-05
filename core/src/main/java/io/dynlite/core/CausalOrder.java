package io.dynlite.core;

/**
 * Vector-clock partial order between left and right.
 * EQUAL: identical counters.
 * LEFT_DOMINATES_RIGHT: LEFT >= RIGHT elementwise and LEFT != RIGHT.
 * RIGHT_DOMINATES_LEFT: RIGHT >= LEFT elementwise and LEFT != RIGHT.
 * CONCURRENT: neither dominates the other.
 */
public enum CausalOrder {
    EQUAL, LEFT_DOMINATES_RIGHT, RIGHT_DOMINATES_LEFT, CONCURRENT;

    /** Convenience for symmetry checks in tests. */
    public CausalOrder swap() {
        return switch (this) {
            case LEFT_DOMINATES_RIGHT -> RIGHT_DOMINATES_LEFT;
            case RIGHT_DOMINATES_LEFT -> LEFT_DOMINATES_RIGHT;
            default -> this;
        };
    }
}
