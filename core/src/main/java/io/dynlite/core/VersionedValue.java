// file: src/main/java/io/dynlite/core/VersionedValue.java
package io.dynlite.core;

import java.util.Arrays;
import java.util.Objects;

/**
 * Immutable envelope for a value stored in DynamoLite.
 * <p>
 * Fields:
 *  - value:      opaque payload as bytes (null when this is a tombstone).
 *  - tombstone:  delete marker that replicates like a Write.
 *  - clock:      vector clock encoding causal history.
 *  - lwwMillis:  wall-clock timestamp used only for "last-writer-wins"
 *                tie-breaking in the UI layer. It is not part of the causal
 *                ordering and must not be used for correctness.
 * <p>
 * Invariants:
 *  - All fields are immutable.
 *  - Defensive copies of the value bytes are taken on input and output.
 */
public final class VersionedValue {
    private final byte[] value;
    private final boolean tombstone;
    private final VectorClock clock;
    private final long lwwMillis;

    public VersionedValue(byte[] value, boolean tombstone, VectorClock clock, long lwwMillis) {
        // Defensive copy to ensure external code cannot mutate our internal bytes.
        this.value = value == null ? null : Arrays.copyOf(value, value.length);
        this.tombstone = tombstone;
        this.clock = Objects.requireNonNull(clock, "clock");
        this.lwwMillis = lwwMillis;
    }

    public byte[] value() { return value == null ? null : Arrays.copyOf(value, value.length); }

    public boolean tombstone() { return tombstone; }

    public VectorClock clock() { return clock; }

    public long lwwMillis() { return lwwMillis; }
}
