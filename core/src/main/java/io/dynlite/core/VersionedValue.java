package io.dynlite.core;

import java.util.Arrays;
import java.util.Objects;

/**
 * Immutable value + causal metadata for a key.
 * - value: opaque bytes (null when tombstone=true)
 * - tombstone: delete marker that replicates like a write
 * - clock: vector clock for causality
 * - lwwMillis: coordinator timestamp for demo LWW resolver only
 */
public final class VersionedValue {
    private final byte[] value;
    private final boolean tombstone;
    private final VectorClock clock;
    private final long lwwMillis;

    public VersionedValue(byte[] value, boolean tombstone, VectorClock clock, long lwwMillis) {
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
