package io.dynlite.storage;

/**
 * Write-ahead log for durability and recovery.
 * Contract: append is atomic per record, and fsync happens before acking the caller.
 */
public interface Wal extends AutoCloseable {
    /** Append a serialized record and fsync it. Returns logical position for manifest if needed. */
    void append(byte[] serializedRecord);

    /** Create a new segment when size thresholds are hit. */
    void rotateIfNeeded();

    /** Sequential reader used during recovery. Stops at first corrupt or partial record. */
    WalReader openReader();

    interface WalReader extends AutoCloseable {
        /** Returns next valid payload or null at EOF or on first detected corruption at tail. */
        byte[] next();
    }
}
