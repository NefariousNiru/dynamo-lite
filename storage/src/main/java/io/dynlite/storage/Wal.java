// file: src/main/java/io/dynlite/storage/Wal.java
package io.dynlite.storage;

/**
 * Write-Ahead Log abstraction for durability and recovery.
 * <p>
 * Contract:
 *  - append() is atomic at "record" granularity: a partial write is treated
 *    as absent during recovery (reader stops at first corrupt/truncated record).
 *  - append() must fsync the record to disk before returning, so that if
 *    the process crashes after append() returns, recovery will see the record.
 */
public interface Wal extends AutoCloseable {

    /**
     * Append a single serialized record and fsync it.
     *
     * @param serializedRecord header+payload bytes, typically from RecordCodec.encode(...)
     */
    void append(byte[] serializedRecord);

    /**
     * Rotate log segment if configured thresholds are hit. (Log Rotations)
     * Called by the store after each write.
     */
    void rotateIfNeeded();

    /**
     * Open a sequential reader over the WAL.
     * Reader starts from the earliest segment and stops at:
     *  - first corrupt header,
     *  - first truncated payload, or
     *  - end of file.
     */
    WalReader openReader();

    /**
     * Reader abstraction used during recovery.
     */
    interface WalReader extends AutoCloseable {

        /**
         * @return next valid payload (NOT including header), or null when:
         *   - at EOF, or
         *   - corruption/truncation is detected at the tail.
         */
        byte[] next();
    }
}
