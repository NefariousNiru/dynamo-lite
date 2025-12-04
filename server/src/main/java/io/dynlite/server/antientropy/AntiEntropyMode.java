// file: server/src/main/java/io/dynlite/server/antientropy/AntiEntropyMode.java
package io.dynlite.server.antientropy;

/**
 * Anti-entropy scheduling mode.
 *
 * FIFO:
 *   - Repair tokens in the order they appear from Merkle diff.
 *   - No awareness of hotness or divergence age.
 *
 * RAAE:
 *   - "Repair-Aware Anti-Entropy":
 *   - Tokens are scored using hotness × divergence age.
 *   - Only the highest-scoring tokens are repaired under a bandwidth cap.
 */
public enum AntiEntropyMode {
    FIFO,
    RAAE
}
