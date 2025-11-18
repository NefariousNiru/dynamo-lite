// file: server/src/main/java/io/dynlite/server/cluster/ConsistencyMetrics.java
package io.dynlite.server.cluster;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Minimal consistency metrics:
 *  - totalReads: number of GET operations coordinated.
 *  - readsWithStaleReplicas: reads where at least one replica was stale vs winner.
 *  - readsWithSiblings: reads where multiple maximal versions (siblings) were seen.
 *  - readsBelowQuorum: reads where successes < R (quorum failure).
 *  - totalReplicaContacts: total replica GET calls attempted.
 *
 * This is deliberately simple; later you can expose these via an admin endpoint
 * or integrate with a real metrics backend.
 */
public final class ConsistencyMetrics {

    private static final AtomicLong totalReads = new AtomicLong();
    private static final AtomicLong readsWithStaleReplicas = new AtomicLong();
    private static final AtomicLong readsWithSiblings = new AtomicLong();
    private static final AtomicLong readsBelowQuorum = new AtomicLong();
    private static final AtomicLong totalReplicaContacts = new AtomicLong();

    private ConsistencyMetrics() {
    }

    /**
     * Record the outcome of a single coordinated GET.
     *
     * @param found              whether any replica had a value for the key
     * @param replicaContacts    how many replicas were successfully contacted
     * @param hadStaleReplica    whether at least one replica was stale vs the returned winner
     * @param hadSiblings        whether multiple maximal versions (siblings) were observed
     * @param belowQuorum        whether successes < R for this read
     */
    public static void recordRead(
            boolean found,
            int replicaContacts,
            boolean hadStaleReplica,
            boolean hadSiblings,
            boolean belowQuorum
    ) {
        totalReads.incrementAndGet();
        totalReplicaContacts.addAndGet(replicaContacts);
        if (hadStaleReplica) {
            readsWithStaleReplicas.incrementAndGet();
        }
        if (hadSiblings) {
            readsWithSiblings.incrementAndGet();
        }
        if (belowQuorum) {
            readsBelowQuorum.incrementAndGet();
        }
    }

    /**
     * Snapshot of metrics for debugging or an admin endpoint.
     */
    public static Snapshot snapshot() {
        long r = totalReads.get();
        long stale = readsWithStaleReplicas.get();
        long sibs = readsWithSiblings.get();
        long below = readsBelowQuorum.get();
        long contacts = totalReplicaContacts.get();

        return new Snapshot(r, stale, sibs, below, contacts);
    }

    public static void reset() {
        totalReads.set(0L);
        readsWithStaleReplicas.set(0L);
        readsWithSiblings.set(0L);
        readsBelowQuorum.set(0L);
        totalReplicaContacts.set(0L);
    }

    public record Snapshot(
            long totalReads,
            long readsWithStaleReplicas,
            long readsWithSiblings,
            long readsBelowQuorum,
            long totalReplicaContacts
    ) {
    }
}
