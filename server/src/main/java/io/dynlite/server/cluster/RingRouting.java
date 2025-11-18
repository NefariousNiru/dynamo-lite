// file: src/main/java/io/dynlite/server/cluster/RingRouting.java
package io.dynlite.server.cluster;

import io.dynlite.core.HashRing;

import java.util.List;
import java.util.Objects;

/**
 * Thin wrapper that ties ClusterConfig to the HashRing.
 * <p>
 * Responsibilities:
 *  - Build a consistent hash ring over all cluster nodes using vnodes.
 *  - For a given key, compute the replica set (N owners).
 */
public final class RingRouting {

    private final ClusterConfig cluster;
    private final HashRing ring;

    public RingRouting(ClusterConfig cluster) {
        this.cluster = Objects.requireNonNull(cluster, "cluster");
        this.ring = HashRing.build(
                cluster.nodes().stream()
                        .map(n -> new HashRing.NodeSpec(n.nodeId()))
                        .toList(),
                cluster.vnodes()
        );
    }

    /**
     * Return the ordered list of nodeIds responsible for 'key',
     * according to the cluster's replication factor N.
     * The list is ordered clockwise around the ring starting from the key's hash.
     */
    public List<String> replicaSetForKey(String key) {
        return ring.ownersForKey(key, cluster.replicationFactor());
    }

    /**
     * Convenience: is the local node part of the replica set for this key?
     */
    public boolean localNodeIsReplica(String key) {
        String localId = cluster.localNodeId();
        return replicaSetForKey(key).contains(localId);
    }

    /**
     * Return the index of the local node in the replica set for this key,
     * or -1 if the local node is not a replica.
     */
    public int localReplicaIndex(String key) {
        String localId = cluster.localNodeId();
        List<String> replicas = replicaSetForKey(key);
        for (int i = 0; i < replicas.size(); i++) {
            if (replicas.get(i).equals(localId)) {
                return i;
            }
        }
        return -1;
    }

    public ClusterConfig cluster() {
        return cluster;
    }
}