// file: server/src/main/java/io/dynlite/server/slo/ReplicaLatencyTracker.java
package io.dynlite.server.slo;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Per-replica latency tracker for SLO-driven routing and hedged reads.
 *
 * For each nodeId we keep:
 *  - EWMA of latency in milliseconds,
 *  - circular buffer of recent samples (maxSamples),
 *  - p95/p99 estimates from the buffer.
 */
public final class ReplicaLatencyTracker {

    public record Stats(
            double ewmaMillis,
            double p95Millis,
            double p99Millis,
            int sampleCount
    ) {}

    private static final class Window {
        private final double[] samples;
        private final int capacity;
        private int size;
        private int index;
        private double ewma;
        private boolean hasEwma;

        Window(int capacity) {
            this.samples = new double[capacity];
            this.capacity = capacity;
        }

        synchronized void add(double millis, double alpha) {
            if (millis < 0.0) return;

            if (!hasEwma) {
                ewma = millis;
                hasEwma = true;
            } else {
                ewma = (1.0 - alpha) * ewma + alpha * millis;
            }

            samples[index] = millis;
            index = (index + 1) % capacity;
            if (size < capacity) {
                size++;
            }
        }

        synchronized Stats snapshot() {
            if (size == 0) {
                return new Stats(
                        hasEwma ? ewma : Double.NaN,
                        Double.NaN,
                        Double.NaN,
                        0
                );
            }

            double[] copy = new double[size];
            for (int i = 0; i < size; i++) {
                int pos = (index - 1 - i + capacity) % capacity;
                copy[i] = samples[pos];
            }
            Arrays.sort(copy);

            double p95 = percentile(copy, 0.95);
            double p99 = percentile(copy, 0.99);

            return new Stats(ewma, p95, p99, size);
        }

        private static double percentile(double[] sorted, double q) {
            if (sorted.length == 0) return Double.NaN;
            double idx = q * (sorted.length - 1);
            int lo = (int) Math.floor(idx);
            int hi = (int) Math.ceil(idx);
            if (lo == hi) return sorted[lo];
            double w = idx - lo;
            return sorted[lo] * (1.0 - w) + sorted[hi] * w;
        }
    }

    private final double alpha;
    private final int maxSamples;
    private final ConcurrentHashMap<String, Window> windows = new ConcurrentHashMap<>();

    public ReplicaLatencyTracker(double alpha, int maxSamples) {
        if (!(alpha > 0.0 && alpha <= 1.0)) {
            throw new IllegalArgumentException("alpha must be in (0,1], got " + alpha);
        }
        if (maxSamples <= 0) {
            throw new IllegalArgumentException("maxSamples must be > 0");
        }
        this.alpha = alpha;
        this.maxSamples = maxSamples;
    }

    private Window windowFor(String nodeId) {
        return windows.computeIfAbsent(nodeId, id -> new Window(maxSamples));
    }

    public void recordSample(String nodeId, double millis) {
        if (nodeId == null || nodeId.isBlank()) return;
        windowFor(nodeId).add(millis, alpha);
    }

    public Stats stats(String nodeId) {
        Window w = windows.get(nodeId);
        if (w == null) {
            return new Stats(Double.NaN, Double.NaN, Double.NaN, 0);
        }
        return w.snapshot();
    }

    public double estimateEwmaMillis(String nodeId) {
        return stats(nodeId).ewmaMillis();
    }

    public double estimateP95Millis(String nodeId) {
        return stats(nodeId).p95Millis();
    }

    public double estimateP99Millis(String nodeId) {
        return stats(nodeId).p99Millis();
    }

    public Map<String, Stats> snapshotAll() {
        return windows.entrySet().stream()
                .collect(Collectors.toUnmodifiableMap(
                        Map.Entry::getKey,
                        e -> e.getValue().snapshot()
                ));
    }
}
