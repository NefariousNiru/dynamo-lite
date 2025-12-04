// file: bench/src/main/java/io/dynlite/bench/ZipfianKeyGenerator.java
package io.dynlite.bench;

import java.util.Random;

/**
 * Simple Zipfian key generator for integer key ids in [0, n).
 *
 * This is a crude implementation: precomputes the harmonic weights once
 * and then uses a binary search over CDF for sampling.
 */
public final class ZipfianKeyGenerator {

    private final int n;
    private final double[] cdf;
    private final Random rnd;

    public ZipfianKeyGenerator(int n, double skew, long seed) {
        if (n <= 0) throw new IllegalArgumentException("n must be > 0");
        if (skew <= 0.0) throw new IllegalArgumentException("skew must be > 0");

        this.n = n;
        this.rnd = new Random(seed);

        double[] w = new double[n];
        double sum = 0.0;
        for (int i = 0; i < n; i++) {
            double weight = 1.0 / Math.pow(i + 1, skew);
            w[i] = weight;
            sum += weight;
        }
        this.cdf = new double[n];
        double running = 0.0;
        for (int i = 0; i < n; i++) {
            running += w[i] / sum;
            cdf[i] = running;
        }
    }

    /** Return a key id in [0, n). */
    public int nextKey() {
        double u = rnd.nextDouble();
        int lo = 0;
        int hi = n - 1;
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            if (u <= cdf[mid]) {
                hi = mid;
            } else {
                lo = mid + 1;
            }
        }
        return lo;
    }
}
