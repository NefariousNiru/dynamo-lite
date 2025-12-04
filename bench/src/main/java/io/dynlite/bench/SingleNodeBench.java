// file: bench/src/main/java/io/dynlite/bench/SingleNodeBench.java
package io.dynlite.bench;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Microbenchmark + YCSB-like workload driver against a single HTTP node.
 *
 * Usage:
 *   java -jar bench.jar \
 *     --base-url http://localhost:8080 \
 *     --threads 8 \
 *     --duration-seconds 30 \
 *     --keyspace 100000 \
 *     --value-bytes 512 \
 *     --write-ratio 0.5 \
 *     --zipf-skew 0.99
 *
 * Output:
 *   - Summary line to stderr.
 *   - CSV to stdout with per-op latency samples:
 *       op,success,latency_ms
 */
public final class SingleNodeBench {

    private static final class Sample {
        final String op;
        final boolean ok;
        final double latencyMs;

        Sample(String op, boolean ok, double latencyMs) {
            this.op = op;
            this.ok = ok;
            this.latencyMs = latencyMs;
        }
    }

    public static void main(String[] args) throws Exception {
        Map<String, String> cfg = parseArgs(args);

        String baseUrl = cfg.getOrDefault("base-url", "http://localhost:8080");
        int threads = Integer.parseInt(cfg.getOrDefault("threads", "4"));
        int durationSeconds = Integer.parseInt(cfg.getOrDefault("duration-seconds", "30"));
        int keyspace = Integer.parseInt(cfg.getOrDefault("keyspace", "100000"));
        int valueBytes = Integer.parseInt(cfg.getOrDefault("value-bytes", "512"));
        double writeRatio = Double.parseDouble(cfg.getOrDefault("write-ratio", "0.5"));
        double zipfSkew = Double.parseDouble(cfg.getOrDefault("zipf-skew", "0.99"));

        runBenchmark(baseUrl, threads, durationSeconds, keyspace, valueBytes, writeRatio, zipfSkew);
    }

    private static Map<String, String> parseArgs(String[] args) {
        Map<String, String> out = new HashMap<>();
        for (int i = 0; i < args.length; i++) {
            String a = args[i];
            if (a.startsWith("--")) {
                String key = a.substring(2);
                if (i + 1 >= args.length) {
                    throw new IllegalArgumentException("missing value for " + a);
                }
                out.put(key, args[++i]);
            } else {
                throw new IllegalArgumentException("unexpected arg: " + a);
            }
        }
        return out;
    }

    private static void runBenchmark(
            String baseUrl,
            int threads,
            int durationSeconds,
            int keyspace,
            int valueBytes,
            double writeRatio,
            double zipfSkew
    ) throws Exception {

        HttpClient client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(3))
                .build();

        byte[] value = new byte[valueBytes];
        Arrays.fill(value, (byte) 'x');
        String valueB64 = Base64.getEncoder().encodeToString(value);

        ZipfianKeyGenerator zipf = new ZipfianKeyGenerator(keyspace, zipfSkew, 42L);

        ExecutorService exec = Executors.newFixedThreadPool(threads);
        BlockingQueue<Sample> samples = new LinkedBlockingQueue<>();
        AtomicLong opCount = new AtomicLong();
        long endTime = System.nanoTime() + TimeUnit.SECONDS.toNanos(durationSeconds);

        Runnable worker = () -> {
            ThreadLocalRandom rnd = ThreadLocalRandom.current();
            while (System.nanoTime() < endTime) {
                boolean isWrite = rnd.nextDouble() < writeRatio;
                int keyId = zipf.nextKey();
                String key = "key-" + keyId;

                String op = isWrite ? "PUT" : "GET";
                long start = System.nanoTime();
                boolean ok = false;
                try {
                    if (isWrite) {
                        doPut(client, baseUrl, key, valueB64);
                    } else {
                        doGet(client, baseUrl, key);
                    }
                    ok = true;
                } catch (Exception ignored) {
                    ok = false;
                } finally {
                    double latencyMs = (System.nanoTime() - start) / 1_000_000.0;
                    samples.add(new Sample(op, ok, latencyMs));
                    opCount.incrementAndGet();
                }
            }
        };

        for (int i = 0; i < threads; i++) {
            exec.submit(worker);
        }
        exec.shutdown();
        exec.awaitTermination(durationSeconds + 5L, TimeUnit.SECONDS);

        List<Sample> all = new ArrayList<>(samples.size());
        samples.drainTo(all);

        summarizeAndPrint(all, opCount.get(), durationSeconds);
    }

    private static void doPut(HttpClient client, String baseUrl, String key, String valueB64) throws Exception {
        String body = "{\"valueBase64\":\"" + valueB64 + "\",\"nodeId\":null,\"opId\":null}";

        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/kv/" + key))
                .header("Content-Type", "application/json")
                .PUT(HttpRequest.BodyPublishers.ofString(body))
                .build();

        HttpResponse<String> resp = client.send(req, HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() != 200) {
            throw new RuntimeException("PUT failed: " + resp.statusCode());
        }
    }

    private static void doGet(HttpClient client, String baseUrl, String key) throws Exception {
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/kv/" + key))
                .GET()
                .build();

        HttpResponse<String> resp = client.send(req, HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() != 200 && resp.statusCode() != 404) {
            throw new RuntimeException("GET failed: " + resp.statusCode());
        }
    }

    private static void summarizeAndPrint(List<Sample> all, long totalOps, int durationSeconds) {
        if (all.isEmpty()) {
            System.err.println("no samples collected");
            return;
        }

        double throughput = totalOps / (double) durationSeconds;

        List<Double> latencies = new ArrayList<>(all.size());
        for (Sample s : all) {
            if (s.ok) {
                latencies.add(s.latencyMs);
            }
        }
        Collections.sort(latencies);

        double p50 = percentile(latencies, 0.50);
        double p95 = percentile(latencies, 0.95);
        double p99 = percentile(latencies, 0.99);

        long okCount = all.stream().filter(s -> s.ok).count();
        long errCount = all.size() - okCount;

        System.err.printf(
                "throughput=%.2f ops/s, ok=%d, err=%d, p50=%.2fms, p95=%.2fms, p99=%.2fms%n",
                throughput, okCount, errCount, p50, p95, p99
        );

        // CSV to stdout.
        System.out.println("op,success,latency_ms");
        for (Sample s : all) {
            System.out.printf("%s,%s,%.3f%n", s.op, s.ok ? "1" : "0", s.latencyMs);
        }
    }

    private static double percentile(List<Double> sorted, double q) {
        if (sorted.isEmpty()) return Double.NaN;
        double idx = q * (sorted.size() - 1);
        int lo = (int) Math.floor(idx);
        int hi = (int) Math.ceil(idx);
        if (lo == hi) return sorted.get(lo);
        double w = idx - lo;
        return sorted.get(lo) * (1 - w) + sorted.get(hi) * w;
    }
}
