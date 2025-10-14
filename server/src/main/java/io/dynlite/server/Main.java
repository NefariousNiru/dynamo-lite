package io.dynlite.server;

import io.dynlite.storage.*;

import java.nio.file.Path;

/** Boots DurableStore and the HTTP server. */
public final class Main {
    public static void main(String[] args) {
        var cfg = ServerConfig.fromArgs(args);
        var wal = new FileWal(Path.of(cfg.walDir()), 64L * 1024 * 1024); // rotate ~64MB
        var snaps = new FileSnapshotter(Path.of(cfg.snapDir()));
        var dedupe = new SimpleOpIdDeduper(); // TODO: use your real deduper; for now a small in-mem impl
        var store = new DurableStore(wal, snaps, dedupe);

        var svc = new KvService(store, cfg.nodeId());
        var web = new WebServer(cfg.httpPort(), svc);
        System.out.printf("Server %s listening on http://localhost:%d%n", cfg.nodeId(), cfg.httpPort());
        web.start();
    }

    /** Minimal in-proc deduper (replace with a bounded TTL cache later). */
    static final class SimpleOpIdDeduper implements OpIdDeduper {
        private final java.util.Set<String> seen = java.util.concurrent.ConcurrentHashMap.newKeySet();
        @Override public boolean firstTime(String opId) { return seen.add(opId); }
        @Override public void setTtl(java.time.Duration ttl) { /* no-op */ }
    }
}
