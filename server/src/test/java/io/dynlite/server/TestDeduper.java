package io.dynlite.server;

import io.dynlite.storage.OpIdDeduper;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/** Test-only deduper: remembers opIds forever (good enough for unit tests). */
final class TestDeduper implements OpIdDeduper {
    private final Set<String> seen = ConcurrentHashMap.newKeySet();
    @Override public boolean firstTime(String opId) { return seen.add(opId); }
    @Override public void setTtl(Duration ttl) { /* no-op for tests */ }
}
