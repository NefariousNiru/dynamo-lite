package io.dynlite.storage;

import java.time.Duration;

/**
 * Drops duplicate operations by remembering recently seen opIds.
 * Implementation can be a bounded Caffeine cache or a ring buffer for MVP.
 */
public interface OpIdDeduper {
    /** Returns true if this opId was not seen before and is now recorded. */
    boolean firstTime(String opId);

    /** Configure retention window for remembering ids. */
    void setTtl(Duration ttl);
}
