// file: src/test/java/io/dynlite/storage/TtlOpIdDeduperSpec.java
package io.dynlite.storage;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Basic behavior checks for TtlOpIdDeduper.
 */
class TtlOpIdDeduperSpec {

    @Test
    void same_opid_within_ttl_is_not_first_time() {
        var d = new TtlOpIdDeduper(Duration.ofSeconds(5));

        String opId = "op-123";

        assertTrue(d.firstTime(opId), "first call should be firstTime");
        assertFalse(d.firstTime(opId), "second call within TTL should NOT be firstTime");
    }

    @Test
    void opid_becomes_first_time_again_after_ttl_expires() throws Exception {
        var d = new TtlOpIdDeduper(Duration.ofMillis(50));

        String opId = "op-xyz";

        assertTrue(d.firstTime(opId), "initial call should be firstTime");
        assertFalse(d.firstTime(opId), "immediate repeat should NOT be firstTime");

        // Wait for TTL to expire
        Thread.sleep(60);

        assertTrue(d.firstTime(opId), "after TTL, opId should be treated as new again");
    }

    @Test
    void set_ttl_rejects_non_positive_values() {
        var d = new TtlOpIdDeduper(Duration.ofMillis(10));

        assertThrows(IllegalArgumentException.class,
                () -> d.setTtl(Duration.ZERO));
        assertThrows(IllegalArgumentException.class,
                () -> d.setTtl(Duration.ofMillis(-1)));
    }
}
