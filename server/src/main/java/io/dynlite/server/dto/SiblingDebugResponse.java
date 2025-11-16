// file: src/main/java/io/dynlite/server/dto/SiblingDebugResponse.java
package io.dynlite.server.dto;

import java.util.List;
import java.util.Map;

/**
 * JSON response for GET /debug/siblings/{key}.
 * Example shape:
 * {
 *   "key": "user:42",
 *   "siblings": [
 *     {
 *       "tombstone": false,
 *       "lwwMillis": 1728000000000,
 *       "valueBase64": "dmFsdWU=",
 *       "vectorClock": { "A": 2, "B": 1 }
 *     },
 *     ...
 *   ]
 * }
 */
public class SiblingDebugResponse {

    public String key;
    public List<SiblingRecord> siblings;

    public static class SiblingRecord {
        public boolean tombstone;
        public long lwwMillis;
        public String valueBase64;
        public Map<String, Integer> vectorClock;
    }
}
