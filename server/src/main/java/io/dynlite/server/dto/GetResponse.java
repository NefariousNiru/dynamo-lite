package io.dynlite.server.dto;

import java.util.Map;

/**
 * JSON response for GET /kv/{key}.
 */
public class GetResponse {
    public boolean found;
    public String valueBase64;      // present when found && !tombstone
    public Map<String, Integer> vectorClock;
}
