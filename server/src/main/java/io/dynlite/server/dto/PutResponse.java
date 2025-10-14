package io.dynlite.server.dto;


import java.util.Map;

/** JSON response for PUT/DELETE. */
public class PutResponse {
    public boolean ok;
    public boolean tombstone;
    public long lwwMillis;
    public Map<String,Integer> vectorClock;
}
