// file: src/main/java/io/dynlite/storage/FileSnapshotter.java
package io.dynlite.storage;

import io.dynlite.core.VectorClock;
import io.dynlite.core.VersionedValue;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;

/**
 * Binary snapshot implementation backed by a single file per snapshot.
 * <p>
 * Format:
 *   int32 count
 *   repeated 'count' times:
 *     - key:        int32 len + UTF-8 bytes
 *     - tombstone:  boolean
 *     - lwwMillis:  int64
 *     - value:      int32 len + bytes (len == -1 => null)
 *     - vcCount:    int32
 *         repeated vcCount times:
 *           - nodeId: int32 len + UTF-8 bytes
 *           - counter: int32
 * <p>
 * Atomicity:
 *   - We write to "snapshot-<ts>.bin.tmp" first,
 *   - then move to "snapshot-<ts>.bin" using ATOMIC_MOVE.
 */
public final class FileSnapshotter implements Snapshotter {
    private final Path dir;

    public FileSnapshotter(Path dir) {
        this.dir = dir;
        try { Files.createDirectories(dir); } catch (IOException e) { throw new RuntimeException(e); }
    }

    @Override
    public String writeSnapshot(Map<String, List<VersionedValue>> current) {
        String name = "snapshot-" + System.currentTimeMillis() + ".bin";
        Path tmp = dir.resolve(name + ".tmp");
        Path dst = dir.resolve(name);

        try (var out = new DataOutputStream(Files.newOutputStream(tmp,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING))) {
            out.writeInt(current.size());

            for (Map.Entry<String, List<VersionedValue>> e : current.entrySet()) {
                String key = e.getKey();
                List<VersionedValue> siblings = e.getValue();

                writeString(out, key);
                out.writeInt(siblings.size());

                for (VersionedValue v : siblings) {
                    out.writeBoolean(v.tombstone());
                    out.writeLong(v.lwwMillis());
                    writeBytes(out, v.value());

                    Map<String, Integer> vc = v.clock().entries();
                    out.writeInt(vc.size());
                    for (Map.Entry<String, Integer> ce : vc.entrySet()) {
                        writeString(out, ce.getKey());
                        out.writeInt(ce.getValue());
                    }
                }
            }
        } catch (IOException ex) { throw new RuntimeException(ex); }

        try { Files.move(tmp, dst, ATOMIC_MOVE); }
        catch (IOException e) { throw new RuntimeException(e); }

        return dst.getFileName().toString();
    }

    @Override
    public LoadedSnapshot loadLatest() {
        try {
            Path snap = Files.list(dir)
                    .filter(p -> p.getFileName().toString().startsWith("snapshot-"))
                    .sorted()
                    .reduce((a,b)->b)
                    .orElse(null);

            if (snap == null) return null;

            try (var in = new DataInputStream(Files.newInputStream(snap))) {
                int keyCount = in.readInt();
                Map<String, List<VersionedValue>> map = new HashMap<>(keyCount * 2);

                for (int i = 0; i < keyCount; i++) {
                    String key = readString(in);
                    int siblingCount = in.readInt();

                    List<VersionedValue> siblings = new ArrayList<>(siblingCount);
                    for (int s = 0; s < siblingCount; s++) {
                        boolean tomb = in.readBoolean();
                        long lww = in.readLong();
                        byte[] val = readBytes(in);

                        int m = in.readInt();
                        Map<String, Integer> vc = new HashMap<>(m * 2);
                        for (int j = 0; j < m; j++) {
                            String nodeId = readString(in);
                            int counter = in.readInt();
                            vc.put(nodeId, counter);
                        }
                        siblings.add(new VersionedValue(val, tomb, new VectorClock(vc), lww));
                    }
                    map.put(key, siblings);
                }
                return new LoadedSnapshot(snap.getFileName().toString(), map);
            }
        } catch (IOException e) { throw new RuntimeException(e); }
    }

    private static void writeString(DataOutputStream out, String s) throws IOException {
        byte[] b = s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        out.writeInt(b.length);
        out.write(b);
    }
    private static String readString(DataInputStream in) throws IOException {
        int len = in.readInt();
        byte[] b = in.readNBytes(len);
        return new String(b, java.nio.charset.StandardCharsets.UTF_8);
    }
    private static void writeBytes(DataOutputStream out, byte[] v) throws IOException {
        if (v == null) {
            out.writeInt(-1);
            return;
        }
        out.writeInt(v.length); out.write(v);
    }
    private static byte[] readBytes(DataInputStream in) throws IOException {
        int len = in.readInt();
        if (len == -1) return null;
        return in.readNBytes(len);
    }
}
