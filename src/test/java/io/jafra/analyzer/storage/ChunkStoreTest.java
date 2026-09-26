package io.jafra.analyzer.storage;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ChunkStoreTest {
    @Test
    void commitIsDurableAcrossRestartWithoutEagerStitch(@TempDir Path root) throws Exception {
        ChunkStore store = new ChunkStore(root);
        store.recover();
        ChunkMetadata first = metadata("chunk-a", 0, new byte[] {1, 2, 3, 4});
        persist(store, first, new byte[] {1, 2, 3, 4});

        assertTrue(Files.exists(store.payloadPath("chunk-a")));
        assertFalse(Files.exists(root.resolve("stitch-cache")));
        assertTrue(Files.walk(root.resolve("recordings"))
                .noneMatch(path -> path.getFileName().toString().equals("stitched.jfr")));

        ChunkStore restarted = new ChunkStore(root);
        restarted.recover();
        assertTrue(restarted.contains("chunk-a"));
        assertEquals(List.of("chunk-a"), restarted.contiguousChunks(first.recordingId()).stream()
                .map(ChunkMetadata::chunkId)
                .toList());
        assertArrayEquals(
                new byte[] {1, 2, 3, 4},
                readInOrder(restarted.contiguousPayloads(List.of(first.recordingId()))));
    }

    @Test
    void payloadWithoutMetaIsDiscardedOnRecover(@TempDir Path root) throws Exception {
        ChunkStore store = new ChunkStore(root);
        store.recover();
        ChunkStore.IncomingWrite write = store.beginWrite("orphan");
        write.write(new byte[] {9, 9});
        write.force();
        write.closeQuietly();
        Files.createDirectories(root.resolve("chunks"));
        Files.move(root.resolve("tmp/orphan.part"), root.resolve("chunks/orphan.jfr"));

        ChunkStore restarted = new ChunkStore(root);
        restarted.recover();
        assertFalse(restarted.contains("orphan"));
        assertFalse(Files.exists(root.resolve("chunks/orphan.jfr")));
        assertFalse(Files.exists(root.resolve("tmp/orphan.part")));
    }

    @Test
    void outOfOrderChunksWaitForTheHole(@TempDir Path root) throws Exception {
        ChunkStore store = new ChunkStore(root);
        store.recover();
        byte[] later = new byte[] {5, 6};
        persist(store, metadata("chunk-b", 4, later), later);
        assertTrue(store.contiguousChunks("local-demo/pod/auth-cache/profile-0.jfr").isEmpty());
        assertThrows(
                IOException.class,
                () -> store.contiguousPayloads(List.of("local-demo/pod/auth-cache/profile-0.jfr")));

        byte[] early = new byte[] {1, 2, 3, 4};
        persist(store, metadata("chunk-a", 0, early), early);
        assertEquals(2, store.contiguousChunks("local-demo/pod/auth-cache/profile-0.jfr").size());
        List<Path> paths = store.contiguousPayloads(List.of("local-demo/pod/auth-cache/profile-0.jfr"));
        assertEquals(List.of("chunk-a", "chunk-b"), chunkIds(paths));
        assertArrayEquals(new byte[] {1, 2, 3, 4, 5, 6}, readInOrder(paths));
    }

    @Test
    void contiguousPayloadsFollowRecordingThenOffsetOrder(@TempDir Path root) throws Exception {
        ChunkStore store = new ChunkStore(root);
        store.recover();
        persist(store, metadata("b0", "rec-b", 0, new byte[] {9}), new byte[] {9});
        persist(store, metadata("a1", "rec-a", 2, new byte[] {3, 4}), new byte[] {3, 4});
        persist(store, metadata("a0", "rec-a", 0, new byte[] {1, 2}), new byte[] {1, 2});

        List<Path> paths = store.contiguousPayloads(List.of("rec-a", "rec-b"));
        assertEquals(List.of("a0", "a1", "b0"), chunkIds(paths));
        assertArrayEquals(new byte[] {1, 2, 3, 4, 9}, readInOrder(paths));
        assertArrayEquals(
                new byte[] {9, 1, 2, 3, 4},
                readInOrder(store.contiguousPayloads(List.of("rec-b", "rec-a"))));
    }

    @Test
    void recoverDeletesLeftoverStitchCache(@TempDir Path root) throws Exception {
        Path cache = root.resolve("stitch-cache");
        Files.createDirectories(cache);
        Files.write(cache.resolve("abc.jfr"), new byte[] {1, 2, 3});

        ChunkStore store = new ChunkStore(root);
        store.recover();
        assertFalse(Files.exists(cache));
    }

    @Test
    void recoverDeletesLegacyDurableStitchedFiles(@TempDir Path root) throws Exception {
        ChunkStore store = new ChunkStore(root);
        store.recover();
        persist(store, metadata("chunk-a", 0, new byte[] {1, 2}), new byte[] {1, 2});

        Path legacyDir = root.resolve("recordings/local-demo/pod/auth-cache/profile-0.jfr");
        Files.createDirectories(legacyDir);
        Files.write(legacyDir.resolve("stitched.jfr"), new byte[] {9, 9, 9});
        Files.writeString(legacyDir.resolve("manifest.json"), "{\"nextOffset\":2}");

        ChunkStore restarted = new ChunkStore(root);
        restarted.recover();
        assertFalse(Files.exists(legacyDir.resolve("stitched.jfr")));
        assertFalse(Files.exists(legacyDir.resolve("manifest.json")));
        assertTrue(restarted.contains("chunk-a"));
    }

    @Test
    void recoverMovesLegacyChunkAndDoesNotMoveItAgain(@TempDir Path root) throws Exception {
        Path chunks = root.resolve("chunks");
        Files.createDirectories(chunks);
        byte[] payload = new byte[68];
        ByteBuffer header = ByteBuffer.wrap(payload).order(ByteOrder.BIG_ENDIAN);
        header.put(new byte[] {'F', 'L', 'R', 0});
        header.putInt(2);
        header.putLong(68);
        header.putLong(68);
        header.putLong(68);
        header.putLong(1_750_000_000_000_000_000L);
        header.putLong(5_000_000_000L);
        Files.write(chunks.resolve("chunk-a.jfr"), payload);
        Files.writeString(chunks.resolve("chunk-a.meta"), """
                {"chunkId":"chunk-a","recordingId":"local-demo/pod/auth-cache/profile-0.jfr","clusterId":"local-demo","namespace":"default","podUid":"pod","podName":"auth-cache","containerName":"auth-cache","physicalFilename":"profile-0.jfr","chunkOffset":0,"chunkLength":68,"checksum":"checksum","chunkStartTimeNs":0,"chunkDurationNs":0}
                """);

        ChunkStore store = new ChunkStore(root);
        store.recover();
        assertTrue(store.contains("chunk-a"));
        assertFalse(Files.exists(chunks.resolve("chunk-a.jfr")));
        assertFalse(Files.exists(chunks.resolve("chunk-a.meta")));
        ChunkStore.StoredChunk stored = store.storedChunks().getFirst();
        assertEquals("chunk-a", stored.parsed().chunkId());
        assertEquals(1_750_000_000L, stored.parsed().startSeconds());
        Path firstPayload = stored.payload();

        ChunkStore restarted = new ChunkStore(root);
        restarted.recover();
        assertEquals(firstPayload, restarted.storedChunks().getFirst().payload());
        assertTrue(restarted.contains("chunk-a"));
    }

    private static List<String> chunkIds(List<Path> paths) {
        return paths.stream()
                .map(path -> ChunkFileName.parse(path.getFileName().toString()).chunkId())
                .toList();
    }

    private static byte[] readInOrder(List<Path> paths) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (Path path : paths) {
            out.write(Files.readAllBytes(path));
        }
        return out.toByteArray();
    }

    private static void persist(ChunkStore store, ChunkMetadata metadata, byte[] payload) throws Exception {
        ChunkStore.IncomingWrite write = store.beginWrite(metadata.chunkId());
        write.write(payload);
        store.commit(write, metadata);
    }

    private static ChunkMetadata metadata(String chunkId, long offset, byte[] payload) {
        return metadata(chunkId, "local-demo/pod/auth-cache/profile-0.jfr", offset, payload);
    }

    private static ChunkMetadata metadata(String chunkId, String recordingId, long offset, byte[] payload) {
        return new ChunkMetadata(
                chunkId,
                recordingId,
                "local-demo",
                "default",
                "pod",
                "auth-cache",
                "auth-cache",
                "profile-0.jfr",
                offset,
                payload.length,
                "checksum");
    }
}
