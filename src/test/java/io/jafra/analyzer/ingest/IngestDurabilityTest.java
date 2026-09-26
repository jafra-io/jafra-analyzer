package io.jafra.analyzer.ingest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.google.protobuf.ByteString;

import io.jafra.analyzer.storage.ChunkStore;
import io.jafra.ingest.v1.AckStatus;
import io.jafra.ingest.v1.ChunkFrame;
import io.jafra.ingest.v1.CommitChunk;
import io.jafra.ingest.v1.OpenChunk;
import io.jafra.ingest.v1.UploadRequest;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

class IngestDurabilityTest {
    @Test
    void acceptedChunkIsDuplicateAfterRestart(@TempDir Path root) throws Exception {
        byte[] payload = "FLR\0chunk-one".getBytes(StandardCharsets.UTF_8);
        ChunkStore store = new ChunkStore(root);
        store.recover();
        IngestRegistry registry = new IngestRegistry(new SimpleMeterRegistry(), store, 8, 4096);
        var accepted = ingest(registry, payload);
        assertEquals(AckStatus.ACCEPTED, accepted.getStatus());
        assertTrue(store.contains(IngestSession.chunkId(open(payload.length))));
        assertEquals(0, registry.status().stitchedBytes());
        assertTrue(Files.walk(root.resolve("recordings"))
                .noneMatch(path -> path.getFileName().toString().equals("stitched.jfr")));

        ChunkStore restarted = new ChunkStore(root);
        restarted.recover();
        IngestRegistry afterRestart = new IngestRegistry(new SimpleMeterRegistry(), restarted, 8, 4096);
        IngestRegistry.StreamContext replay = afterRestart.openStream();
        var ack = afterRestart.handle(replay, UploadRequest.newBuilder().setOpen(open(payload.length)).build());
        afterRestart.closeStream(replay);
        assertEquals(AckStatus.DUPLICATE, ack.getStatus());
    }

    private static io.jafra.ingest.v1.UploadAck ingest(IngestRegistry registry, byte[] payload) throws Exception {
        IngestRegistry.StreamContext context = registry.openStream();
        OpenChunk open = open(payload.length);
        var openAck = registry.handle(context, UploadRequest.newBuilder().setOpen(open).build());
        assertTrue(openAck == null);
        assertTrue(registry.handle(context, UploadRequest.newBuilder()
                .setFrame(ChunkFrame.newBuilder()
                        .setRecordingId(open.getRecordingId())
                        .setChunkId(IngestSession.chunkId(open))
                        .setFrameOffset(0)
                        .setPayload(ByteString.copyFrom(payload))
                        .build())
                .build()) == null);
        var ack = registry.handle(context, UploadRequest.newBuilder()
                .setCommit(CommitChunk.newBuilder()
                        .setRecordingId(open.getRecordingId())
                        .setChunkId(IngestSession.chunkId(open))
                        .setTotalBytes(payload.length)
                        .setChecksumAlgorithm("sha256")
                        .setChecksum(sha256(payload))
                        .build())
                .build());
        registry.closeStream(context);
        return ack;
    }

    private static OpenChunk open(int length) {
        return OpenChunk.newBuilder()
                .setProtocolVersion(1)
                .setClusterId("local-demo")
                .setNodeName("worker-1")
                .setNamespace("default")
                .setPodUid("pod")
                .setPodName("auth-cache")
                .setContainerName("auth-cache")
                .setPhysicalFilename("profile-0.jfr")
                .setRecordingId("local-demo/pod/auth-cache/profile-0.jfr")
                .setChunkOffset(0)
                .setChunkLength(length)
                .build();
    }

    private static String sha256(byte[] payload) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(payload));
    }
}
