package io.jafra.analyzer.ingest;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import io.jafra.analyzer.storage.ChunkMetadata;
import io.jafra.analyzer.storage.ChunkStore;
import io.jafra.ingest.v1.AckStatus;
import io.jafra.ingest.v1.OpenChunk;
import io.jafra.ingest.v1.UploadAck;
import io.jafra.ingest.v1.UploadRequest;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

@ApplicationScoped
public class IngestRegistry {
    private static final Logger LOG = Logger.getLogger(IngestRegistry.class);

    private final Map<String, IngestSession> active = new ConcurrentHashMap<>();
    private final AtomicInteger activeStreams = new AtomicInteger();
    private final AtomicLong receivedChunks = new AtomicLong();
    private final AtomicLong rejectedChunks = new AtomicLong();
    private final AtomicLong duplicateChunks = new AtomicLong();
    private final AtomicLong receivedBytes = new AtomicLong();
    private final int maxActiveStreams;
    private final int maxMetadataLength;
    private final ChunkStore store;
    private final Counter accepted;
    private final Counter rejected;
    private final Counter duplicates;
    private final Counter checksumFailures;
    private final Counter protocolFailures;

    public IngestRegistry(
            MeterRegistry meterRegistry,
            ChunkStore store,
            @ConfigProperty(name = "jafra.ingest.max-active-streams", defaultValue = "32") int maxActiveStreams,
            @ConfigProperty(name = "jafra.ingest.max-metadata-length", defaultValue = "4096") int maxMetadataLength) {
        this.store = store;
        this.maxActiveStreams = maxActiveStreams;
        this.maxMetadataLength = maxMetadataLength;
        this.accepted = meterRegistry.counter("jafra_analyzer_accepted_chunks_total");
        this.rejected = meterRegistry.counter("jafra_analyzer_rejected_chunks_total");
        this.duplicates = meterRegistry.counter("jafra_analyzer_duplicate_chunks_total");
        this.checksumFailures = meterRegistry.counter("jafra_analyzer_checksum_failures_total");
        this.protocolFailures = meterRegistry.counter("jafra_analyzer_protocol_failures_total");
    }

    public StreamContext openStream() {
        int current = activeStreams.incrementAndGet();
        if (current > maxActiveStreams) {
            activeStreams.decrementAndGet();
            throw new IllegalStateException("too many concurrent ingest streams");
        }
        return new StreamContext(new IngestSession(maxMetadataLength));
    }

    public StatusSnapshot status() {
        return new StatusSnapshot(
                "jafra-analyzer",
                "ready",
                activeStreams.get(),
                receivedChunks.get(),
                rejectedChunks.get(),
                store.durableChunkCount(),
                0);
    }

    void closeStream(StreamContext context) {
        if (context != null && !context.committed()) {
            context.abort(store);
            if (context.chunkId != null) {
                active.remove(context.chunkId);
            }
        }
        activeStreams.decrementAndGet();
    }

    UploadAck handle(StreamContext context, UploadRequest request) {
        if (request.getPayloadCase() == UploadRequest.PayloadCase.PAYLOAD_NOT_SET) {
            return reject(context, "", "", "empty payload");
        }
        return switch (request.getPayloadCase()) {
            case OPEN -> handleOpen(context, request.getOpen());
            case FRAME -> handleFrame(context, request.getFrame());
            case COMMIT -> handleCommit(context, request.getCommit());
            case PAYLOAD_NOT_SET -> reject(context, "", "", "payload not set");
        };
    }

    private UploadAck handleOpen(StreamContext context, OpenChunk open) {
        String chunkId = IngestSession.chunkId(open);
        if (store.contains(chunkId)) {
            duplicateChunks.incrementAndGet();
            duplicates.increment();
            return ack(open.getRecordingId(), chunkId, AckStatus.DUPLICATE, "duplicate chunk ID", 0);
        }
        if (!store.hasRoom(open.getChunkLength())) {
            return ack(open.getRecordingId(), chunkId, AckStatus.RETRY, "insufficient durable storage", 0);
        }
        if (active.putIfAbsent(chunkId, context.session) != null) {
            duplicateChunks.incrementAndGet();
            duplicates.increment();
            return ack(open.getRecordingId(), chunkId, AckStatus.REJECTED, "duplicate simultaneous chunk ID", 0);
        }
        IngestSession.OpenResult result = context.session.open(open);
        if (!result.accepted()) {
            active.remove(chunkId);
            protocolFailures.increment();
            return reject(context, open.getRecordingId(), chunkId, result.reason());
        }
        try {
            context.incoming = store.beginWrite(chunkId);
            context.session.attachSink(payload -> context.incoming.write(payload));
        } catch (IOException error) {
            active.remove(chunkId);
            return ack(open.getRecordingId(), chunkId, AckStatus.RETRY, "unable to open durable write: " + error.getMessage(), 0);
        }
        context.chunkId = chunkId;
        LOG.infof(
                "{\"event\":\"jfr_chunk_opened\",\"recording_id\":\"%s\",\"chunk_id\":\"%s\",\"node\":\"%s\",\"namespace\":\"%s\",\"pod_uid\":\"%s\",\"container\":\"%s\",\"filename\":\"%s\",\"chunk_offset\":%d,\"chunk_size\":%d}",
                open.getRecordingId(),
                chunkId,
                open.getNodeName(),
                open.getNamespace(),
                open.getPodUid(),
                open.getContainerName(),
                open.getPhysicalFilename(),
                open.getChunkOffset(),
                open.getChunkLength());
        return null;
    }

    private UploadAck handleFrame(StreamContext context, io.jafra.ingest.v1.ChunkFrame frame) {
        IngestSession.FrameResult result = context.session.frame(frame);
        if (result.retry()) {
            context.abort(store);
            return ack(frame.getRecordingId(), frame.getChunkId(), AckStatus.RETRY, result.reason(), context.session.receivedBytes());
        }
        if (!result.accepted()) {
            protocolFailures.increment();
            return reject(context, frame.getRecordingId(), frame.getChunkId(), result.reason());
        }
        receivedBytes.addAndGet(frame.getPayload().size());
        return null;
    }

    private UploadAck handleCommit(StreamContext context, io.jafra.ingest.v1.CommitChunk commit) {
        UploadAck ack = context.session.commit(commit);
        if (ack.getStatus() == AckStatus.ACCEPTED) {
            OpenChunk open = context.session.openChunk();
            ChunkMetadata metadata = new ChunkMetadata(
                    commit.getChunkId(),
                    open.getRecordingId(),
                    open.getClusterId(),
                    open.getNamespace(),
                    open.getPodUid(),
                    open.getPodName(),
                    open.getContainerName(),
                    open.getPhysicalFilename(),
                    open.getChunkOffset(),
                    open.getChunkLength(),
                    commit.getChecksum(),
                    open.getChunkStartTimeNs(),
                    open.getChunkDurationNs());
            try {
                store.commit(context.incoming, metadata);
                context.incoming = null;
            } catch (IOException error) {
                context.abort(store);
                active.remove(commit.getChunkId());
                return ack(commit.getRecordingId(), commit.getChunkId(), AckStatus.RETRY,
                        "durable persist failed: " + error.getMessage(), context.session.receivedBytes());
            }
            receivedChunks.incrementAndGet();
            accepted.increment();
            active.remove(commit.getChunkId());
            LOG.infof(
                    "{\"event\":\"jfr_chunk_received\",\"recording_id\":\"%s\",\"chunk_id\":\"%s\",\"node\":\"%s\",\"namespace\":\"%s\",\"pod_uid\":\"%s\",\"container\":\"%s\",\"filename\":\"%s\",\"chunk_offset\":%d,\"chunk_size\":%d,\"frames_received\":%d,\"checksum_valid\":true,\"durable\":true}",
                    commit.getRecordingId(),
                    commit.getChunkId(),
                    open.getNodeName(),
                    open.getNamespace(),
                    open.getPodUid(),
                    open.getContainerName(),
                    open.getPhysicalFilename(),
                    open.getChunkOffset(),
                    open.getChunkLength(),
                    context.session.framesEstimate());
            return ack;
        }
        if (ack.getStatus() == AckStatus.DUPLICATE) {
            duplicateChunks.incrementAndGet();
            duplicates.increment();
            context.abort(store);
            return ack;
        }
        if (ack.getMessage().contains("checksum")) {
            checksumFailures.increment();
        }
        return reject(context, commit.getRecordingId(), commit.getChunkId(), ack.getMessage());
    }

    private UploadAck reject(StreamContext context, String recordingId, String chunkId, String reason) {
        rejectedChunks.incrementAndGet();
        rejected.increment();
        context.abort(store);
        if (chunkId != null && !chunkId.isBlank()) {
            active.remove(chunkId);
        }
        LOG.warnf("rejecting ingest stream: %s", reason);
        return ack(recordingId, chunkId == null ? "" : chunkId, AckStatus.REJECTED, reason, context.session.receivedBytes());
    }

    private static UploadAck ack(String recordingId, String chunkId, AckStatus status, String message, long bytes) {
        return UploadAck.newBuilder()
                .setRecordingId(recordingId == null ? "" : recordingId)
                .setChunkId(chunkId == null ? "" : chunkId)
                .setStatus(status)
                .setMessage(message)
                .setReceivedBytes(bytes)
                .build();
    }

    public record StatusSnapshot(
            String service,
            String status,
            int activeStreams,
            long receivedChunks,
            long rejectedChunks,
            long durableChunks,
            long stitchedBytes) {
    }

    public static final class StreamContext {
        private final IngestSession session;
        private String chunkId;
        private ChunkStore.IncomingWrite incoming;

        StreamContext(IngestSession session) {
            this.session = session;
        }

        public IngestSession session() {
            return session;
        }

        public boolean committed() {
            return session.isCommitted();
        }

        void abort(ChunkStore store) {
            store.abort(incoming);
            incoming = null;
        }
    }
}
