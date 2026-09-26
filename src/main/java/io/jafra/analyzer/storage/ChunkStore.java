package io.jafra.analyzer.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import org.jboss.logging.Logger;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ChunkStore {
    private static final Logger LOG = Logger.getLogger(ChunkStore.class);
    private static final long HEADROOM_BYTES = 1024 * 1024;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Path root;
    private final Path tmpDir;
    private final Path chunksDir;
    private final Path dataDir;
    private final Path metaDir;
    private final Path recordingsDir;
    private final Path identitiesDir;
    private final Map<String, ChunkMetadata> committed = new ConcurrentHashMap<>();
    private final Map<String, TreeMap<Long, ChunkMetadata>> byRecording = new ConcurrentHashMap<>();
    private final Map<String, String> podNames = new ConcurrentHashMap<>();
    private final Map<String, Path> payloadPaths = new ConcurrentHashMap<>();
    private final Map<String, Path> metaPaths = new ConcurrentHashMap<>();
    private final Set<String> chunkIds = ConcurrentHashMap.newKeySet();

    public ChunkStore(Path root) {
        this.root = root;
        this.tmpDir = root.resolve("tmp");
        this.chunksDir = root.resolve("chunks");
        this.dataDir = root.resolve("data");
        this.metaDir = root.resolve("meta");
        this.recordingsDir = root.resolve("recordings");
        this.identitiesDir = root.resolve("identities");
    }

    public Path root() {
        return root;
    }

    public void recover() throws IOException {
        Files.createDirectories(tmpDir);
        Files.createDirectories(chunksDir);
        Files.createDirectories(dataDir);
        Files.createDirectories(metaDir);
        Files.createDirectories(recordingsDir);
        Files.createDirectories(identitiesDir);
        deleteTmpParts();
        committed.clear();
        byRecording.clear();
        podNames.clear();
        payloadPaths.clear();
        metaPaths.clear();
        chunkIds.clear();
        loadIdentities();
        migrateLegacyChunks();
        deleteOrphanPayloads();
        deleteLegacyStitchedRecordings();
        deleteStitchCache();
        indexStoredNames();
        LOG.infof("recovered %d durable chunks under %s", chunkIds.size(), root);
    }

    public boolean contains(String chunkId) {
        return chunkIds.contains(chunkId);
    }

    public Set<String> committedIds() {
        return Set.copyOf(committed.keySet());
    }

    public int durableChunkCount() {
        return chunkIds.size();
    }

    /** Bytes of durable chunk payloads. */
    public long chunksBytes() {
        long total = 0;
        for (Path payload : payloadPaths.values()) {
            try {
                total += Files.size(payload);
            } catch (IOException ignored) {
                // A missing file is not counted.
            }
        }
        return total;
    }

    /**
     * @deprecated Prefer {@link #chunksBytes()}. Status JSON still has {@code stitchedBytes} and always reports 0.
     */
    @Deprecated
    public long stitchedBytes() {
        return 0;
    }

    public boolean hasRoom(long chunkLength) {
        try {
            return Files.getFileStore(root).getUsableSpace() > chunkLength + HEADROOM_BYTES;
        } catch (IOException error) {
            return true;
        }
    }

    public IncomingWrite beginWrite(String chunkId) throws IOException {
        Path part = tmpDir.resolve(chunkId + ".part");
        Files.deleteIfExists(part);
        FileChannel channel = FileChannel.open(
                part,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING);
        return new IncomingWrite(chunkId, part, channel);
    }

    public void commit(IncomingWrite incoming, ChunkMetadata metadata) throws IOException {
        incoming.force();
        incoming.closeQuietly();
        Path payload = dataFile(metadata);
        Path meta = metaFile(metadata);
        Files.createDirectories(payload.getParent());
        Files.createDirectories(meta.getParent());
        Files.move(incoming.part, payload, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        writeAtomic(meta, MAPPER.writeValueAsBytes(metadata));
        chunkIds.add(metadata.chunkId());
        payloadPaths.put(metadata.chunkId(), payload);
        metaPaths.put(metadata.chunkId(), meta);
        committed.put(metadata.chunkId(), metadata);
        byRecording.computeIfAbsent(metadata.recordingId(), ignored -> new TreeMap<>())
                .put(metadata.chunkOffset(), metadata);
        rememberPodName(metadata.namespace(), metadata.podUid(), metadata.podName());
    }

    public void abort(IncomingWrite incoming) {
        if (incoming != null) {
            incoming.abort();
        }
    }

    public Path payloadPath(String chunkId) {
        Path known = payloadPaths.get(chunkId);
        if (known != null) {
            return known;
        }
        return chunksDir.resolve(chunkId + ".jfr");
    }

    /** Names already on disk. Parsing the name does not open the meta file. */
    public List<StoredChunk> storedChunks() {
        List<StoredChunk> stored = new ArrayList<>();
        if (!Files.isDirectory(metaDir)) {
            return stored;
        }
        try (Stream<Path> buckets = Files.list(metaDir)) {
            for (Path bucket : buckets.filter(Files::isDirectory).toList()) {
                try (Stream<Path> files = Files.list(bucket)) {
                    for (Path meta : files.toList()) {
                        ChunkFileName.Parsed parsed = ChunkFileName.parse(meta.getFileName().toString());
                        if (parsed == null) {
                            continue;
                        }
                        Path payload = dataDir.resolve(bucket.getFileName()).resolve(meta.getFileName());
                        if (!Files.isRegularFile(payload)) {
                            continue;
                        }
                        stored.add(new StoredChunk(parsed, payload, meta));
                    }
                }
            }
        } catch (IOException error) {
            LOG.warnf(error, "unable to list stored chunks under %s", metaDir);
        }
        return stored;
    }

    public ChunkMetadata readMeta(StoredChunk stored) {
        if (stored == null) {
            return null;
        }
        ChunkMetadata cached = committed.get(stored.parsed().chunkId());
        if (cached != null) {
            return cached;
        }
        try {
            return MAPPER.readValue(stored.meta().toFile(), ChunkMetadata.class);
        } catch (IOException error) {
            LOG.warnf(error, "skipping unreadable chunk metadata %s", stored.meta());
            return null;
        }
    }

    /** Remember meta that a query already selected, so analysis can order that recording. */
    public void absorb(ChunkMetadata metadata) {
        if (metadata == null || metadata.chunkId() == null || metadata.recordingId() == null) {
            return;
        }
        committed.put(metadata.chunkId(), metadata);
        byRecording.computeIfAbsent(metadata.recordingId(), ignored -> new TreeMap<>())
                .put(metadata.chunkOffset(), metadata);
    }

    public Set<String> recordingIds() {
        return Set.copyOf(byRecording.keySet());
    }

    public ChunkMetadata sample(String recordingId) {
        TreeMap<Long, ChunkMetadata> chunks = byRecording.get(recordingId);
        if (chunks == null || chunks.isEmpty()) {
            return null;
        }
        return chunks.firstEntry().getValue();
    }

    public Collection<ChunkMetadata> chunks(String recordingId) {
        TreeMap<Long, ChunkMetadata> chunks = byRecording.get(recordingId);
        if (chunks == null || chunks.isEmpty()) {
            return List.of();
        }
        return List.copyOf(chunks.values());
    }

    /** Contiguous chunks from offset 0 for a physical recording (holes stop the prefix). */
    public List<ChunkMetadata> contiguousChunks(String recordingId) {
        if (!byRecording.containsKey(recordingId)) {
            loadRecording(recordingId);
        }
        TreeMap<Long, ChunkMetadata> chunks = byRecording.get(recordingId);
        if (chunks == null || chunks.isEmpty()) {
            return List.of();
        }
        List<ChunkMetadata> contiguous = new ArrayList<>();
        long nextOffset = 0;
        while (true) {
            ChunkMetadata next = chunks.get(nextOffset);
            if (next == null) {
                break;
            }
            contiguous.add(next);
            nextOffset += next.chunkLength();
        }
        return contiguous;
    }

    public RecordingManifest manifest(String recordingId) {
        RecordingManifest manifest = new RecordingManifest();
        for (ChunkMetadata chunk : contiguousChunks(recordingId)) {
            manifest.chunkIds.add(chunk.chunkId());
            manifest.nextOffset += chunk.chunkLength();
        }
        manifest.stitchedBytes = manifest.nextOffset;
        return manifest;
    }

    public boolean hasContiguousPrefix(String recordingId) {
        return !contiguousChunks(recordingId).isEmpty();
    }

    /**
     * Payload paths in the order a stitch used to write them: each recording id, then that
     * recording's contiguous chunks from offset 0. A hole stops the prefix.
     */
    public List<Path> contiguousPayloads(List<String> recordingIds) throws IOException {
        List<Path> paths = new ArrayList<>();
        for (String recordingId : recordingIds) {
            List<ChunkMetadata> contiguous = contiguousChunks(recordingId);
            if (contiguous.isEmpty()) {
                throw new IOException("no contiguous chunks for " + recordingId);
            }
            for (ChunkMetadata chunk : contiguous) {
                paths.add(payloadPath(chunk.chunkId()));
            }
        }
        return List.copyOf(paths);
    }

    public String podName(String podUid, String stored) {
        if (stored != null && !stored.isBlank()) {
            return stored;
        }
        return podNames.getOrDefault(podUid, "");
    }

    public void rememberPodName(String namespace, String podUid, String podName) {
        if (podUid == null || podUid.isBlank() || podName == null || podName.isBlank()) {
            return;
        }
        String previous = podNames.put(podUid, podName);
        if (podName.equals(previous)) {
            return;
        }
        try {
            Files.createDirectories(identitiesDir);
            writeAtomic(
                    identitiesDir.resolve(safe(podUid) + ".json"),
                    MAPPER.writeValueAsBytes(new WorkloadIdentity(namespace, podName, podUid)));
        } catch (IOException error) {
            LOG.warnf(error, "unable to persist pod name for %s", podUid);
        }
    }

    private void loadRecording(String recordingId) {
        for (StoredChunk stored : storedChunks()) {
            ChunkMetadata metadata = readMeta(stored);
            if (metadata != null && recordingId.equals(metadata.recordingId())) {
                absorb(metadata);
            }
        }
    }

    private void indexStoredNames() {
        for (StoredChunk stored : storedChunks()) {
            chunkIds.add(stored.parsed().chunkId());
            payloadPaths.put(stored.parsed().chunkId(), stored.payload());
            metaPaths.put(stored.parsed().chunkId(), stored.meta());
        }
    }

    private void migrateLegacyChunks() throws IOException {
        if (!Files.isDirectory(chunksDir)) {
            return;
        }
        List<Path> metas;
        try (Stream<Path> files = Files.list(chunksDir)) {
            metas = files.filter(path -> path.getFileName().toString().endsWith(".meta")).toList();
        }
        for (Path meta : metas) {
            migrateOne(meta);
        }
    }

    private void migrateOne(Path meta) {
        try {
            ChunkMetadata metadata = MAPPER.readValue(meta.toFile(), ChunkMetadata.class);
            if (metadata.chunkId() == null) {
                deleteQuietly(meta);
                return;
            }
            Path legacyPayload = chunksDir.resolve(metadata.chunkId() + ".jfr");
            if (!Files.isRegularFile(legacyPayload)) {
                deleteQuietly(meta);
                return;
            }
            ChunkMetadata named = withSearchTimes(metadata, legacyPayload);
            Path payload = dataFile(named);
            Path metaTarget = metaFile(named);
            if (!payload.equals(legacyPayload)) {
                Files.createDirectories(payload.getParent());
                Files.createDirectories(metaTarget.getParent());
                Files.move(legacyPayload, payload, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
                writeAtomic(metaTarget, MAPPER.writeValueAsBytes(named));
                deleteQuietly(meta);
            }
        } catch (IOException error) {
            LOG.warnf(error, "skipping legacy chunk metadata %s", meta);
        }
    }

    private ChunkMetadata withSearchTimes(ChunkMetadata metadata, Path payload) {
        long startNs = metadata.chunkStartTimeNs();
        long durationNs = metadata.chunkDurationNs();
        if (startNs <= 0) {
            long[] times = JfrHeaderTimes.read(payload);
            if (times != null) {
                startNs = times[0];
                durationNs = times[1];
            }
        }
        String pod = podName(metadata.podUid(), metadata.podName());
        return new ChunkMetadata(
                metadata.chunkId(),
                metadata.recordingId(),
                metadata.clusterId(),
                metadata.namespace(),
                metadata.podUid(),
                pod,
                metadata.containerName(),
                metadata.physicalFilename(),
                metadata.chunkOffset(),
                metadata.chunkLength(),
                metadata.checksum(),
                startNs,
                durationNs);
    }

    private Path dataFile(ChunkMetadata metadata) {
        return dataDir.resolve(ChunkFileName.bucket(metadata)).resolve(ChunkFileName.fileName(metadata));
    }

    private Path metaFile(ChunkMetadata metadata) {
        return metaDir.resolve(ChunkFileName.bucket(metadata)).resolve(ChunkFileName.fileName(metadata));
    }

    private void deleteTmpParts() throws IOException {
        if (!Files.exists(tmpDir)) {
            return;
        }
        try (Stream<Path> parts = Files.list(tmpDir)) {
            parts.filter(path -> path.getFileName().toString().endsWith(".part")).forEach(ChunkStore::deleteQuietly);
        }
    }

    private void deleteOrphanPayloads() throws IOException {
        deleteLegacyChunkOrphans();
        if (!Files.isDirectory(dataDir)) {
            return;
        }
        try (Stream<Path> buckets = Files.list(dataDir)) {
            for (Path bucket : buckets.filter(Files::isDirectory).toList()) {
                try (Stream<Path> files = Files.list(bucket)) {
                    for (Path payload : files.toList()) {
                        Path meta = metaDir.resolve(bucket.getFileName()).resolve(payload.getFileName());
                        if (!Files.exists(meta)) {
                            deleteQuietly(payload);
                        }
                    }
                }
            }
        }
    }

    private void deleteLegacyChunkOrphans() throws IOException {
        if (!Files.isDirectory(chunksDir)) {
            return;
        }
        try (Stream<Path> files = Files.list(chunksDir)) {
            files.filter(path -> path.getFileName().toString().endsWith(".jfr")).forEach(payload -> {
                Path meta = chunksDir.resolve(payload.getFileName().toString().replaceFirst("\\.jfr$", ".meta"));
                if (!Files.exists(meta)) {
                    deleteQuietly(payload);
                }
            });
        }
    }

    private void deleteStitchCache() throws IOException {
        Path cacheDir = root.resolve("stitch-cache");
        if (!Files.exists(cacheDir)) {
            return;
        }
        Files.walkFileTree(cacheDir, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                deleteQuietly(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException error) {
                deleteQuietly(dir);
                return FileVisitResult.CONTINUE;
            }
        });
    }

    private void deleteLegacyStitchedRecordings() throws IOException {
        if (!Files.exists(recordingsDir)) {
            return;
        }
        Files.walkFileTree(recordingsDir, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                String name = file.getFileName().toString();
                if (name.equals("stitched.jfr") || name.equals("manifest.json")) {
                    deleteQuietly(file);
                }
                return FileVisitResult.CONTINUE;
            }
        });
    }

    private void loadIdentities() throws IOException {
        if (!Files.exists(identitiesDir)) {
            return;
        }
        try (Stream<Path> files = Files.list(identitiesDir)) {
            files.filter(path -> path.getFileName().toString().endsWith(".json")).forEach(path -> {
                try {
                    WorkloadIdentity identity = MAPPER.readValue(path.toFile(), WorkloadIdentity.class);
                    if (identity.podUid() != null && identity.podName() != null && !identity.podName().isBlank()) {
                        podNames.putIfAbsent(identity.podUid(), identity.podName());
                    }
                } catch (IOException error) {
                    LOG.warnf(error, "skipping unreadable identity %s", path);
                }
            });
        }
    }

    static String safe(String value) {
        if (value == null || value.isBlank()) {
            return "_";
        }
        String cleaned = value.replaceAll("[^A-Za-z0-9._-]", "_");
        if (cleaned.equals(".") || cleaned.equals("..")) {
            return "_";
        }
        return cleaned;
    }

    private static void writeAtomic(Path target, byte[] bytes) throws IOException {
        Path tmp = target.resolveSibling(target.getFileName() + ".tmp");
        try (FileChannel channel = FileChannel.open(
                tmp,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING)) {
            channel.write(ByteBuffer.wrap(bytes));
            channel.force(true);
        }
        Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    }

    static void deleteQuietly(Path path) {
        try {
            Files.deleteIfExists(path);
        } catch (IOException ignored) {
        }
    }

    public record StoredChunk(ChunkFileName.Parsed parsed, Path payload, Path meta) {}

    public final class IncomingWrite {
        private final String chunkId;
        private final Path part;
        private final FileChannel channel;

        IncomingWrite(String chunkId, Path part, FileChannel channel) {
            this.chunkId = chunkId;
            this.part = part;
            this.channel = channel;
        }

        public void write(byte[] payload) throws IOException {
            channel.write(ByteBuffer.wrap(payload));
        }

        public void force() throws IOException {
            channel.force(true);
        }

        public void closeQuietly() {
            try {
                channel.close();
            } catch (IOException ignored) {
            }
        }

        public void abort() {
            closeQuietly();
            deleteQuietly(part);
        }

        public String chunkId() {
            return chunkId;
        }
    }

    public record WorkloadIdentity(
            @JsonProperty("namespace") String namespace,
            @JsonProperty("podName") String podName,
            @JsonProperty("podUid") String podUid) {
    }
}
