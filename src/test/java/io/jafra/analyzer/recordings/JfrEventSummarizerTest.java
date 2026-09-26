package io.jafra.analyzer.recordings;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import jakarta.inject.Inject;

import org.junit.jupiter.api.Test;

import io.jafra.analyzer.storage.ChunkMetadata;
import io.jafra.analyzer.storage.ChunkStore;
import io.quarkus.test.junit.QuarkusTest;
import jdk.jfr.Configuration;
import jdk.jfr.Recording;

@QuarkusTest
class JfrEventSummarizerTest {
    @Inject
    JfrEventSummarizer summarizer;

    @Inject
    RecordingCatalog catalog;

    @Inject
    ChunkStore store;

    @Test
    void topicOfUsesEventNameNotProfilerSubstring() {
        assertEquals("cpu", JfrEventSummarizer.topicOf("jdk.CPUInformation"));
        assertEquals("cpu", JfrEventSummarizer.topicOf("profiler.WallClockSample"));
        assertEquals("heap", JfrEventSummarizer.topicOf("jdk.ObjectAllocationInNewTLAB"));
        assertEquals("native_memory", JfrEventSummarizer.topicOf("profiler.Free"));
        assertEquals("io", JfrEventSummarizer.topicOf("jdk.FileWrite"));
        assertEquals("general", JfrEventSummarizer.topicOf("profiler.Log"));
        assertEquals("jvm", JfrEventSummarizer.topicOf("jdk.OSInformation"));
        assertEquals("jit", JfrEventSummarizer.topicOf("jdk.Compilation"));
        assertEquals("garbage_collection", JfrEventSummarizer.topicOf("jdk.GCPhasePause"));
        assertEquals("garbage_collection", JfrEventSummarizer.topicOf("jdk.YoungGarbageCollection"));
        assertEquals("general", JfrEventSummarizer.topicOf("jdk.ActiveSettingc4005a0c-22d8-4210-b5ba-de888a4220c6"));
        assertEquals("jvm", JfrEventSummarizer.topicOf("jdk.SafepointBegin"));
        assertEquals("io", JfrEventSummarizer.topicOf("jdk.SocketRead"));
    }

    @Test
    void summaryReadsCpuTypeFromJfrAndSkipsAddresses() throws Exception {
        persist("sum-cpu", "sum-jfr", "sum-pod", "sum-container", "profile-1.jfr", dumpBusyRecording());
        var selection =
                catalog.singleFile(catalog.requireRecording("sum-jfr", "sum-pod", "sum-container", "profile-1.jfr"));
        var document = summarizer.summarize(selection, null);

            assertFalse(document.events().isEmpty());
            document.events().values().forEach(event -> {
                assertTrue(event.count() > 0, event.name());
                event.fields().forEach((name, stats) -> {
                    assertFalse(name.contains("eventType") || name.equals("(eventType)"), name);
                    if (stats.values() != null) {
                        stats.values().forEach(value ->
                                assertFalse(value.startsWith("Type(") && value.endsWith(")"), value));
                    }
                    assertFalse(name.toLowerCase().contains("address"), name);
                    assertFalse(name.equalsIgnoreCase("gcId"), name);
                    if (stats.values() == null) {
                        assertFalse(isHugePointer(stats.min()), name);
                        assertFalse(isHugePointer(stats.max()), name);
                        assertFalse(isHugePointer(stats.sum()), name);
                    }
                });
            });
            document.topics().values().forEach(topic -> {
                assertFalse(topic.stats().containsKey("CPU_TYPE"));
                assertFalse(topic.stats().containsKey("BASE_ADDRESS"));
                assertFalse(topic.stats().containsKey("ALLOCATION_TOTAL"));
            });

            var cpu = document.events().get("jdk.CPUInformation");
            if (cpu != null && cpu.fields().containsKey("cpu")) {
                List<String> values = cpu.fields().get("cpu").values();
                assertFalse(values == null || values.isEmpty());
            }
    }

    private void persist(
            String chunkId, String namespace, String pod, String container, String filename, byte[] payload)
            throws Exception {
        String recordingId = "local-demo/" + pod + "/" + container + "/" + filename;
        ChunkMetadata metadata = new ChunkMetadata(
                chunkId,
                recordingId,
                "local-demo",
                namespace,
                "uid-" + pod,
                pod,
                container,
                filename,
                0,
                payload.length,
                "checksum");
        ChunkStore.IncomingWrite write = store.beginWrite(chunkId);
        write.write(payload);
        store.commit(write, metadata);
    }

    private static boolean isHugePointer(Number value) {
        return value != null && value.doubleValue() > 1e13d;
    }

    private static byte[] dumpBusyRecording() throws Exception {
        Path dump = Files.createTempFile("jafra-summarizer", ".jfr");
        try (Recording recording = new Recording(Configuration.getConfiguration("default"))) {
            recording.start();
            byte[] scratch = new byte[256];
            long until = System.nanoTime() + 200_000_000L;
            while (System.nanoTime() < until) {
                scratch = new byte[256 + (scratch.length % 32)];
            }
            recording.dump(dump);
            return Files.readAllBytes(dump);
        } finally {
            Files.deleteIfExists(dump);
        }
    }
}
