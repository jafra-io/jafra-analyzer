package io.jafra.analyzer.storage;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Singleton;

import org.eclipse.microprofile.config.inject.ConfigProperty;

import io.quarkus.runtime.StartupEvent;

@ApplicationScoped
public class AnalyzerStorage {
    @Produces
    @Singleton
    ChunkStore chunkStore(
            @ConfigProperty(name = "jafra.storage.root", defaultValue = "/var/lib/jafra/analyzer") String root)
            throws IOException {
        Path path = Path.of(root);
        Files.createDirectories(path);
        ChunkStore store = new ChunkStore(path);
        store.recover();
        return store;
    }

    void warmup(@Observes StartupEvent event, ChunkStore store) {
        store.durableChunkCount();
    }
}
