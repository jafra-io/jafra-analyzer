package io.jafra.analyzer.recordings;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.openjdk.jmc.common.item.IItemCollection;
import org.openjdk.jmc.flightrecorder.CouldNotLoadRecordingException;
import org.openjdk.jmc.flightrecorder.JfrLoaderToolkit;

/** Loads chunk files in the order given. Callers pass stitch order; this class does not sort. */
final class JfrChunkFiles {
    private JfrChunkFiles() {}

    static IItemCollection load(List<Path> chunks) throws IOException {
        if (chunks == null || chunks.isEmpty()) {
            throw new IOException("recording chunks are empty");
        }
        List<File> files = new ArrayList<>(chunks.size());
        for (Path chunk : chunks) {
            if (chunk == null || !Files.isRegularFile(chunk) || Files.size(chunk) == 0) {
                throw new IOException("recording chunk is empty");
            }
            files.add(chunk.toFile());
        }
        try {
            return JfrLoaderToolkit.loadEvents(files);
        } catch (CouldNotLoadRecordingException error) {
            throw new IOException("unable to load JFR: " + error.getMessage(), error);
        }
    }
}
