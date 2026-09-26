package io.jafra.analyzer.recordings;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import jdk.jfr.Label;
import jdk.jfr.Name;
import jdk.jfr.Recording;
import jdk.jfr.StackTrace;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.openjdk.jmc.common.item.IAttribute;
import org.openjdk.jmc.common.item.IItem;
import org.openjdk.jmc.common.item.IItemCollection;
import org.openjdk.jmc.common.item.IItemIterable;
import org.openjdk.jmc.common.item.IMemberAccessor;
import org.openjdk.jmc.common.item.IType;
import org.openjdk.jmc.flightrecorder.JfrLoaderToolkit;

class JfrChunkLoadTest {
    @Test
    void listLoadMatchesConcatenatedBytesInStitchOrder(@TempDir Path tmp) throws Exception {
        Path first = tmp.resolve("a.jfr");
        Path second = tmp.resolve("b.jfr");
        writeRecording(first, "alpha");
        writeRecording(second, "beta");

        List<Path> stitchOrder = List.of(first, second);
        IItemCollection fromChunks = JfrChunkFiles.load(stitchOrder);

        Path concatenated = tmp.resolve("stitched.jfr");
        try (OutputStream out = Files.newOutputStream(concatenated)) {
            Files.copy(first, out);
            Files.copy(second, out);
        }
        IItemCollection fromFile = JfrLoaderToolkit.loadEvents(concatenated.toFile());
        assertEquals(tokens(fromFile), tokens(fromChunks));

        Path reversedConcat = tmp.resolve("reversed.jfr");
        try (OutputStream out = Files.newOutputStream(reversedConcat)) {
            Files.copy(second, out);
            Files.copy(first, out);
        }
        List<Path> reversed = List.of(second, first);
        IItemCollection fromReversed = JfrChunkFiles.load(reversed);
        IItemCollection fromReversedFile = JfrLoaderToolkit.loadEvents(reversedConcat.toFile());
        assertEquals(tokens(fromReversedFile), tokens(fromReversed));
        assertFalse(Arrays.equals(Files.readAllBytes(concatenated), Files.readAllBytes(reversedConcat)));
    }

    private static void writeRecording(Path path, String token) throws Exception {
        try (Recording recording = new Recording()) {
            recording.enable(Marker.class);
            recording.start();
            Marker marker = new Marker();
            marker.token = token;
            marker.commit();
            recording.stop();
            recording.dump(path);
        }
    }

    private static List<String> tokens(IItemCollection events) {
        List<String> tokens = new ArrayList<>();
        for (IItemIterable iterable : events) {
            IType<IItem> type = iterable.getType();
            if (type == null || !"io.jafra.Marker".equals(type.getIdentifier())) {
                continue;
            }
            IAttribute<String> token = tokenAttribute(type);
            if (token == null) {
                continue;
            }
            IMemberAccessor<String, IItem> accessor = token.getAccessor(type);
            for (IItem item : iterable) {
                tokens.add(accessor.getMember(item));
            }
        }
        return tokens;
    }

    @SuppressWarnings("unchecked")
    private static IAttribute<String> tokenAttribute(IType<IItem> type) {
        for (IAttribute<?> attribute : type.getAttributes()) {
            if (attribute != null && "token".equals(attribute.getIdentifier())) {
                return (IAttribute<String>) attribute;
            }
        }
        return null;
    }

    @Name("io.jafra.Marker")
    @Label("Marker")
    @StackTrace(false)
    public static class Marker extends jdk.jfr.Event {
        @Label("Token")
        public String token;
    }
}
