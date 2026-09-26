package io.jafra.analyzer.recordings;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import jakarta.enterprise.context.ApplicationScoped;

import com.fasterxml.jackson.annotation.JsonInclude;

import org.openjdk.jmc.common.item.Aggregators;
import org.openjdk.jmc.common.item.IAttribute;
import org.openjdk.jmc.common.item.IItem;
import org.openjdk.jmc.common.item.IItemCollection;
import org.openjdk.jmc.common.item.IItemIterable;
import org.openjdk.jmc.common.item.IMemberAccessor;
import org.openjdk.jmc.common.item.IType;
import org.openjdk.jmc.common.item.ItemFilters;
import org.openjdk.jmc.common.unit.ContentType;
import org.openjdk.jmc.common.unit.IQuantity;
import org.openjdk.jmc.common.unit.KindOfQuantity;
import org.openjdk.jmc.common.unit.QuantityConversionException;
import org.openjdk.jmc.common.unit.UnitLookup;

@ApplicationScoped
public class JfrEventSummarizer {
    private static final int MAX_DISTINCT_TEXT = 8;
    private static final int MAX_TEXT_CHARS = 4000;
    private static final Set<String> SKIP_FIELDS = Set.of(
            "startTime",
            "endTime",
            "eventType",
            "eventThread",
            "stackTrace",
            "sampledThread");

    public EventSummaryDocument summarize(RecordingCatalog.WindowSelection selection, String filter)
            throws IOException {
        IItemCollection events = JfrChunkFiles.load(selection.chunkFiles());
        events = JfrTimeFilter.apply(events, selection.from(), selection.to());
        String needle = filter == null ? "" : filter.trim().toLowerCase(Locale.ROOT);
        Map<String, EventSummary> byType = summarizeEvents(events, needle);
        Map<String, TopicSummary> topics = summarizeTopics(byType);
        List<String> files = selection.recordings().stream()
                .map(item -> item.summary().filename())
                .toList();
        return new EventSummaryDocument(
                selection.key().namespace(),
                selection.key().pod(),
                selection.key().container(),
                String.join(",", files),
                files,
                iso(selection.start()),
                iso(selection.stop()),
                iso(selection.from()),
                iso(selection.to()),
                selection.bytes(),
                selection.chunks(),
                Instant.now().toString(),
                byType,
                topics);
    }

    private static Map<String, EventSummary> summarizeEvents(IItemCollection events, String needle) {
        Map<String, IType<?>> types = new LinkedHashMap<>();
        for (IItemIterable iterable : events) {
            IType<?> type = iterable.getType();
            if (type != null && type.getIdentifier() != null) {
                types.putIfAbsent(type.getIdentifier(), type);
            }
        }
        Map<String, EventSummary> summaries = new LinkedHashMap<>();
        for (Map.Entry<String, IType<?>> entry : types.entrySet()) {
            IType<?> type = entry.getValue();
            String topic = topicOf(type.getIdentifier());
            if (!matches(needle, type.getIdentifier(), type.getName(), topic)) {
                continue;
            }
            IItemCollection typed = events.apply(ItemFilters.type(type.getIdentifier()));
            long count = quantityLong(typed.getAggregate(Aggregators.count()));
            if (count <= 0) {
                continue;
            }
            Map<String, FieldStats> fields = new LinkedHashMap<>();
            for (IAttribute<?> attribute : type.getAttributes()) {
                if (attribute == null || skipAttribute(attribute)) {
                    continue;
                }
                FieldStats stats = attributeStats(typed, attribute);
                if (stats != null) {
                    fields.put(attribute.getIdentifier(), stats);
                }
            }
            summaries.put(type.getIdentifier(), new EventSummary(type.getName(), topic, count, fields));
        }
        return summaries;
    }

    private static Map<String, TopicSummary> summarizeTopics(Map<String, EventSummary> byType) {
        Map<String, TopicAccumulator> buckets = new LinkedHashMap<>();
        for (Map.Entry<String, EventSummary> entry : byType.entrySet()) {
            EventSummary event = entry.getValue();
            TopicAccumulator bucket = buckets.computeIfAbsent(event.topic(), ignored -> new TopicAccumulator());
            bucket.count += event.count();
            bucket.types.add(entry.getKey());
            for (Map.Entry<String, FieldStats> field : event.fields().entrySet()) {
                FieldStats stats = field.getValue();
                if (stats.values() == null || stats.values().isEmpty()) {
                    continue;
                }
                String key = field.getKey();
                if (bucket.stats.containsKey(key)) {
                    key = entry.getKey() + "." + field.getKey();
                }
                bucket.stats.put(key, new ValueStats(stats.name(), null, null, null, String.join("\n", stats.values())));
            }
        }
        Map<String, TopicSummary> topics = new LinkedHashMap<>();
        buckets.forEach((topic, bucket) -> topics.put(
                topic, new TopicSummary(bucket.count, List.copyOf(bucket.types), bucket.stats)));
        return topics;
    }

    private static FieldStats attributeStats(IItemCollection events, IAttribute<?> attribute) {
        ContentType<?> contentType = attribute.getContentType();
        if (contentType == UnitLookup.TIMESTAMP || isKind(contentType, "timestamp")) {
            return timestampStats(events, attribute);
        }
        if (!(contentType instanceof KindOfQuantity)) {
            List<String> values = distinctText(events, attribute);
            return values.isEmpty() ? null : FieldStats.text(attribute.getName(), values);
        }
        if (skipNumeric(attribute, contentType)) {
            return null;
        }
        @SuppressWarnings("unchecked")
        IAttribute<IQuantity> quantity = (IAttribute<IQuantity>) attribute;
        return quantityStats(events, quantity, contentType);
    }

    private static FieldStats timestampStats(IItemCollection events, IAttribute<?> attribute) {
        try {
            @SuppressWarnings("unchecked")
            IAttribute<IQuantity> quantity = (IAttribute<IQuantity>) attribute;
            IQuantity min = events.getAggregate(Aggregators.min(quantity));
            IQuantity max = events.getAggregate(Aggregators.max(quantity));
            String first = isoQuantity(min);
            String last = isoQuantity(max);
            if (first == null && last == null) {
                return null;
            }
            List<String> values = new ArrayList<>();
            if (first != null) {
                values.add(first);
            }
            if (last != null && !last.equals(first)) {
                values.add(last);
            }
            return FieldStats.text(attribute.getName(), values);
        } catch (Exception ignored) {
            return null;
        }
    }

    private static FieldStats quantityStats(
            IItemCollection events, IAttribute<IQuantity> attribute, ContentType<?> contentType) {
        try {
            IQuantity min = events.getAggregate(Aggregators.min(attribute));
            IQuantity max = events.getAggregate(Aggregators.max(attribute));
            IQuantity avg = events.getAggregate(Aggregators.avg(attribute));
            if (sentinel(min) || sentinel(max) || sentinel(avg)) {
                return null;
            }
            if (min == null && max == null && avg == null) {
                return null;
            }
            IQuantity sum = null;
            if (additive(contentType)) {
                sum = events.getAggregate(Aggregators.sum(attribute));
                if (sentinel(sum)) {
                    sum = null;
                }
            }
            boolean percent = contentType == UnitLookup.PERCENTAGE || isKind(contentType, "percentage");
            return FieldStats.numeric(
                    attribute.getName(),
                    percent ? "%" : unitOf(first(min, max, avg, sum)),
                    quantityNumber(percent ? asPercent(min) : min),
                    quantityNumber(percent ? asPercent(max) : max),
                    quantityNumber(percent ? asPercent(avg) : avg),
                    quantityNumber(percent ? asPercent(sum) : sum),
                    display(percent ? asPercent(min) : min),
                    display(percent ? asPercent(max) : max),
                    display(percent ? asPercent(avg) : avg),
                    display(percent ? asPercent(sum) : sum));
        } catch (Exception ignored) {
            return null;
        }
    }

    private static List<String> distinctText(IItemCollection events, IAttribute<?> attribute) {
        Set<String> values = new LinkedHashSet<>();
        for (IItemIterable iterable : events) {
            IMemberAccessor<?, IItem> accessor = accessor(attribute, iterable.getType());
            if (accessor == null) {
                continue;
            }
            for (IItem item : iterable) {
                Object value = accessor.getMember(item);
                String text = stringify(value);
                if (text == null) {
                    continue;
                }
                values.add(text);
                if (values.size() >= MAX_DISTINCT_TEXT) {
                    return List.copyOf(values);
                }
            }
        }
        return List.copyOf(values);
    }

    @SuppressWarnings("unchecked")
    private static IMemberAccessor<?, IItem> accessor(IAttribute<?> attribute, IType<?> type) {
        try {
            return attribute.getAccessor((IType<IItem>) type);
        } catch (Exception ignored) {
            return null;
        }
    }

    private static String stringify(Object value) {
        if (value == null || value instanceof IType<?>) {
            return null;
        }
        if (value instanceof IQuantity quantity) {
            return isoQuantity(quantity);
        }
        String text = String.valueOf(value).trim();
        if (text.isEmpty() || "null".equals(text)) {
            return null;
        }
        if (text.startsWith("Type(") && text.endsWith(")")) {
            return null;
        }
        return text.length() <= MAX_TEXT_CHARS ? text : text.substring(0, MAX_TEXT_CHARS);
    }

    private static boolean skipAttribute(IAttribute<?> attribute) {
        String raw = attribute.getIdentifier() == null ? "" : attribute.getIdentifier();
        String id = unwrapSynthetic(raw);
        if (SKIP_FIELDS.contains(raw) || SKIP_FIELDS.contains(id)) {
            return true;
        }
        ContentType<?> contentType = attribute.getContentType();
        return contentType == UnitLookup.TYPE
                || contentType == UnitLookup.THREAD
                || contentType == UnitLookup.STACKTRACE
                || contentType == UnitLookup.STACKTRACE_FRAME;
    }

    private static String unwrapSynthetic(String identifier) {
        if (identifier.length() >= 2 && identifier.startsWith("(") && identifier.endsWith(")")) {
            return identifier.substring(1, identifier.length() - 1);
        }
        return identifier;
    }

    private static boolean skipNumeric(IAttribute<?> attribute, ContentType<?> contentType) {
        if (contentType == UnitLookup.ADDRESS
                || contentType == UnitLookup.IDENTIFIER
                || contentType == UnitLookup.INDEX) {
            return true;
        }
        if (isKind(contentType, "address", "identifier", "index")) {
            return true;
        }
        String id = attribute.getIdentifier() == null ? "" : attribute.getIdentifier().toLowerCase(Locale.ROOT);
        if (id.contains("address") || id.equals("gcid") || id.endsWith(":gcid")) {
            return true;
        }
        return (id.endsWith(":start") || id.endsWith(":end")) && !id.contains("size");
    }

    private static boolean additive(ContentType<?> contentType) {
        return contentType == UnitLookup.MEMORY
                || isKind(contentType, "memory");
    }

    private static boolean isKind(ContentType<?> contentType, String... kinds) {
        if (contentType == null || contentType.getIdentifier() == null) {
            return false;
        }
        String identifier = contentType.getIdentifier().toLowerCase(Locale.ROOT);
        for (String kind : kinds) {
            if (identifier.equals(kind) || identifier.endsWith("." + kind)) {
                return true;
            }
        }
        return false;
    }

    private static boolean sentinel(IQuantity quantity) {
        if (quantity == null) {
            return false;
        }
        try {
            long value = quantity.longValue();
            if (value == Long.MAX_VALUE || value == Long.MIN_VALUE) {
                return true;
            }
        } catch (Exception ignored) {
            // percentages and fractions are not always convertible to long
        }
        try {
            return !Double.isFinite(quantity.doubleValue());
        } catch (Exception ignored) {
            return false;
        }
    }

    private static IQuantity asPercent(IQuantity quantity) {
        if (quantity == null) {
            return null;
        }
        try {
            return quantity.in(UnitLookup.PERCENT);
        } catch (Exception ignored) {
            return quantity;
        }
    }

    private static IQuantity first(IQuantity... values) {
        for (IQuantity value : values) {
            if (value != null) {
                return value;
            }
        }
        return null;
    }

    private static Number quantityNumber(IQuantity quantity) {
        if (quantity == null) {
            return null;
        }
        Number value = quantity.numberValue();
        if (value == null) {
            return null;
        }
        double numeric = value.doubleValue();
        if (!Double.isFinite(numeric)) {
            return null;
        }
        if (Math.abs(numeric - Math.rint(numeric)) < 1e-9 && Math.abs(numeric) <= Long.MAX_VALUE) {
            return (long) Math.rint(numeric);
        }
        return Math.round(numeric * 10_000d) / 10_000d;
    }

    private static long quantityLong(IQuantity quantity) {
        return quantity == null ? 0 : quantity.longValue();
    }

    private static String display(IQuantity quantity) {
        if (quantity == null) {
            return null;
        }
        String formatted = quantity.interactiveFormat();
        if (formatted != null && formatted.contains("\u00d7100")) {
            return quantityNumber(asPercent(quantity)) + " %";
        }
        return formatted;
    }

    private static String unitOf(IQuantity quantity) {
        if (quantity == null || quantity.getUnit() == null) {
            return null;
        }
        String unit = quantity.getUnit().getIdentifier();
        return unit == null ? null : unit.replace('\u00d7', 'x');
    }

    private static String isoQuantity(IQuantity quantity) {
        if (quantity == null) {
            return null;
        }
        try {
            return Instant.ofEpochMilli(quantity.longValueIn(UnitLookup.EPOCH_MS)).toString();
        } catch (QuantityConversionException | RuntimeException ignored) {
            return quantity.interactiveFormat();
        }
    }

    static String topicOf(String typeId) {
        String id = typeId == null ? "" : typeId.toLowerCase(Locale.ROOT);
        String name = id.contains(".") ? id.substring(id.lastIndexOf('.') + 1) : id;
        if (containsAny(name, "cpu", "execution", "wallclock", "threadcpu")) {
            return "cpu";
        }
        if (containsAny(name, "heap", "alloc", "tlab", "objectcount", "metaspace", "tenuring", "liveobject")
                || name.equals("malloc")) {
            return "heap";
        }
        if (name.startsWith("gc")
                || containsAny(name, "garbagecollection", "gcphase", "gcpause", "gcheap", "gctlab", "g1mmu")) {
            return "garbage_collection";
        }
        if (containsAny(name, "monitor", "lock", "park", "contention", "blocked")) {
            return "lock";
        }
        if (name.equals("filewrite")
                || name.equals("fileread")
                || name.equals("fileforce")
                || containsAny(name, "socket", "ioexception")
                || name.startsWith("filei")
                || name.startsWith("socket")) {
            return "io";
        }
        if (containsAny(name, "nativemem", "nativeallocation") || name.equals("free")) {
            return "native_memory";
        }
        if (containsAny(name, "compilation", "codecache", "compiler", "codesweeper", "deoptimization")) {
            return "jit";
        }
        if (containsAny(name, "classload") || name.equals("methodtrace")) {
            return "classloading";
        }
        if (containsAny(name, "jvm", "osinformation", "vmoperation", "systemprocess", "safepoint")
                || name.equals("flag")) {
            return "jvm";
        }
        return "general";
    }

    private static boolean matches(String needle, String... values) {
        if (needle == null || needle.isBlank()) {
            return true;
        }
        for (String value : values) {
            if (value != null && value.toLowerCase(Locale.ROOT).contains(needle)) {
                return true;
            }
        }
        return false;
    }

    private static boolean containsAny(String haystack, String... needles) {
        for (String needle : needles) {
            if (haystack.contains(needle)) {
                return true;
            }
        }
        return false;
    }

    private static String iso(Instant instant) {
        return instant == null ? null : instant.toString();
    }

    private static final class TopicAccumulator {
        long count;
        final List<String> types = new ArrayList<>();
        final Map<String, ValueStats> stats = new LinkedHashMap<>();
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record FieldStats(
            String name,
            String unit,
            Number min,
            Number max,
            Number avg,
            Number sum,
            String minDisplay,
            String maxDisplay,
            String avgDisplay,
            String sumDisplay,
            List<String> values) {
        static FieldStats numeric(
                String name,
                String unit,
                Number min,
                Number max,
                Number avg,
                Number sum,
                String minDisplay,
                String maxDisplay,
                String avgDisplay,
                String sumDisplay) {
            return new FieldStats(
                    name, unit, min, max, avg, sum, minDisplay, maxDisplay, avgDisplay, sumDisplay, null);
        }

        static FieldStats text(String name, List<String> values) {
            return new FieldStats(name, null, null, null, null, null, null, null, null, null, List.copyOf(values));
        }
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record ValueStats(String name, String unit, Number value, String display, String text) {}

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record EventSummary(String name, String topic, long count, Map<String, FieldStats> fields) {}

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record TopicSummary(long eventCount, List<String> eventTypes, Map<String, ValueStats> stats) {}

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record EventSummaryDocument(
            String namespace,
            String pod,
            String container,
            String recording,
            List<String> recordings,
            String start,
            String end,
            String from,
            String to,
            long bytes,
            int chunks,
            String generatedAt,
            Map<String, EventSummary> events,
            Map<String, TopicSummary> topics) {}
}
