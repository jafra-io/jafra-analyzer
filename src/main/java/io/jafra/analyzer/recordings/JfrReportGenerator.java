package io.jafra.analyzer.recordings;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.RunnableFuture;

import jakarta.enterprise.context.ApplicationScoped;

import com.fasterxml.jackson.annotation.JsonInclude;

import org.openjdk.jmc.common.item.IItemCollection;
import org.openjdk.jmc.common.unit.IQuantity;
import org.openjdk.jmc.common.util.IPreferenceValueProvider;
import org.openjdk.jmc.common.util.TypedPreference;
import org.openjdk.jmc.flightrecorder.rules.IResult;
import org.openjdk.jmc.flightrecorder.rules.IRule;
import org.openjdk.jmc.flightrecorder.rules.ResultProvider;
import org.openjdk.jmc.flightrecorder.rules.RuleRegistry;
import org.openjdk.jmc.flightrecorder.rules.Severity;
import org.openjdk.jmc.flightrecorder.rules.TypedResult;

@ApplicationScoped
public class JfrReportGenerator {
    public Map<String, AnalysisFinding> analyze(List<Path> chunks, String filter) throws IOException {
        return analyze(chunks, filter, null, null);
    }

    public Map<String, AnalysisFinding> analyze(List<Path> chunks, String filter, Instant from, Instant to)
            throws IOException {
        IItemCollection events = JfrChunkFiles.load(chunks);
        events = JfrTimeFilter.apply(events, from, to);
        ResultProvider results = new ResultProvider();
        Map<String, AnalysisFinding> findings = new LinkedHashMap<>();
        String needle = filter == null ? "" : filter.trim().toLowerCase();
        for (IRule rule : RuleRegistry.getRules()) {
            try {
                RunnableFuture<IResult> future =
                        rule.createEvaluation(events, IPreferenceValueProvider.DEFAULT_VALUES, results);
                future.run();
                IResult result = future.get();
                if (result == null) {
                    continue;
                }
                results.addResults(result);
                if (!matches(rule, needle)) {
                    continue;
                }
                findings.put(rule.getId(), toFinding(result));
            } catch (Exception error) {
                if (!matches(rule, needle)) {
                    continue;
                }
                findings.put(
                        rule.getId(),
                        new AnalysisFinding(
                                Severity.NA.getLimit(),
                                rule.getName(),
                                blankToTopic(rule.getTopic()),
                                "Not applicable: this recording does not contain the JDK Flight Recorder events this rule needs."));
            }
        }
        return findings;
    }

    public SummaryReport report(
            RecordingCatalog.IndexedRecording recording, Map<String, AnalysisFinding> findings) {
        return new SummaryReport(
                recording.key().namespace(),
                recording.key().pod(),
                recording.key().container(),
                recording.summary().filename(),
                List.of(recording.summary().filename()),
                recording.summary().start(),
                recording.summary().end(),
                null,
                null,
                recording.summary().bytes(),
                recording.summary().chunks(),
                Instant.now().toString(),
                findings);
    }

    public SummaryReport report(
            RecordingCatalog.WindowSelection window, Map<String, AnalysisFinding> findings) {
        List<String> files = window.recordings().stream()
                .map(recording -> recording.summary().filename())
                .toList();
        return new SummaryReport(
                window.key().namespace(),
                window.key().pod(),
                window.key().container(),
                String.join(",", files),
                files,
                iso(window.start()),
                iso(window.stop()),
                iso(window.from()),
                iso(window.to()),
                window.bytes(),
                window.chunks(),
                Instant.now().toString(),
                findings);
    }

    private static String iso(Instant instant) {
        return instant == null ? null : instant.toString();
    }

    private static boolean matches(IRule rule, String needle) {
        if (needle.isEmpty()) {
            return true;
        }
        return contains(rule.getId(), needle)
                || contains(rule.getName(), needle)
                || contains(rule.getTopic(), needle);
    }

    private static boolean contains(String value, String needle) {
        return value != null && value.toLowerCase().contains(needle);
    }

    private static AnalysisFinding toFinding(IResult result) {
        IRule rule = result.getRule();
        return new AnalysisFinding(
                scoreOf(result),
                rule.getName(),
                blankToTopic(rule.getTopic()),
                descriptionOf(result));
    }

    private static double scoreOf(IResult result) {
        IRule rule = result.getRule();
        if (rule.getResults() != null) {
            for (TypedResult<?> typed : rule.getResults()) {
                if (typed == null || typed.getIdentifier() == null) {
                    continue;
                }
                if (!typed.getIdentifier().toLowerCase().contains("score")) {
                    continue;
                }
                Object value = result.getResult(typed);
                if (value instanceof Number number) {
                    return number.doubleValue();
                }
                if (value instanceof IQuantity quantity) {
                    return quantity.doubleValue();
                }
            }
        }
        return result.getSeverity().getLimit();
    }

    private static String descriptionOf(IResult result) {
        StringBuilder text = new StringBuilder();
        append(text, interpolate(result, result.getSummary()));
        append(text, interpolate(result, result.getExplanation()));
        append(text, interpolate(result, result.getSolution()));
        return text.toString().trim();
    }

    private static String interpolate(IResult result, String text) {
        if (text == null || text.isBlank()) {
            return "";
        }
        String rendered = text;
        IRule rule = result.getRule();
        if (rule.getResults() != null) {
            for (TypedResult<?> typed : rule.getResults()) {
                if (typed == null || typed.getIdentifier() == null) {
                    continue;
                }
                rendered = rendered.replace("{" + typed.getIdentifier() + "}", formatValue(result.getResult(typed)));
            }
        }
        if (rule.getConfigurationAttributes() != null) {
            for (TypedPreference<?> preference : rule.getConfigurationAttributes()) {
                if (preference == null || preference.getIdentifier() == null) {
                    continue;
                }
                rendered = rendered.replace("{" + preference.getIdentifier() + "}", formatValue(result.getPreference(preference)));
            }
        }
        return rendered;
    }

    private static String formatValue(Object value) {
        if (value == null) {
            return "n/a";
        }
        if (value instanceof IQuantity quantity) {
            return String.valueOf(quantity);
        }
        return String.valueOf(value);
    }

    private static void append(StringBuilder target, String value) {
        if (value == null || value.isBlank()) {
            return;
        }
        if (!target.isEmpty()) {
            target.append('\n');
        }
        target.append(value.trim());
    }

    private static String blankToTopic(String topic) {
        return topic == null || topic.isBlank() ? "general" : topic;
    }

    public record AnalysisFinding(double score, String name, String topic, String description) {}

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record SummaryReport(
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
            Map<String, AnalysisFinding> findings) {}
}
