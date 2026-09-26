package io.jafra.analyzer.api;

import java.io.IOException;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import io.jafra.analyzer.recordings.JfrEventSummarizer;
import io.jafra.analyzer.recordings.JfrReportGenerator;
import io.jafra.analyzer.recordings.RecordingCatalog;
import io.jafra.analyzer.recordings.ReportWindow;
import io.smallrye.common.annotation.Blocking;
import org.jboss.resteasy.reactive.RestPath;

@Path("/api/v1")
@Produces(MediaType.APPLICATION_JSON)
public class RecordingResource {
    @Inject
    RecordingCatalog catalog;

    @Inject
    JfrReportGenerator reports;

    @Inject
    JfrEventSummarizer summarizer;

    @GET
    @Path("/recordings")
    public RecordingCatalog.RecordingListResponse list(
            @QueryParam("namespace") String namespace,
            @QueryParam("pod") String pod,
            @QueryParam("container") String container) {
        return catalog.list(namespace, pod, container);
    }

    @GET
    @Path("/namespaces/{namespace}/pods/{pod}/containers/{container}")
    public RecordingCatalog.WorkloadRecordings workload(
            @RestPath String namespace, @RestPath String pod, @RestPath String container) {
        return catalog.requireWorkload(namespace, pod, container);
    }

    @GET
    @Blocking
    @Path("/namespaces/{namespace}/pods/{pod}/containers/{container}/report")
    public JfrReportGenerator.SummaryReport workloadReport(
            @RestPath String namespace,
            @RestPath String pod,
            @RestPath String container,
            @QueryParam("recording") String recording,
            @QueryParam("filter") String filter,
            @QueryParam("last") String last,
            @QueryParam("from") String from,
            @QueryParam("to") String to,
            @QueryParam("before") String before,
            @QueryParam("after") String after) {
        return withSelection(namespace, pod, container, recording, last, from, to, before, after, selection -> {
            try {
                return reports.report(
                        selection, reports.analyze(selection.chunkFiles(), filter, selection.from(), selection.to()));
            } catch (IOException error) {
                throw unavailable(selection, error.getMessage());
            }
        });
    }

    @GET
    @Blocking
    @Path("/namespaces/{namespace}/pods/{pod}/containers/{container}/summary")
    public JfrEventSummarizer.EventSummaryDocument workloadSummary(
            @RestPath String namespace,
            @RestPath String pod,
            @RestPath String container,
            @QueryParam("recording") String recording,
            @QueryParam("filter") String filter,
            @QueryParam("last") String last,
            @QueryParam("from") String from,
            @QueryParam("to") String to,
            @QueryParam("before") String before,
            @QueryParam("after") String after) {
        return withSelection(namespace, pod, container, recording, last, from, to, before, after, selection -> {
            try {
                return summarizer.summarize(selection, filter);
            } catch (IOException error) {
                throw unavailable(selection, error.getMessage());
            }
        });
    }

    @GET
    @Blocking
    @Path("/namespaces/{namespace}/pods/{pod}/containers/{container}/recordings/{filename}/report")
    public JfrReportGenerator.SummaryReport recordingReport(
            @RestPath String namespace,
            @RestPath String pod,
            @RestPath String container,
            @RestPath String filename,
            @QueryParam("filter") String filter) {
        return withSelection(namespace, pod, container, filename, null, null, null, null, null, selection -> {
            try {
                return reports.report(
                        selection, reports.analyze(selection.chunkFiles(), filter, selection.from(), selection.to()));
            } catch (IOException error) {
                throw unavailable(selection, error.getMessage());
            }
        });
    }

    @GET
    @Blocking
    @Path("/namespaces/{namespace}/pods/{pod}/containers/{container}/recordings/{filename}/summary")
    public JfrEventSummarizer.EventSummaryDocument recordingSummary(
            @RestPath String namespace,
            @RestPath String pod,
            @RestPath String container,
            @RestPath String filename,
            @QueryParam("filter") String filter) {
        return withSelection(namespace, pod, container, filename, null, null, null, null, null, selection -> {
            try {
                return summarizer.summarize(selection, filter);
            } catch (IOException error) {
                throw unavailable(selection, error.getMessage());
            }
        });
    }

    private <T> T withSelection(
            String namespace,
            String pod,
            String container,
            String recording,
            String last,
            String from,
            String to,
            String before,
            String after,
            Function<RecordingCatalog.WindowSelection, T> action) {
        ReportWindow window;
        try {
            window = ReportWindow.parse(Instant.now(), last, from, to, before, after);
        } catch (IllegalArgumentException error) {
            throw badRequest(error.getMessage());
        }
        if (window != null && recording != null && !recording.isBlank()) {
            throw badRequest("recording and time window cannot be combined");
        }
        RecordingCatalog.WindowSelection selected;
        try {
            if (window != null) {
                Optional<RecordingCatalog.WindowSelection> opened = catalog.openWindow(namespace, pod, container, window);
                if (opened.isEmpty()) {
                    throw missingWindow(namespace, pod, container, window);
                }
                selected = opened.get();
            } else {
                RecordingCatalog.IndexedRecording file = recording == null || recording.isBlank()
                        ? catalog.latestClosed(namespace, pod, container)
                        : catalog.requireRecording(namespace, pod, container, recording);
                selected = catalog.singleFile(file);
            }
        } catch (IllegalArgumentException error) {
            throw badRequest(error.getMessage());
        } catch (IOException error) {
            throw unavailable(namespace, pod, container, recording == null ? "window" : recording, error.getMessage());
        }
        return action.apply(selected);
    }

    private static WebApplicationException missingWindow(
            String namespace, String pod, String container, ReportWindow window) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("status", "not_found");
        body.put("namespace", namespace);
        body.put("pod", pod);
        body.put("container", container);
        if (window.from() != null) {
            body.put("from", window.from().toString());
        }
        if (window.to() != null) {
            body.put("to", window.to().toString());
        }
        body.put("error", "no recordings overlap the requested window");
        return new WebApplicationException(
                Response.status(404).type(MediaType.APPLICATION_JSON).entity(body).build());
    }

    private static WebApplicationException badRequest(String error) {
        return new WebApplicationException(
                Response.status(400)
                        .type(MediaType.APPLICATION_JSON)
                        .entity(Map.of("status", "bad_request", "error", error))
                        .build());
    }

    private static WebApplicationException unavailable(RecordingCatalog.WindowSelection selection, String error) {
        return unavailable(
                selection.key().namespace(),
                selection.key().pod(),
                selection.key().container(),
                String.join(",", selection.recordings().stream().map(item -> item.summary().filename()).toList()),
                error);
    }

    private static WebApplicationException unavailable(
            String namespace, String pod, String container, String recording, String error) {
        return new WebApplicationException(
                Response.status(422)
                        .type(MediaType.APPLICATION_JSON)
                        .entity(Map.of(
                                "status", "unavailable",
                                "namespace", namespace,
                                "pod", pod,
                                "container", container,
                                "recording", recording,
                                "error", error))
                        .build());
    }
}
