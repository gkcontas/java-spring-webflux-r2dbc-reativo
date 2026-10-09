package com.gkcontas.reactive.api.blocking;

import java.util.List;

import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import com.gkcontas.reactive.api.StationCodes;
import com.gkcontas.reactive.api.dto.NewReadingRequest;
import com.gkcontas.reactive.api.dto.ReadingResponse;
import com.gkcontas.reactive.api.dto.RequestIdReport;
import com.gkcontas.reactive.api.dto.RuntimeReport;
import com.gkcontas.reactive.api.dto.SeedReport;
import com.gkcontas.reactive.api.dto.StationResponse;
import com.gkcontas.reactive.api.dto.SummaryResponse;
import com.gkcontas.reactive.api.dto.WaitReport;
import com.gkcontas.reactive.blocking.BlockingTelemetryService;
import com.gkcontas.reactive.context.RequestId;
import com.gkcontas.reactive.context.RequestIdWebFilter;
import com.gkcontas.reactive.downstream.SlowDownstream;
import com.gkcontas.reactive.runtime.RuntimeInspector;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.Valid;

/**
 * The same routes, the same payloads, the same status codes - on Spring MVC.
 * The only difference a client can observe is the ordering of
 * {@code /stations/summary}, because the sequential version has no choice but
 * to answer in request order.
 */
@RestController
@ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.SERVLET)
public class BlockingTelemetryController {

    private final BlockingTelemetryService service;
    private final SlowDownstream downstream;
    private final RuntimeInspector inspector;

    public BlockingTelemetryController(BlockingTelemetryService service,
            SlowDownstream downstream, RuntimeInspector inspector) {
        this.service = service;
        this.downstream = downstream;
        this.inspector = inspector;
    }

    @GetMapping("/stations")
    public List<StationResponse> stations() {
        return service.listStations().stream().map(StationResponse::from).toList();
    }

    /**
     * No {@code mode} parameter, because there is no choice to make: the loop is
     * the only composition available without introducing an executor. A client
     * may still send one - MVC ignores query parameters it was not asked about -
     * and the answer always reports {@code SEQUENTIAL}.
     */
    @GetMapping("/stations/summary")
    public SummaryResponse summary(@RequestParam String codes) {
        return service.summarise(StationCodes.split(codes));
    }

    @GetMapping("/stations/{code}")
    public StationResponse station(@PathVariable String code) {
        return StationResponse.from(service.findStation(code));
    }

    /**
     * The cursored export, written straight to the response as NDJSON. JDBC
     * streams: a fetch size and a per-row callback, and the heap holds a window
     * rather than a result set. A reactive stack is not what buys that.
     * <p>
     * The return type is {@code StreamingResponseBody} and not a wildcard:
     * Spring MVC decides how to treat the body from the declared generic type,
     * so a {@code ResponseEntity<?>} holding a {@code StreamingResponseBody} is
     * handed to the message converters as if it were a POJO, and the response is
     * a 500 about a missing converter for a lambda.
     */
    @GetMapping("/readings/stream")
    public ResponseEntity<StreamingResponseBody> readings(
            @RequestParam String stationCode,
            @RequestParam(defaultValue = "1000") int limit,
            @RequestParam(defaultValue = "500") int fetchSize) {
        return ResponseEntity.ok()
                .contentType(MediaType.APPLICATION_NDJSON)
                .body(output -> service.streamReadings(stationCode, limit, fetchSize, output));
    }

    /**
     * The materialised export: the whole result set becomes a {@code List} and
     * only then is serialised. This is the shape the framework makes easiest,
     * and the one the README measures against the cursored version.
     */
    @GetMapping("/readings/all")
    public List<ReadingResponse> allReadings(
            @RequestParam String stationCode,
            @RequestParam(defaultValue = "1000") int limit) {
        return service.findReadings(stationCode, limit);
    }

    @PostMapping("/readings")
    @ResponseStatus(HttpStatus.CREATED)
    public ReadingResponse record(@Valid @RequestBody NewReadingRequest request) {
        return service.record(request);
    }

    /** The same demonstration on a thread-bound transaction. */
    @PostMapping("/readings/failing")
    public void recordThenFail(@Valid @RequestBody NewReadingRequest request) {
        service.recordThenFail(request);
    }

    @PostMapping("/admin/readings")
    public SeedReport seed(@RequestParam(defaultValue = "10000") long rowsPerStation) {
        return service.seedReadings(rowsPerStation);
    }

    @GetMapping("/admin/readings/count")
    public long count() {
        return service.countReadings();
    }

    /**
     * Waiting on the slow dependency with a thread held for the whole delay.
     * Below Tomcat's 200 threads this is indistinguishable from the reactive
     * version; above it, the requests queue.
     */
    @GetMapping("/work/downstream")
    public WaitReport downstream(@RequestParam(defaultValue = "500") long delayMs) {
        long start = System.nanoTime();
        downstream.awaitBlocking(delayMs);
        return new WaitReport(delayMs, (System.nanoTime() - start) / 1_000_000,
                Thread.currentThread().getName());
    }

    @GetMapping("/work/db")
    public WaitReport db(@RequestParam(defaultValue = "100") long delayMs) {
        return new WaitReport(delayMs, service.waitInDatabase(delayMs),
                Thread.currentThread().getName());
    }

    @GetMapping("/runtime")
    public RuntimeReport runtime() {
        return inspector.report();
    }

    /**
     * The control for the context experiment: one thread from filter to handler,
     * so the {@code ThreadLocal} the reactive stack loses is right where it was
     * left.
     */
    @GetMapping("/context/request-id")
    public RequestIdReport requestId(HttpServletRequest request) {
        return new RequestIdReport(
                null,
                RequestId.fromThreadLocal(),
                RequestId.fromMdc(),
                (String) request.getAttribute(RequestIdWebFilter.FILTER_THREAD_ATTRIBUTE),
                Thread.currentThread().getName());
    }
}
