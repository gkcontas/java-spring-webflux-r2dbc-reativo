package com.gkcontas.reactive.api.reactive;

import java.util.List;

import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import com.gkcontas.reactive.api.StationCodes;
import com.gkcontas.reactive.api.dto.NewReadingRequest;
import com.gkcontas.reactive.api.dto.ReadingResponse;
import com.gkcontas.reactive.api.dto.SeedReport;
import com.gkcontas.reactive.api.dto.StationResponse;
import com.gkcontas.reactive.api.dto.SummaryResponse;
import com.gkcontas.reactive.composition.CompositionMode;
import com.gkcontas.reactive.reactive.ReactiveTelemetryService;

import jakarta.validation.Valid;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * The reactive side of the identical API. Return types are {@code Mono} and
 * {@code Flux}; nothing here subscribes, because the framework is the
 * subscriber. A controller that calls {@code subscribe()} itself has started
 * work the request no longer waits for, and a controller that calls
 * {@code block()} has given the event loop away.
 */
@RestController
@ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.REACTIVE)
public class ReactiveTelemetryController {

    private final ReactiveTelemetryService service;

    public ReactiveTelemetryController(ReactiveTelemetryService service) {
        this.service = service;
    }

    @GetMapping("/stations")
    public Flux<StationResponse> stations() {
        return service.listStations().map(StationResponse::from);
    }

    @GetMapping("/stations/summary")
    public Mono<SummaryResponse> summary(
            @RequestParam String codes,
            @RequestParam(defaultValue = "flatMap") String mode) {
        return service.summarise(StationCodes.split(codes), CompositionMode.parse(mode));
    }

    @GetMapping("/stations/{code}")
    public Mono<StationResponse> station(@PathVariable String code) {
        return service.findStation(code).map(StationResponse::from);
    }

    /**
     * The streaming export, as NDJSON. The media type is what makes it a stream:
     * with {@code application/json} WebFlux collects the whole {@code Flux} to
     * serialise one array, and the flow control below is wasted work.
     * <p>
     * {@code fetchSize} is a parameter and not a constant so the difference it
     * makes is measurable in one session: at zero r2dbc-postgresql reads the
     * whole result into memory before the first element reaches the client.
     */
    @GetMapping(value = "/readings/stream", produces = MediaType.APPLICATION_NDJSON_VALUE)
    public Flux<ReadingResponse> streamReadings(
            @RequestParam String stationCode,
            @RequestParam(defaultValue = "1000") int limit,
            @RequestParam(defaultValue = "500") int fetchSize) {
        return service.streamReadings(stationCode, limit, fetchSize);
    }

    /**
     * The same rows, materialised. {@code collectList} is where a reactive
     * application gives up the one guarantee the streaming version had: the
     * whole result is on the heap before a byte goes out, and a {@code Flux} in
     * the signature does not stop that.
     * <p>
     * It exists so the comparison in the README has both shapes on both stacks,
     * instead of comparing reactive-streaming with blocking-materialised and
     * crediting the stack for the difference.
     */
    @GetMapping("/readings/all")
    public Mono<List<ReadingResponse>> allReadings(
            @RequestParam String stationCode,
            @RequestParam(defaultValue = "1000") int limit) {
        return service.streamReadings(stationCode, limit, 500).collectList();
    }

    @PostMapping("/readings")
    @ResponseStatus(HttpStatus.CREATED)
    public Mono<ReadingResponse> record(@Valid @RequestBody NewReadingRequest request) {
        return service.record(request);
    }

    /**
     * Writes a reading and then fails, so the rollback of a reactive
     * transaction is something a test can observe rather than trust. The count
     * before and after has to be the same.
     */
    @PostMapping("/readings/failing")
    public Mono<Long> recordThenFail(@Valid @RequestBody NewReadingRequest request) {
        return service.recordThenFail(request);
    }

    @PostMapping("/admin/readings")
    public Mono<SeedReport> seed(@RequestParam(defaultValue = "10000") long rowsPerStation) {
        return service.seedReadings(rowsPerStation);
    }

    @GetMapping("/admin/readings/count")
    public Mono<Long> count() {
        return service.countReadings();
    }
}
