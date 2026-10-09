package com.gkcontas.reactive.reactive;

import java.time.Duration;
import java.util.List;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.gkcontas.reactive.api.dto.NewReadingRequest;
import com.gkcontas.reactive.api.dto.ReadingResponse;
import com.gkcontas.reactive.api.dto.SeedReport;
import com.gkcontas.reactive.api.dto.StationSummary;
import com.gkcontas.reactive.api.dto.SummaryResponse;
import com.gkcontas.reactive.composition.Composition;
import com.gkcontas.reactive.composition.CompositionMode;
import com.gkcontas.reactive.domain.Reading;
import com.gkcontas.reactive.domain.Station;
import com.gkcontas.reactive.domain.StationNotFoundException;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

@Service
public class ReactiveTelemetryService {

    private final ReactiveTelemetryRepository repository;

    public ReactiveTelemetryService(ReactiveTelemetryRepository repository) {
        this.repository = repository;
    }

    public Flux<Station> listStations() {
        return repository.findAllStations();
    }

    /**
     * The reactive form of "or throw".
     * <p>
     * {@code switchIfEmpty} takes a {@code Mono}, and the argument is built when
     * the chain is assembled, not when the fallback is needed. Passing
     * {@code Mono.error(new StationNotFoundException(code))} directly constructs
     * the exception - stack trace included - on every single call, including the
     * ones that find the station. {@code Mono.defer} is what makes it lazy.
     */
    public Mono<Station> findStation(String code) {
        return repository.findStationByCode(code)
                .switchIfEmpty(Mono.defer(() -> Mono.error(new StationNotFoundException(code))));
    }

    /**
     * The streaming read, as a {@code Flux} that is never collected.
     * <p>
     * The station is checked first so an unknown code is a 404 and not an empty
     * stream with a 200. That ordering is the whole difference between an error
     * that a client can act on and one it has to guess at: once the first
     * element of a streaming response has been written, the status line is gone
     * and an error can only arrive as a truncated body.
     */
    public Flux<ReadingResponse> streamReadings(String stationCode, int limit, int fetchSize) {
        return findStation(stationCode)
                .flatMapMany(station -> repository.streamReadings(stationCode, limit, fetchSize))
                .map(ReadingResponse::from);
    }

    /**
     * Fan-out over the requested stations, composed by the chosen operator and
     * timed. One query per code on purpose: the point is the composition, and a
     * single {@code IN (...)} would have nothing to compose.
     */
    public Mono<SummaryResponse> summarise(List<String> codes, CompositionMode mode) {
        long start = System.nanoTime();
        return Composition.apply(Flux.fromIterable(codes), mode, this::summariseOne)
                .collectList()
                .map(summaries -> new SummaryResponse(
                        mode.name(),
                        (System.nanoTime() - start) / 1_000_000,
                        summaries));
    }

    private Mono<StationSummary> summariseOne(String code) {
        // A missing station yields nothing instead of killing the fan-out. Note
        // that onErrorResume would not help here - an empty result is not an
        // error signal, and the two need different operators.
        return repository.summarise(code)
                // The delay makes the operator difference visible with eight
                // stations instead of eight thousand. It stands in for the
                // latency of a real remote leg.
                .delayElement(Duration.ofMillis(100));
    }

    @Transactional
    public Mono<ReadingResponse> record(NewReadingRequest request) {
        return findStation(request.stationCode())
                .flatMap(station -> repository.insertReading(
                        station.getId(),
                        request.recordedAt(),
                        request.temperatureC(),
                        request.humidityPct())
                        .map(id -> ReadingResponse.from(new Reading(
                                id,
                                station.getId(),
                                request.recordedAt(),
                                request.temperatureC(),
                                request.humidityPct()))));
    }

    /**
     * Writes the reading and then fails, so a test can prove the reactive
     * transaction actually rolls back. {@code @Transactional} on a method
     * returning {@code Mono} is honoured through the reactive {@code Context};
     * the same annotation on a method that returns {@code void} and starts the
     * work with a bare {@code subscribe()} is not, and the write escapes.
     */
    @Transactional
    public Mono<Long> recordThenFail(NewReadingRequest request) {
        return findStation(request.stationCode())
                .flatMap(station -> repository.insertReading(
                        station.getId(),
                        request.recordedAt(),
                        request.temperatureC(),
                        request.humidityPct()))
                .flatMap(id -> Mono.<Long>error(
                        new IllegalStateException("failing after the insert on purpose")));
    }

    public Mono<Long> waitInDatabase(long delayMs) {
        return repository.sleepInDatabase(delayMs);
    }

    public Mono<SeedReport> seedReadings(long rowsPerStation) {
        long start = System.nanoTime();
        return repository.bulkInsertReadings(rowsPerStation)
                .flatMap(inserted -> repository.countReadings()
                        .map(total -> new SeedReport(
                                inserted,
                                total,
                                (System.nanoTime() - start) / 1_000_000)));
    }

    public Mono<Long> countReadings() {
        return repository.countReadings();
    }

    /**
     * Error handling as operators, which is the part of WebFlux that a
     * try/catch habit gets wrong. An exception thrown inside {@code map} does
     * not propagate up a stack - it becomes an {@code onError} signal travelling
     * downstream, so the only code that can see it is an operator placed below.
     * {@code doOnError} observes and re-emits; it does not handle.
     */
    public Mono<String> resilientLookup(String code) {
        return findStation(code)
                .map(Station::getName)
                .timeout(Duration.ofSeconds(2))
                .retryWhen(Retry.backoff(2, Duration.ofMillis(50))
                        // Without this filter the retry also replays a 404, and a
                        // client gets three identical misses at triple the
                        // latency. Retry belongs to the failures that can change
                        // their mind.
                        .filter(error -> !(error instanceof StationNotFoundException)))
                .onErrorResume(StationNotFoundException.class, error -> Mono.just("unknown"));
    }
}
