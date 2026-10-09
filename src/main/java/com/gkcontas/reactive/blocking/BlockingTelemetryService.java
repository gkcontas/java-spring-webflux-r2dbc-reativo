package com.gkcontas.reactive.blocking;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.fasterxml.jackson.databind.ObjectMapper;

import com.gkcontas.reactive.api.dto.NewReadingRequest;
import com.gkcontas.reactive.api.dto.ReadingResponse;
import com.gkcontas.reactive.api.dto.SeedReport;
import com.gkcontas.reactive.api.dto.StationSummary;
import com.gkcontas.reactive.api.dto.SummaryResponse;
import com.gkcontas.reactive.domain.Reading;
import com.gkcontas.reactive.domain.Station;
import com.gkcontas.reactive.domain.StationNotFoundException;

/**
 * The same operations, written the way they are written without Reactor. Same
 * SQL, same validation, same error contract - so a difference measured between
 * the two endpoints belongs to the stack.
 */
@Service
public class BlockingTelemetryService {

    private final BlockingTelemetryRepository repository;
    private final ObjectMapper json;

    public BlockingTelemetryService(BlockingTelemetryRepository repository, ObjectMapper json) {
        this.repository = repository;
        this.json = json;
    }

    public List<Station> listStations() {
        return repository.findAllStations();
    }

    public Station findStation(String code) {
        return repository.findStationByCode(code)
                .orElseThrow(() -> new StationNotFoundException(code));
    }

    public List<ReadingResponse> findReadings(String stationCode, int limit) {
        findStation(stationCode);
        return repository.findReadings(stationCode, limit).stream()
                .map(ReadingResponse::from)
                .toList();
    }

    /**
     * The cursored export, written straight to the response.
     * <p>
     * {@code @Transactional} is not optional here: the Postgres JDBC driver only
     * honours a fetch size with autoCommit off, and without a transaction it
     * reads the whole result set no matter what fetch size was asked for - the
     * code looks like streaming and behaves like the materialised version.
     * <p>
     * The cost, stated rather than hidden: the transaction and its connection
     * stay open for as long as the client takes to read the body. A slow client
     * holds a connection, which is the same bill the reactive export pays.
     */
    @Transactional(readOnly = true)
    public long streamReadings(String stationCode, int limit, int fetchSize, OutputStream output) {
        findStation(stationCode);
        return repository.streamReadings(stationCode, limit, fetchSize, reading -> {
            try {
                output.write(json.writeValueAsBytes(ReadingResponse.from(reading)));
                output.write('\n');
            } catch (IOException e) {
                // The client went away mid-download. Wrapping it keeps the
                // cursor's callback signature clean; the transaction rolls back
                // and the connection goes home either way.
                throw new UncheckedIOException(e);
            }
        });
    }

    /**
     * The fan-out, sequentially. There is no composition operator to choose: the
     * loop is the only order there is, and parallelising it means introducing an
     * executor and deciding its size - the decision the reactive side does not
     * have to make, and the reason the sequential total is the sum of the legs.
     */
    public SummaryResponse summarise(List<String> codes) {
        long start = System.nanoTime();
        List<StationSummary> summaries = new ArrayList<>();
        for (String code : codes) {
            StationSummary summary = repository.summarise(code);
            if (summary != null) {
                summaries.add(summary);
            }
            sleep(100);
        }
        return new SummaryResponse(
                "SEQUENTIAL",
                (System.nanoTime() - start) / 1_000_000,
                summaries);
    }

    @Transactional
    public ReadingResponse record(NewReadingRequest request) {
        Station station = findStation(request.stationCode());
        Long id = repository.insertReading(
                station.getId(),
                request.recordedAt(),
                request.temperatureC(),
                request.humidityPct());
        return ReadingResponse.from(new Reading(
                id,
                station.getId(),
                request.recordedAt(),
                request.temperatureC(),
                request.humidityPct()));
    }

    @Transactional
    public void recordThenFail(NewReadingRequest request) {
        Station station = findStation(request.stationCode());
        repository.insertReading(
                station.getId(),
                request.recordedAt(),
                request.temperatureC(),
                request.humidityPct());
        throw new IllegalStateException("failing after the insert on purpose");
    }

    public long waitInDatabase(long delayMs) {
        long start = System.nanoTime();
        repository.sleepInDatabase(delayMs);
        return (System.nanoTime() - start) / 1_000_000;
    }

    public SeedReport seedReadings(long rowsPerStation) {
        long start = System.nanoTime();
        long inserted = repository.bulkInsertReadings(rowsPerStation);
        return new SeedReport(inserted, repository.countReadings(), (System.nanoTime() - start) / 1_000_000);
    }

    public long countReadings() {
        return repository.countReadings();
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while summarising", e);
        }
    }
}
