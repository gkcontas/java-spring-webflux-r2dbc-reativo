package com.gkcontas.reactive.reactive;

import java.math.BigDecimal;
import java.time.OffsetDateTime;

import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Repository;

import com.gkcontas.reactive.api.dto.StationSummary;
import com.gkcontas.reactive.domain.Reading;
import com.gkcontas.reactive.domain.Station;
import com.gkcontas.reactive.domain.TelemetrySql;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.Readable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * R2DBC two ways, for one reason: {@code fetchSize}.
 * <p>
 * Everything that participates in a transaction goes through
 * {@code DatabaseClient}, which takes the connection the reactive transaction
 * bound to the {@code Context} instead of opening its own. Doing that by hand -
 * {@code ConnectionFactoryUtils} plus {@code usingWhen} - is where the
 * reference counting of the transactional connection goes wrong, because a
 * {@code .next()} cancels upstream and the asynchronous cleanup then races the
 * commit.
 * <p>
 * {@code DatabaseClient} does not expose {@code fetchSize}, and without it
 * r2dbc-postgresql runs the query with no cursor: the server sends every row,
 * the driver's inbound buffer takes them as fast as the socket delivers them,
 * and a {@code Flux} of a million rows holds a million rows in memory.
 * Backpressure is honoured between the operators and ignored at the boundary
 * that matters. So the export - and only the export - goes one level down to
 * the SPI, where the fetch size can be a request parameter and the difference
 * is something the README measures instead of claims.
 */
@Repository
public class ReactiveTelemetryRepository {

    private final DatabaseClient client;
    private final ConnectionFactory connectionFactory;

    public ReactiveTelemetryRepository(DatabaseClient client, ConnectionFactory connectionFactory) {
        this.client = client;
        this.connectionFactory = connectionFactory;
    }

    public Flux<Station> findAllStations() {
        return client.sql(TelemetrySql.FIND_ALL_STATIONS)
                .map(ReactiveTelemetryRepository::mapStation)
                .all();
    }

    public Mono<Station> findStationByCode(String code) {
        return client.sql(TelemetrySql.FIND_STATION_BY_CODE)
                .bind(0, code)
                .map(ReactiveTelemetryRepository::mapStation)
                .one();
    }

    /**
     * The streaming read, on the SPI so the fetch size is a parameter.
     * <p>
     * The connection is taken straight from the pool and not from the
     * transaction, which is correct here and would be a bug anywhere else: an
     * export is read-only, runs for as long as the client takes to download it,
     * and has no business holding a transaction open that whole time.
     * <p>
     * {@code usingWhen} and not {@code using}, because the cleanup has to run on
     * cancel too. A client that abandons the download halfway has to give the
     * connection back, or the pool loses one connection per abandoned request.
     */
    public Flux<Reading> streamReadings(String stationCode, int limit, int fetchSize) {
        return Flux.usingWhen(
                connectionFactory.create(),
                connection -> {
                    io.r2dbc.spi.Statement statement = connection
                            .createStatement(TelemetrySql.STREAM_READINGS_BY_STATION)
                            .bind(0, stationCode)
                            .bind(1, limit);
                    if (fetchSize > 0) {
                        statement.fetchSize(fetchSize);
                    }
                    return Flux.from(statement.execute())
                            .flatMap(result -> result.map(ReactiveTelemetryRepository::mapReading));
                },
                Connection::close);
    }

    public Mono<StationSummary> summarise(String stationCode) {
        return client.sql(TelemetrySql.SUMMARISE_STATION)
                .bind(0, stationCode)
                .map(ReactiveTelemetryRepository::mapSummary)
                .one();
    }

    public Mono<Long> insertReading(long stationId, OffsetDateTime recordedAt,
            BigDecimal temperatureC, BigDecimal humidityPct) {
        return client.sql(TelemetrySql.INSERT_READING)
                .bind(0, stationId)
                .bind(1, recordedAt)
                .bind(2, temperatureC)
                .bind(3, humidityPct)
                .map(row -> row.get("id", Long.class))
                .one();
    }

    /**
     * Holds a connection from the pool for the given duration without consuming
     * CPU. The thread model has nothing to offer here: the scarce resource is
     * the connection, and both stacks hit the same ceiling.
     */
    public Mono<Long> sleepInDatabase(long delayMs) {
        long start = System.nanoTime();
        return client.sql(TelemetrySql.SLEEP_IN_DATABASE)
                .bind(0, delayMs / 1000.0)
                .fetch()
                .rowsUpdated()
                .then(Mono.fromSupplier(() -> (System.nanoTime() - start) / 1_000_000));
    }

    public Mono<Long> bulkInsertReadings(long rowsPerStation) {
        return client.sql(TelemetrySql.BULK_INSERT_READINGS)
                .bind(0, rowsPerStation)
                .fetch()
                .rowsUpdated();
    }

    public Mono<Long> countReadings() {
        return client.sql(TelemetrySql.COUNT_READINGS)
                .map(row -> row.get(0, Long.class))
                .one();
    }

    private static Station mapStation(Readable row) {
        return new Station(
                row.get("id", Long.class),
                row.get("code", String.class),
                row.get("name", String.class),
                row.get("region", String.class),
                Boolean.TRUE.equals(row.get("active", Boolean.class)));
    }

    private static Reading mapReading(Readable row) {
        return new Reading(
                row.get("id", Long.class),
                row.get("station_id", Long.class),
                row.get("recorded_at", OffsetDateTime.class),
                row.get("temperature_c", BigDecimal.class),
                row.get("humidity_pct", BigDecimal.class));
    }

    private static StationSummary mapSummary(Readable row) {
        return new StationSummary(
                row.get("station_code", String.class),
                row.get("readings", Long.class),
                row.get("min_temperature_c", BigDecimal.class),
                row.get("max_temperature_c", BigDecimal.class),
                row.get("avg_temperature_c", BigDecimal.class));
    }
}
