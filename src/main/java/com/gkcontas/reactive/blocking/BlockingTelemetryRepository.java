package com.gkcontas.reactive.blocking;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import com.gkcontas.reactive.api.dto.StationSummary;
import com.gkcontas.reactive.domain.Reading;
import com.gkcontas.reactive.domain.Station;
import com.gkcontas.reactive.domain.TelemetrySql;

/**
 * The blocking half, on {@code JdbcTemplate} and the same statements from
 * {@link TelemetrySql}. Every method here returns a value; the caller's thread
 * is held until the database answers.
 */
@Repository
public class BlockingTelemetryRepository {

    private static final RowMapper<Station> STATION = (rs, rowNum) -> new Station(
            rs.getLong("id"),
            rs.getString("code"),
            rs.getString("name"),
            rs.getString("region"),
            rs.getBoolean("active"));

    private static final RowMapper<Reading> READING = (rs, rowNum) -> mapReading(rs);

    private static final RowMapper<StationSummary> SUMMARY = (rs, rowNum) -> new StationSummary(
            rs.getString("station_code"),
            rs.getLong("readings"),
            rs.getBigDecimal("min_temperature_c"),
            rs.getBigDecimal("max_temperature_c"),
            rs.getBigDecimal("avg_temperature_c"));

    private final JdbcTemplate jdbc;

    public BlockingTelemetryRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public List<Station> findAllStations() {
        return jdbc.query(TelemetrySql.positional(TelemetrySql.FIND_ALL_STATIONS), STATION);
    }

    public Optional<Station> findStationByCode(String code) {
        return jdbc.query(TelemetrySql.positional(TelemetrySql.FIND_STATION_BY_CODE), STATION, code)
                .stream()
                .findFirst();
    }

    /**
     * The naive export: the whole result set in a {@code List}.
     * <p>
     * This is the version the README measures against the streaming one. JDBC
     * can stream too - a fetch size plus a {@code RowCallbackHandler} and the
     * rows never pile up - but the shape the framework makes easiest is this
     * one, and it is the shape that fails at a million rows.
     */
    public List<Reading> findReadings(String stationCode, int limit) {
        return jdbc.query(
                TelemetrySql.positional(TelemetrySql.STREAM_READINGS_BY_STATION),
                READING, stationCode, limit);
    }

    /**
     * The same export with a cursor: a fetch size and a per-row callback, so the
     * driver keeps only a window in memory. The point of having both is that
     * streaming is not what separates the two stacks - the API that makes it the
     * default is.
     */
    public long streamReadings(String stationCode, int limit, int fetchSize,
            java.util.function.Consumer<Reading> consumer) {
        long[] counted = {0};
        jdbc.query(connection -> {
            var statement = connection.prepareStatement(
                    TelemetrySql.positional(TelemetrySql.STREAM_READINGS_BY_STATION));
            // Postgres only honours a fetch size inside a transaction with
            // autoCommit off; outside one the driver reads the whole result
            // regardless of what was asked for.
            statement.setFetchSize(fetchSize);
            statement.setString(1, stationCode);
            statement.setInt(2, limit);
            return statement;
        }, rs -> {
            consumer.accept(mapReading(rs));
            counted[0]++;
        });
        return counted[0];
    }

    public StationSummary summarise(String stationCode) {
        return jdbc.query(TelemetrySql.positional(TelemetrySql.SUMMARISE_STATION), SUMMARY, stationCode)
                .stream()
                .findFirst()
                .orElse(null);
    }

    public Long insertReading(long stationId, OffsetDateTime recordedAt,
            BigDecimal temperatureC, BigDecimal humidityPct) {
        return jdbc.queryForObject(
                TelemetrySql.positional(TelemetrySql.INSERT_READING),
                Long.class,
                stationId, recordedAt, temperatureC, humidityPct);
    }

    public void sleepInDatabase(long delayMs) {
        // pg_sleep returns void, so there is nothing to map - the call is here
        // for its effect on the connection, which it holds for the duration.
        jdbc.query(
                TelemetrySql.positional(TelemetrySql.SLEEP_IN_DATABASE),
                statement -> statement.setDouble(1, delayMs / 1000.0),
                (ResultSet rs) -> {
                });
    }

    public long bulkInsertReadings(long rowsPerStation) {
        return jdbc.update(
                TelemetrySql.positional(TelemetrySql.BULK_INSERT_READINGS),
                rowsPerStation);
    }

    public long countReadings() {
        Long count = jdbc.queryForObject(TelemetrySql.COUNT_READINGS, Long.class);
        return count == null ? 0 : count;
    }

    private static Reading mapReading(ResultSet rs) throws SQLException {
        return new Reading(
                rs.getLong("id"),
                rs.getLong("station_id"),
                rs.getObject("recorded_at", OffsetDateTime.class),
                rs.getBigDecimal("temperature_c"),
                rs.getBigDecimal("humidity_pct"));
    }
}
