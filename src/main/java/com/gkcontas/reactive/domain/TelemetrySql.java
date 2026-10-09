package com.gkcontas.reactive.domain;

/**
 * The SQL both stacks run, in one place.
 * <p>
 * Keeping the statements here is what makes the comparison honest: a reactive
 * endpoint that looks faster because somebody also rewrote its query is not
 * evidence about the stack. The placeholders differ - R2DBC uses {@code $1},
 * JDBC uses {@code ?} - so each statement is written once and the index form is
 * produced from it.
 */
public final class TelemetrySql {

    public static final String FIND_ALL_STATIONS = """
            SELECT id, code, name, region, active
              FROM station
             ORDER BY code
            """;

    public static final String FIND_STATION_BY_CODE = """
            SELECT id, code, name, region, active
              FROM station
             WHERE code = $1
            """;

    public static final String STREAM_READINGS_BY_STATION = """
            SELECT r.id, r.station_id, r.recorded_at, r.temperature_c, r.humidity_pct
              FROM reading r
              JOIN station s ON s.id = r.station_id
             WHERE s.code = $1
             ORDER BY r.recorded_at
             LIMIT $2
            """;

    public static final String SUMMARISE_STATION = """
            SELECT s.code                 AS station_code,
                   COUNT(r.id)            AS readings,
                   MIN(r.temperature_c)   AS min_temperature_c,
                   MAX(r.temperature_c)   AS max_temperature_c,
                   ROUND(AVG(r.temperature_c), 2) AS avg_temperature_c
              FROM station s
              LEFT JOIN reading r ON r.station_id = s.id
             WHERE s.code = $1
             GROUP BY s.code
            """;

    public static final String INSERT_READING = """
            INSERT INTO reading (station_id, recorded_at, temperature_c, humidity_pct)
            VALUES ($1, $2, $3, $4)
            RETURNING id
            """;

    /** Occupies a connection for the given number of seconds and nothing else. */
    public static final String SLEEP_IN_DATABASE = """
            SELECT pg_sleep($1::double precision)
            """;

    public static final String BULK_INSERT_READINGS = """
            INSERT INTO reading (station_id, recorded_at, temperature_c, humidity_pct)
            SELECT s.id,
                   TIMESTAMPTZ '2026-06-01 00:00:00+00' + (g * INTERVAL '1 minute'),
                   15 + (g % 2500) / 100.0,
                   40 + (g % 5000) / 100.0
              FROM station s
             CROSS JOIN generate_series(0, $1::bigint - 1) AS g
            """;

    public static final String COUNT_READINGS = """
            SELECT COUNT(*) FROM reading
            """;

    private TelemetrySql() {
    }

    /**
     * Rewrites {@code $1 $2 ...} as {@code ?}, so the JDBC side runs the same
     * statement without a second copy of it drifting out of step.
     */
    public static String positional(String sql) {
        return sql.replaceAll("\\$\\d+", "?");
    }
}
