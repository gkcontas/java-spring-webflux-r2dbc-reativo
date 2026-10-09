-- Flyway runs over JDBC. There is no R2DBC implementation of Flyway or
-- Liquibase, so a reactive application that versions its schema keeps a
-- blocking driver for exactly this, at startup, and never uses it again.

CREATE TABLE station (
    id         BIGSERIAL    PRIMARY KEY,
    code       VARCHAR(16)  NOT NULL UNIQUE,
    name       VARCHAR(120) NOT NULL,
    region     VARCHAR(60)  NOT NULL,
    active     BOOLEAN      NOT NULL DEFAULT TRUE
);

CREATE TABLE reading (
    id             BIGSERIAL      PRIMARY KEY,
    station_id     BIGINT         NOT NULL REFERENCES station (id),
    recorded_at    TIMESTAMPTZ    NOT NULL,
    temperature_c  NUMERIC(5, 2)  NOT NULL,
    humidity_pct   NUMERIC(5, 2)  NOT NULL
);

-- The streaming export reads by station in recorded_at order. Without this the
-- export is a sort of the whole table and the measurement would be about the
-- sort, not about backpressure.
CREATE INDEX idx_reading_station_recorded ON reading (station_id, recorded_at);
