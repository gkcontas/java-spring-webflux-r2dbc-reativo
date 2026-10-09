INSERT INTO station (code, name, region, active) VALUES
    ('BSB-01', 'Brasilia Central',   'Centro-Oeste', TRUE),
    ('BSB-02', 'Brasilia North',     'Centro-Oeste', TRUE),
    ('SPO-01', 'Sao Paulo Downtown', 'Sudeste',      TRUE),
    ('SPO-02', 'Sao Paulo Airport',  'Sudeste',      TRUE),
    ('RIO-01', 'Rio Harbour',        'Sudeste',      TRUE),
    ('POA-01', 'Porto Alegre South', 'Sul',          TRUE),
    ('REC-01', 'Recife Coast',       'Nordeste',     TRUE),
    ('MAO-01', 'Manaus River',       'Norte',        FALSE);

-- A small, fixed seed: 250 readings per station. Enough for every test to have
-- something to read, small enough that a Testcontainers start is not dominated
-- by it. The volume used for the measurements is generated at runtime through
-- POST /admin/readings, so the heavy data never slows the test suite down.
INSERT INTO reading (station_id, recorded_at, temperature_c, humidity_pct)
SELECT s.id,
       TIMESTAMPTZ '2026-01-01 00:00:00+00' + (g * INTERVAL '1 hour'),
       18 + 12 * SIN(g / 24.0 * 2 * PI()) + (s.id % 5),
       50 + 20 * COS(g / 24.0 * 2 * PI())
FROM station s
CROSS JOIN generate_series(0, 249) AS g;
