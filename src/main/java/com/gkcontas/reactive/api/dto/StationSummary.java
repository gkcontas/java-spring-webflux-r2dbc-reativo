package com.gkcontas.reactive.api.dto;

import java.math.BigDecimal;

/** One leg of the fan-out: a per-station aggregate of its readings. */
public record StationSummary(
        String stationCode,
        long readings,
        BigDecimal minTemperatureC,
        BigDecimal maxTemperatureC,
        BigDecimal avgTemperatureC) {
}
