package com.gkcontas.reactive.api.dto;

/** Result of the bulk generator used to size the data for a measurement. */
public record SeedReport(long rowsInserted, long totalRows, long elapsedMs) {
}
