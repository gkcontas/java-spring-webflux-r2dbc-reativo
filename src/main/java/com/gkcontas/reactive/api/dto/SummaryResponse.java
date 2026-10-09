package com.gkcontas.reactive.api.dto;

import java.util.List;

/**
 * The fan-out result together with how it was composed and what it cost. The
 * operator is part of the answer on purpose: the same legs under
 * {@code flatMap}, {@code concatMap} and {@code flatMapSequential} differ in
 * both ordering and wall time, and the only way to make that concrete is to
 * report the two side by side.
 */
public record SummaryResponse(
        String mode,
        long elapsedMs,
        List<StationSummary> summaries) {
}
