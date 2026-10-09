package com.gkcontas.reactive.api.dto;

/** Answer of every endpoint whose only job is to wait. */
public record WaitReport(long delayMs, long elapsedMs, String thread) {
}
