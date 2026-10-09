package com.gkcontas.reactive.api.dto;

import java.math.BigDecimal;
import java.time.OffsetDateTime;

import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

/**
 * Every message is explicit. Bean Validation resolves its defaults from the JVM
 * locale, which on this machine is pt-BR, and an English contract answering in
 * Portuguese is a contract that lies.
 */
public record NewReadingRequest(
        @NotBlank(message = "stationCode must not be blank")
        String stationCode,

        @NotNull(message = "recordedAt must not be null")
        OffsetDateTime recordedAt,

        @NotNull(message = "temperatureC must not be null")
        @DecimalMin(value = "-90.0", message = "temperatureC must be at least -90.0")
        @DecimalMax(value = "60.0", message = "temperatureC must be at most 60.0")
        BigDecimal temperatureC,

        @NotNull(message = "humidityPct must not be null")
        @DecimalMin(value = "0.0", message = "humidityPct must be at least 0.0")
        @DecimalMax(value = "100.0", message = "humidityPct must be at most 100.0")
        BigDecimal humidityPct) {
}
