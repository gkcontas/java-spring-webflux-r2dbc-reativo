package com.gkcontas.reactive.api.dto;

import java.math.BigDecimal;
import java.time.OffsetDateTime;

import com.gkcontas.reactive.domain.Reading;

public record ReadingResponse(
        Long id,
        Long stationId,
        OffsetDateTime recordedAt,
        BigDecimal temperatureC,
        BigDecimal humidityPct) {

    public static ReadingResponse from(Reading reading) {
        return new ReadingResponse(
                reading.getId(),
                reading.getStationId(),
                reading.getRecordedAt(),
                reading.getTemperatureC(),
                reading.getHumidityPct());
    }
}
