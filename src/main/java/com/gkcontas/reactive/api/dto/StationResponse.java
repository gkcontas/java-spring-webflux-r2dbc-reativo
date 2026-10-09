package com.gkcontas.reactive.api.dto;

import com.gkcontas.reactive.domain.Station;

public record StationResponse(Long id, String code, String name, String region, boolean active) {

    public static StationResponse from(Station station) {
        return new StationResponse(
                station.getId(),
                station.getCode(),
                station.getName(),
                station.getRegion(),
                station.isActive());
    }
}
