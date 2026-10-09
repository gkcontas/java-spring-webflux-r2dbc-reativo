package com.gkcontas.reactive.domain;

import java.math.BigDecimal;
import java.time.OffsetDateTime;

import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.Table;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/** One measurement from one station. */
@Table("reading")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class Reading {

    @Id
    private Long id;

    @Column("station_id")
    private Long stationId;

    @Column("recorded_at")
    private OffsetDateTime recordedAt;

    @Column("temperature_c")
    private BigDecimal temperatureC;

    @Column("humidity_pct")
    private BigDecimal humidityPct;
}
