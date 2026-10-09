package com.gkcontas.reactive.domain;

import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * A weather station. Mapped for Spring Data R2DBC and read by hand on the JDBC
 * side, so both stacks carry the same type - the comparison stays about the
 * stack rather than about two different domain models.
 * <p>
 * There is no relation to {@link Reading} here, and that is not an omission:
 * Spring Data R2DBC is not an ORM. It has no lazy loading and no managed
 * associations, because resolving one would mean a blocking call inside a
 * getter. Joins are written as SQL and mapped explicitly.
 */
@Table("station")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class Station {

    @Id
    private Long id;

    private String code;

    private String name;

    private String region;

    private boolean active;
}
