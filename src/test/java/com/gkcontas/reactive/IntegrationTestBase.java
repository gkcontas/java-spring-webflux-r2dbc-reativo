package com.gkcontas.reactive;

import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;

/**
 * One PostgreSQL container for the whole suite, started from a static
 * initialiser rather than by {@code @Testcontainers}: Spring caches application
 * contexts across test classes, so a per-class container would be torn down
 * while a cached context still pointed at it.
 * <p>
 * Both drivers are pointed at the same container. The R2DBC URL has to be built
 * by hand - Testcontainers only hands out a JDBC one - and Flyway needs the JDBC
 * side anyway, so the two coordinates are registered together and never drift.
 */
public abstract class IntegrationTestBase {

    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("telemetry")
            .withUsername("telemetry")
            .withPassword("telemetry");

    static {
        POSTGRES.start();
    }

    @DynamicPropertySource
    static void dataSources(DynamicPropertyRegistry registry) {
        registry.add("spring.r2dbc.url", () -> "r2dbc:postgresql://%s:%d/%s".formatted(
                POSTGRES.getHost(),
                POSTGRES.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT),
                POSTGRES.getDatabaseName()));
        registry.add("spring.r2dbc.username", POSTGRES::getUsername);
        registry.add("spring.r2dbc.password", POSTGRES::getPassword);

        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
    }
}
