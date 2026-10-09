package com.gkcontas.reactive.config;

import javax.sql.DataSource;

import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.zaxxer.hikari.HikariDataSource;

/**
 * Builds the JDBC {@code DataSource} by hand, because Spring Boot will not.
 * <p>
 * {@code DataSourceAutoConfiguration} carries
 * {@code @ConditionalOnMissingBean(type = "io.r2dbc.spi.ConnectionFactory")}:
 * the moment an R2DBC connection factory exists, the whole JDBC
 * auto-configuration backs off. That is a reasonable default for an application
 * that is purely reactive, and it has a consequence nobody announces - there is
 * no {@code DataSource}, so {@code FlywayAutoConfiguration} finds nothing to
 * migrate and does not run. The application starts, the endpoints answer, and
 * the schema was never created. Nothing in the log says so.
 * <p>
 * It also takes {@code JdbcTemplate} with it, which the blocking half of this
 * project needs.
 * <p>
 * The properties still come from {@code spring.datasource.*}; only the bean is
 * hand-made. {@code DataSourceProperties} has to be enabled here too, since the
 * auto-configuration that normally does it is the one that backed off.
 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(DataSourceProperties.class)
public class MigrationDataSourceConfig {

    @Bean
    @ConfigurationProperties("spring.datasource.hikari")
    public DataSource dataSource(DataSourceProperties properties) {
        return properties.initializeDataSourceBuilder()
                .type(HikariDataSource.class)
                .build();
    }
}
