package com.gkcontas.reactive.config;

import javax.sql.DataSource;

import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.r2dbc.connection.R2dbcTransactionManager;
import org.springframework.transaction.ReactiveTransactionManager;

import io.r2dbc.spi.ConnectionFactory;

/**
 * Picks the transaction manager that matches the running stack, and marks it
 * primary.
 * <p>
 * An application with both R2DBC and JDBC on the classpath ends up with two
 * beans of type {@code TransactionManager}. {@code @Transactional} resolves its
 * manager by type, so with two candidates and no primary the first annotated
 * call fails with {@code NoUniqueBeanDefinitionException} - at runtime, on the
 * first write, not at startup. The alternative is to qualify every annotation
 * by bean name, which spreads the infrastructure choice across the whole
 * service layer.
 * <p>
 * A reactive transaction is not a thread-bound one. {@code R2dbcTransactionManager}
 * keeps the connection in the reactive {@code Context}, which is why
 * {@code @Transactional} on a method returning {@code Mono} works and the same
 * annotation on a method that returns a plain value inside a reactive flow does
 * nothing at all.
 * <p>
 * The conditions are on the web application type, which means a context started
 * with no web environment at all gets neither primary and would fail on the
 * first annotated write. That is deliberate rather than overlooked: the two
 * configurations are what this project compares, and both transaction managers
 * are exercised through HTTP, by the stack that owns them.
 */
@Configuration(proxyBeanMethods = false)
public class TransactionManagerConfig {

    @Bean
    @Primary
    @ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.REACTIVE)
    public ReactiveTransactionManager reactiveTransactionManager(ConnectionFactory connectionFactory) {
        return new R2dbcTransactionManager(connectionFactory);
    }

    @Bean
    @Primary
    @ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.SERVLET)
    public JdbcTransactionManager jdbcTransactionManager(DataSource dataSource) {
        return new JdbcTransactionManager(dataSource);
    }
}
