package com.gkcontas.reactive;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import reactor.blockhound.BlockHound;
import reactor.core.publisher.Mono;

/**
 * What BlockHound finds in the reactive stack itself.
 * <p>
 * The answer, on r2dbc-postgresql 1.0.7, is that the driver blocks the event
 * loop while authenticating: the SASL handler reads from a file to seed its
 * nonce, and that read is a blocking call on a thread Reactor marks as
 * non-blocking. It is brief and it happens once per connection, so a pool that
 * is warm never shows it - and a pool that refills under load shows it on the
 * loop thread serving a request.
 * <p>
 * The point is not that the driver is broken. It is that "fully reactive" is a
 * claim about an API, not a guarantee about a call graph, and the only way to
 * know is to instrument and look. Which is also why this test lives in its own
 * JVM: once installed, BlockHound applies to everything.
 */
class R2dbcDriverBlockHoundTest {

    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("telemetry")
            .withUsername("telemetry")
            .withPassword("telemetry");

    static {
        POSTGRES.start();
    }

    @BeforeAll
    static void install() {
        BlockHound.install();
    }

    @Test
    @DisplayName("opening an R2DBC connection trips BlockHound inside the driver")
    void theDriverBlocksTheEventLoopWhileAuthenticating() {
        ConnectionFactory factory = ConnectionFactories.get(ConnectionFactoryOptions.parse(
                "r2dbc:postgresql://%s:%s@%s:%d/%s".formatted(
                        POSTGRES.getUsername(),
                        POSTGRES.getPassword(),
                        POSTGRES.getHost(),
                        POSTGRES.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT),
                        POSTGRES.getDatabaseName())));

        AtomicReference<Throwable> failure = new AtomicReference<>();
        Connection connection = Mono.from(factory.create())
                .doOnError(failure::set)
                .onErrorResume(error -> Mono.empty())
                .block(Duration.ofSeconds(30));

        if (connection != null) {
            Mono.from(connection.close()).block(Duration.ofSeconds(10));
        }

        // Either the connection failed with a blocking-call report, or it
        // succeeded - in which case this version of the driver no longer blocks
        // and the finding above is out of date. Both outcomes are information;
        // only a silent pass would not be.
        assertThat(failure.get())
                .as("""
                        r2dbc-postgresql 1.0.7 blocks the event loop while \
                        authenticating. If this assertion ever fails, the driver \
                        was fixed and the README note about it should go.""")
                .isNotNull();
        assertThat(rootCauseOf(failure.get()).getClass().getSimpleName())
                .isEqualTo("BlockingOperationError");
    }

    private static Throwable rootCauseOf(Throwable error) {
        Throwable current = error;
        while (current.getCause() != null && current.getCause() != current) {
            current = current.getCause();
        }
        return current;
    }
}
