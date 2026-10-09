package com.gkcontas.reactive;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import reactor.blockhound.BlockHound;
import reactor.blockhound.BlockingOperationError;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

/**
 * The blocking call found before production finds it.
 * <p>
 * BlockHound instruments the JDK so that a known-blocking method throws when it
 * runs on a thread Reactor marks as non-blocking. It is a test-scope diagnostic
 * and needs {@code -XX:+AllowRedefinitionToAddDeleteMethods} to install, which
 * the build passes to every test JVM - without the flag the agent fails quietly
 * and a test like this one is a false pass. The last assertion here exists to
 * make that impossible: it checks that the instrumentation is live.
 */
class BlockHoundTest {

    @BeforeAll
    static void install() {
        BlockHound.install();
    }

    @Test
    @DisplayName("Thread.sleep on a parallel scheduler thread is refused")
    void blockingOnNonBlockingThreadThrows() {
        Mono<String> offender = Mono.fromCallable(() -> {
            Thread.sleep(10);
            return "done";
        }).subscribeOn(Schedulers.parallel());

        assertThatThrownBy(() -> offender.block(Duration.ofSeconds(5)))
                .rootCause()
                .isInstanceOf(BlockingOperationError.class)
                .hasMessageContaining("Blocking call");
    }

    @Test
    @DisplayName("the same call on boundedElastic is allowed, which is what the escape hatch is")
    void blockingOnElasticIsAllowed() {
        String result = Mono.fromCallable(() -> {
            Thread.sleep(10);
            return "done";
        }).subscribeOn(Schedulers.boundedElastic()).block(Duration.ofSeconds(5));

        assertThat(result)
                .as("boundedElastic exists to hold blocking calls, and BlockHound knows it")
                .isEqualTo("done");
    }

    @Test
    @DisplayName("the instrumentation is actually installed, so the test above means something")
    void instrumentationIsLive() {
        assertThatThrownBy(() -> Mono.fromRunnable(() -> {
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }).subscribeOn(Schedulers.single()).block(Duration.ofSeconds(5)))
                .as("if the agent had not installed, this would simply sleep and pass")
                .hasRootCauseInstanceOf(BlockingOperationError.class);
    }
}
