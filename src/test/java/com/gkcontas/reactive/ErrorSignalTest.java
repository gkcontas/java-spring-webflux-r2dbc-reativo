package com.gkcontas.reactive;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.util.retry.Retry;

/**
 * Error handling as signals rather than as stack unwinding. These are the
 * mistakes a try/catch habit produces in a reactive codebase, each as an
 * assertion.
 */
class ErrorSignalTest {

    @Test
    @DisplayName("try/catch around assembly catches nothing, because nothing has run yet")
    void tryCatchAroundAssemblyIsUseless() {
        Mono<String> mono;
        boolean caught = false;
        try {
            // Assembly time. The lambda is stored, not invoked - there is no
            // subscriber yet, so there is nothing to fail.
            mono = Mono.just("x").map(value -> {
                throw new IllegalStateException("boom");
            });
        } catch (RuntimeException e) {
            caught = true;
            mono = Mono.empty();
        }

        assertThat(caught)
                .as("the exception does not exist until something subscribes")
                .isFalse();

        // Subscription time: now it happens, and it arrives as an onError signal
        // travelling downstream - not as a throw up the call stack.
        StepVerifier.create(mono)
                .expectErrorMessage("boom")
                .verify(Duration.ofSeconds(5));
    }

    @Test
    @DisplayName("doOnError observes and re-emits; it does not handle")
    void doOnErrorDoesNotHandle() {
        AtomicInteger observed = new AtomicInteger();

        StepVerifier.create(Mono.error(new IllegalStateException("boom"))
                .doOnError(error -> observed.incrementAndGet()))
                .expectError(IllegalStateException.class)
                .verify(Duration.ofSeconds(5));

        assertThat(observed.get()).isEqualTo(1);
    }

    @Test
    @DisplayName("an operator only sees errors from upstream of itself")
    void handlerPlacementMatters() {
        // onErrorReturn placed above the failing map never sees the failure.
        StepVerifier.create(Mono.just("ok")
                .onErrorReturn("recovered")
                .map(value -> {
                    throw new IllegalStateException("boom");
                }))
                .expectError(IllegalStateException.class)
                .verify(Duration.ofSeconds(5));

        // The same operator, one line lower.
        StepVerifier.create(Mono.just("ok")
                .map(value -> {
                    throw new IllegalStateException("boom");
                })
                .onErrorReturn("recovered"))
                .expectNext("recovered")
                .verifyComplete();
    }

    @Test
    @DisplayName("switchIfEmpty builds its fallback at assembly, so the error needs a defer")
    void switchIfEmptyNeedsDefer() {
        AtomicInteger built = new AtomicInteger();

        // The argument of switchIfEmpty is an expression, evaluated where it is
        // written. A service method that assembles this chain per request pays
        // for the exception - construction and stack trace - on every request,
        // including the ones that find what they were looking for.
        Mono<String> eager = Mono.just("found").switchIfEmpty(missing(built));
        assertThat(built.get())
                .as("built before anything subscribed")
                .isEqualTo(1);
        assertThat(eager.block()).isEqualTo("found");

        built.set(0);
        Mono<String> deferred = Mono.just("found")
                .switchIfEmpty(Mono.defer(() -> missing(built)));
        assertThat(deferred.block()).isEqualTo("found");
        assertThat(built.get())
                .as("the fallback was never needed, so it was never built")
                .isZero();
    }

    private static Mono<String> missing(AtomicInteger built) {
        built.incrementAndGet();
        return Mono.error(new IllegalStateException("missing"));
    }

    @Test
    @DisplayName("retryWhen replays the subscription, so a filter decides what deserves a retry")
    void retryNeedsAFilter() {
        AtomicInteger attempts = new AtomicInteger();

        Mono<String> alwaysNotFound = Mono.defer(() -> {
            attempts.incrementAndGet();
            return Mono.error(new IllegalArgumentException("not found"));
        });

        // Two things happen here, and the second one is the surprise. The call
        // is retried four times for a failure that will never succeed - and the
        // error that finally reaches downstream is a RetryExhaustedException,
        // not the original. An @ExceptionHandler written for the domain
        // exception stops matching, and a 404 silently becomes a 500.
        StepVerifier.create(alwaysNotFound.retryWhen(Retry.max(3)))
                .expectErrorMatches(Exceptions::isRetryExhausted)
                .verify(Duration.ofSeconds(5));

        assertThat(attempts.get())
                .as("four identical misses at four times the latency")
                .isEqualTo(4);

        // onRetryExhaustedThrow puts the original error back on the wire.
        attempts.set(0);
        StepVerifier.create(alwaysNotFound.retryWhen(Retry.max(3)
                .onRetryExhaustedThrow((spec, signal) -> signal.failure())))
                .expectError(IllegalArgumentException.class)
                .verify(Duration.ofSeconds(5));
        assertThat(attempts.get()).isEqualTo(4);

        // And a filter keeps the retry away from a failure that cannot change
        // its mind, which also leaves the error type intact.
        attempts.set(0);
        StepVerifier.create(alwaysNotFound.retryWhen(Retry.max(3)
                .filter(error -> !(error instanceof IllegalArgumentException))))
                .expectError(IllegalArgumentException.class)
                .verify(Duration.ofSeconds(5));

        assertThat(attempts.get())
                .as("one attempt, original type preserved")
                .isEqualTo(1);
    }

    @Test
    @DisplayName("timeout cancels the subscription instead of waiting for it")
    void timeoutCancelsUpstream() {
        AtomicInteger cancelled = new AtomicInteger();

        StepVerifier.create(Mono.delay(Duration.ofSeconds(10))
                .doOnCancel(cancelled::incrementAndGet)
                .timeout(Duration.ofMillis(50)))
                .expectError(java.util.concurrent.TimeoutException.class)
                .verify(Duration.ofSeconds(5));

        assertThat(cancelled.get())
                .as("the slow work is cancelled, which a blocking timeout cannot do")
                .isEqualTo(1);
    }

    @Test
    @DisplayName("one failing leg of a flatMap kills the whole fan-out unless it is contained")
    void oneLegCanKillTheFanOut() {
        Flux<Integer> legs = Flux.just(1, 2, 3)
                .flatMap(value -> value == 2
                        ? Mono.error(new IllegalStateException("leg 2 failed"))
                        : Mono.just(value));

        assertThatThrownBy(() -> legs.collectList().block())
                .isInstanceOf(IllegalStateException.class);

        // Contained per leg, the fan-out survives and the caller knows what is
        // missing - the difference between a degraded answer and no answer.
        Flux<Integer> contained = Flux.just(1, 2, 3)
                .flatMap(value -> (value == 2
                        ? Mono.<Integer>error(new IllegalStateException("leg 2 failed"))
                        : Mono.just(value))
                        .onErrorResume(error -> Mono.empty()));

        assertThat(contained.collectList().block()).containsExactly(1, 3);
    }
}
