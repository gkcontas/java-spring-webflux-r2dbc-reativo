package com.gkcontas.reactive;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;

import reactor.core.Exceptions;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;

/**
 * Backpressure as a number, not as a word.
 * <p>
 * The claim being tested is narrow and worth stating precisely: a reactive
 * pipeline gives the consumer a way to say how much it wants, and a publisher
 * that respects it produces no more than that. Everything people usually mean by
 * "backpressure protects you" depends on the producer being able to slow down -
 * which a database cursor can and a firehose of sensor events cannot, and the
 * last test here is about what happens then.
 */
class BackpressureTest {

    @Test
    @DisplayName("a subscriber that asks for two rows gets two rows, and the source stops")
    void demandLimitsProduction() {
        AtomicLong produced = new AtomicLong();
        List<Integer> received = new ArrayList<>();

        Flux<Integer> source = Flux.range(1, 1_000).doOnNext(item -> produced.incrementAndGet());

        source.subscribe(new BaseSubscriber<>() {
            @Override
            protected void hookOnSubscribe(Subscription subscription) {
                request(2);
            }

            @Override
            protected void hookOnNext(Integer value) {
                received.add(value);
            }
        });

        assertThat(received).containsExactly(1, 2);
        assertThat(produced.get())
                .as("nothing is generated beyond what was asked for")
                .isEqualTo(2);
    }

    @Test
    @DisplayName("flatMap asks for 256 upstream before the first element is needed")
    void flatMapPrefetchesTwoHundredFiftySix() {
        List<Long> requests = new ArrayList<>();

        Flux.range(1, 10_000)
                .doOnRequest(requests::add)
                .flatMap(Mono::just)
                .take(1)
                .blockLast();

        assertThat(requests)
                .as("the default concurrency of flatMap, asked for upfront - which "
                        + "is how a flatMap over a thousand ids ends up wanting 256 "
                        + "connections from a pool of ten")
                .containsExactly(256L);
    }

    @Test
    @DisplayName("concatMap asks for one, which is the other half of its cost")
    void concatMapRequestsOneAtATime() {
        List<Long> requests = new ArrayList<>();

        Flux.range(1, 10_000)
                .doOnRequest(requests::add)
                .concatMap(Mono::just)
                .take(1)
                .blockLast();

        assertThat(requests)
                .as("no prefetch at all: the operator that keeps the order also "
                        + "refuses to look ahead")
                .containsExactly(1L);
    }

    @Test
    @DisplayName("limitRate turns one large request into many small ones")
    void limitRateReshapesDemand() {
        List<Long> requests = new ArrayList<>();

        Flux.range(1, 100)
                .doOnRequest(requests::add)
                .limitRate(10)
                .blockLast();

        assertThat(requests).isNotEmpty();
        assertThat(requests)
                .as("no single request exceeds the configured rate")
                .allMatch(request -> request <= 10);
    }

    @Test
    @DisplayName("a producer that cannot slow down overflows, and the strategy decides what is lost")
    void unboundedProducerNeedsAnOverflowStrategy() {
        // The hard case: the rows are already arriving and no amount of asking
        // politely will stop them. Backpressure then is not flow control, it is
        // a choice about what to drop - and onBackpressureBuffer with no bound
        // is the choice to drop the process.
        TestPublisher<Integer> firehose = TestPublisher.createNoncompliant(
                TestPublisher.Violation.REQUEST_OVERFLOW);

        StepVerifier.create(
                firehose.flux().onBackpressureBuffer(2), 0)
                .then(() -> firehose.next(1, 2, 3, 4))
                .thenRequest(2)
                .expectNext(1, 2)
                // Asserted through Exceptions.isOverflow rather than on the
                // message: the message is wording, the overflow is the contract.
                .expectErrorMatches(Exceptions::isOverflow)
                .verify(Duration.ofSeconds(5));
    }

    @Test
    @DisplayName("onBackpressureDrop keeps the process alive by losing data, and says which")
    void dropStrategyNamesWhatItLost() {
        List<Integer> dropped = new ArrayList<>();
        TestPublisher<Integer> firehose = TestPublisher.createNoncompliant(
                TestPublisher.Violation.REQUEST_OVERFLOW);

        StepVerifier.create(firehose.flux().onBackpressureDrop(dropped::add), 1)
                .then(() -> firehose.next(1, 2, 3))
                .expectNext(1)
                .then(firehose::complete)
                .verifyComplete();

        assertThat(dropped)
                .as("the data the consumer never saw, available to be counted or logged")
                .containsExactly(2, 3);
    }
}
