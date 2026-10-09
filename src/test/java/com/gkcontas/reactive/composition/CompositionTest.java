package com.gkcontas.reactive.composition;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * The three composition operators, with no database and no web server. The
 * difference between them belongs to Reactor, and proving it should not need
 * infrastructure.
 * <p>
 * Each leg takes 100 ms, and the legs are given in an order whose durations
 * differ, so completion order and request order are not the same list.
 */
class CompositionTest {

    private static final List<Integer> DELAYS_MS = List.of(300, 100, 200);

    private static Mono<Integer> leg(int delayMs) {
        return Mono.delay(Duration.ofMillis(delayMs)).thenReturn(delayMs);
    }

    @Test
    @DisplayName("flatMap runs the legs together and answers in completion order")
    void flatMapIsConcurrentAndUnordered() {
        long start = System.nanoTime();
        List<Integer> result = Composition
                .apply(Flux.fromIterable(DELAYS_MS), CompositionMode.FLAT_MAP, CompositionTest::leg)
                .collectList()
                .block();
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;

        assertThat(result).containsExactly(100, 200, 300);
        assertThat(elapsedMs)
                .as("the total is the slowest leg, not the sum")
                .isLessThan(600);
    }

    @Test
    @DisplayName("concatMap keeps the order by running one leg at a time")
    void concatMapIsOrderedAndSequential() {
        long start = System.nanoTime();
        List<Integer> result = Composition
                .apply(Flux.fromIterable(DELAYS_MS), CompositionMode.CONCAT_MAP, CompositionTest::leg)
                .collectList()
                .block();
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;

        assertThat(result).containsExactly(300, 100, 200);
        assertThat(elapsedMs)
                .as("the total is the sum of the legs - the cost of ordering this way")
                .isGreaterThanOrEqualTo(600);
    }

    @Test
    @DisplayName("flatMapSequential gets both: concurrent legs, request order")
    void flatMapSequentialIsConcurrentAndOrdered() {
        long start = System.nanoTime();
        List<Integer> result = Composition
                .apply(Flux.fromIterable(DELAYS_MS), CompositionMode.FLAT_MAP_SEQUENTIAL, CompositionTest::leg)
                .collectList()
                .block();
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;

        assertThat(result).containsExactly(300, 100, 200);
        assertThat(elapsedMs)
                .as("ordering is paid in buffered results, not in wall time")
                .isLessThan(600);
    }

    @Test
    @DisplayName("mode names are parsed leniently, and an unknown one is rejected")
    void parsesModeNames() {
        assertThat(CompositionMode.parse("flatMap")).isEqualTo(CompositionMode.FLAT_MAP);
        assertThat(CompositionMode.parse("concat-map")).isEqualTo(CompositionMode.CONCAT_MAP);
        assertThat(CompositionMode.parse("FLAT_MAP_SEQUENTIAL"))
                .isEqualTo(CompositionMode.FLAT_MAP_SEQUENTIAL);
        assertThat(CompositionMode.parse("flatMapSequential"))
                .isEqualTo(CompositionMode.FLAT_MAP_SEQUENTIAL);
        assertThat(CompositionMode.parse(null)).isEqualTo(CompositionMode.FLAT_MAP);

        org.assertj.core.api.Assertions
                .assertThatThrownBy(() -> CompositionMode.parse("parallel"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("flatMap");
    }
}
