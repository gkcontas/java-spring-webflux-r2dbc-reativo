package com.gkcontas.reactive.composition;

import java.util.function.Function;

import org.reactivestreams.Publisher;

import reactor.core.publisher.Flux;

/**
 * Applies a {@link CompositionMode} to a {@code Flux}.
 * <p>
 * Pulled out of the service so the three operators can be exercised by a unit
 * test with no database and no web server: the difference between them is a
 * property of Reactor, and proving it should not need infrastructure.
 */
public final class Composition {

    private Composition() {
    }

    public static <T, R> Flux<R> apply(Flux<T> source, CompositionMode mode,
            Function<T, Publisher<R>> leg) {
        return switch (mode) {
            // The default concurrency of flatMap is 256. Worth knowing before
            // pointing it at a list of a thousand ids and a connection pool of
            // ten: the operator will happily ask for 256 connections at once and
            // the extra 246 subscriptions sit waiting for the pool to time out.
            case FLAT_MAP -> source.flatMap(leg);
            case CONCAT_MAP -> source.concatMap(leg);
            case FLAT_MAP_SEQUENTIAL -> source.flatMapSequential(leg);
        };
    }
}
