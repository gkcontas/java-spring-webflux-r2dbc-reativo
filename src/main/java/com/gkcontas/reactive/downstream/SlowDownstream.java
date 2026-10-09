package com.gkcontas.reactive.downstream;

import java.time.Duration;

import org.springframework.stereotype.Component;

import reactor.core.publisher.Mono;

/**
 * The slow dependency every waiting endpoint calls.
 * <p>
 * Simulated in process, with a timer on the reactive side and
 * {@code Thread.sleep} on the blocking one. A real HTTP call would add a second
 * server's own thread model to every number, and the question here is what the
 * caller's stack does while it waits - not what a second service does.
 * <p>
 * The two forms are the comparison in miniature. {@link #awaitReactive} parks
 * nothing: the request is a callback on a timer and the carrier thread is free.
 * {@link #awaitBlocking} holds whichever thread called it for the whole delay.
 */
@Component
public class SlowDownstream {

    public Mono<Long> awaitReactive(long delayMs) {
        return Mono.delay(Duration.ofMillis(delayMs)).thenReturn(delayMs);
    }

    public long awaitBlocking(long delayMs) {
        try {
            Thread.sleep(delayMs);
            return delayMs;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while waiting on downstream", e);
        }
    }
}
