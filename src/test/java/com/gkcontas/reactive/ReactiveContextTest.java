package com.gkcontas.reactive;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;

import com.gkcontas.reactive.context.MdcRequestIdAccessor;
import com.gkcontas.reactive.context.RequestId;

import io.micrometer.context.ContextRegistry;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.util.context.Context;

/**
 * Where per-request state lives in a reactive flow, and what happens to the
 * {@code ThreadLocal} that a servlet application keeps it in.
 */
class ReactiveContextTest {

    @Test
    @DisplayName("a ThreadLocal set before a thread hop is gone after it")
    void threadLocalDoesNotSurviveAHop() {
        RequestId.THREAD_LOCAL.set("req-1");
        try {
            AtomicReference<String> afterHop = new AtomicReference<>("untouched");

            Mono.just("x")
                    .publishOn(Schedulers.parallel())
                    .doOnNext(value -> afterHop.set(RequestId.fromThreadLocal()))
                    .block();

            assertThat(afterHop.get())
                    .as("the handler runs on another thread, and the value stayed behind")
                    .isNull();
        } finally {
            RequestId.THREAD_LOCAL.remove();
        }
    }

    @Test
    @DisplayName("the reactive Context travels with the subscription")
    void contextSurvivesAHop() {
        String seen = Mono.deferContextual(context -> Mono.just(context.get(RequestId.KEY)))
                .publishOn(Schedulers.parallel())
                .cast(String.class)
                .contextWrite(Context.of(RequestId.KEY, "req-2"))
                .block();

        assertThat(seen).isEqualTo("req-2");
    }

    @Test
    @DisplayName("contextWrite decorates upstream, so its position is not a style choice")
    void contextWritePropagatesUpstream() {
        // Written below the reader: the reader sees it.
        assertThat(Mono.deferContextual(context -> Mono.just(context.hasKey(RequestId.KEY)))
                .contextWrite(Context.of(RequestId.KEY, "req-3"))
                .block())
                .isTrue();

        // Written above the reader: the reader sees nothing, and the code looks
        // identical to anyone reading it top to bottom.
        assertThat(Mono.just("x")
                .contextWrite(Context.of(RequestId.KEY, "req-3"))
                .flatMap(value -> Mono.deferContextual(
                        context -> Mono.just(context.hasKey(RequestId.KEY))))
                .block())
                .isFalse();
    }

    @Test
    @DisplayName("with an accessor registered, the Context entry reaches MDC across the hop")
    void automaticPropagationFeedsMdc() {
        ContextRegistry.getInstance().registerThreadLocalAccessor(new MdcRequestIdAccessor());
        Hooks.enableAutomaticContextPropagation();
        try {
            AtomicReference<String> fromMdc = new AtomicReference<>();

            Mono.just("x")
                    .publishOn(Schedulers.parallel())
                    .doOnNext(value -> fromMdc.set(MDC.get(RequestId.KEY)))
                    .contextWrite(Context.of(RequestId.KEY, "req-4"))
                    .block();

            assertThat(fromMdc.get())
                    .as("this is what puts the request id in a log line")
                    .isEqualTo("req-4");
        } finally {
            Hooks.disableAutomaticContextPropagation();
            MDC.remove(RequestId.KEY);
        }
    }
}
