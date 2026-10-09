package com.gkcontas.reactive.api.reactive;

import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ServerWebExchange;

import com.gkcontas.reactive.api.dto.RequestIdReport;
import com.gkcontas.reactive.api.dto.WaitReport;
import com.gkcontas.reactive.context.RequestId;
import com.gkcontas.reactive.context.RequestIdWebFilter;
import com.gkcontas.reactive.downstream.SlowDownstream;

import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.util.context.ContextView;

/**
 * The ways a reactive application gets slower than the blocking one it
 * replaced, each reachable by HTTP so it can be measured rather than described.
 */
@RestController
@ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.REACTIVE)
public class TrapController {

    private final SlowDownstream downstream;

    public TrapController(SlowDownstream downstream) {
        this.downstream = downstream;
    }

    /**
     * One blocking call on the event loop. This is the whole failure mode of
     * WebFlux in one line, and it is what a driver with no reactive
     * implementation, a {@code RestTemplate} left behind in a migration, or a
     * logging appender writing to a slow disk does to an application.
     * <p>
     * There are twelve loop threads on this machine, so the ceiling is twelve
     * concurrent requests. Past it the requests are not queued by a thread pool
     * that was sized for queueing - they wait for a loop thread that is also
     * responsible for reading the socket, so new connections stall too.
     */
    @GetMapping("/trap/blocking")
    public Mono<WaitReport> blockingOnEventLoop(@RequestParam(defaultValue = "500") long delayMs) {
        long start = System.nanoTime();
        return Mono.fromSupplier(() -> {
            downstream.awaitBlocking(delayMs);
            return new WaitReport(delayMs, (System.nanoTime() - start) / 1_000_000,
                    Thread.currentThread().getName());
        });
    }

    /**
     * The same blocking call moved off the loop. It works, and it is the right
     * answer when a blocking dependency cannot be removed - but notice what it
     * is: a thread pool, capped by default at ten per core, in front of a
     * blocking call. Which is the architecture that was replaced, now with a
     * reactive API bolted on top.
     */
    @GetMapping("/trap/blocking-elastic")
    public Mono<WaitReport> blockingOnElastic(@RequestParam(defaultValue = "500") long delayMs) {
        long start = System.nanoTime();
        return Mono.fromSupplier(() -> {
            downstream.awaitBlocking(delayMs);
            return new WaitReport(delayMs, (System.nanoTime() - start) / 1_000_000,
                    Thread.currentThread().getName());
        }).subscribeOn(Schedulers.boundedElastic());
    }

    /**
     * {@code block()} inside a handler. Reactor refuses it on a non-blocking
     * thread, which is one of the few traps in this project that announces
     * itself - the response is a 500 and the message names the thread.
     */
    @GetMapping("/trap/block-call")
    public String blockOnEventLoop() {
        return downstream.awaitReactive(10).map(String::valueOf).block();
    }

    /**
     * Where the request id survived.
     * <p>
     * The hop to another scheduler is forced so the result is deterministic. In
     * a real flow it is not forced - it comes from the database driver, an HTTP
     * client or any operator with a scheduler of its own, which is why the
     * {@code ThreadLocal} version of this code can pass a smoke test and fail
     * under load.
     */
    @GetMapping("/context/request-id")
    public Mono<RequestIdReport> requestId(ServerWebExchange exchange) {
        return Mono.deferContextual(Mono::just)
                .publishOn(Schedulers.parallel())
                .map(context -> new RequestIdReport(
                        fromContext(context),
                        RequestId.fromThreadLocal(),
                        RequestId.fromMdc(),
                        exchange.getAttribute(RequestIdWebFilter.FILTER_THREAD_ATTRIBUTE),
                        Thread.currentThread().getName()));
    }

    private static String fromContext(ContextView context) {
        return context.hasKey(RequestId.KEY) ? context.get(RequestId.KEY) : null;
    }
}
