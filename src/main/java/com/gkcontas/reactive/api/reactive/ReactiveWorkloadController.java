package com.gkcontas.reactive.api.reactive;

import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.gkcontas.reactive.api.dto.RuntimeReport;
import com.gkcontas.reactive.api.dto.WaitReport;
import com.gkcontas.reactive.downstream.SlowDownstream;
import com.gkcontas.reactive.reactive.ReactiveTelemetryService;
import com.gkcontas.reactive.runtime.RuntimeInspector;

import reactor.core.publisher.Mono;

/**
 * The endpoints that only wait, which is where the two stacks are supposed to
 * differ, plus the one that reports which stack is answering.
 */
@RestController
@ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.REACTIVE)
public class ReactiveWorkloadController {

    private final SlowDownstream downstream;
    private final ReactiveTelemetryService service;
    private final RuntimeInspector inspector;

    public ReactiveWorkloadController(SlowDownstream downstream,
            ReactiveTelemetryService service, RuntimeInspector inspector) {
        this.downstream = downstream;
        this.service = service;
        this.inspector = inspector;
    }

    /**
     * Waiting done right: a timer, no thread held. Twelve event loop threads can
     * carry an unbounded number of these at once, because none of them is
     * sitting on one.
     */
    @GetMapping("/work/downstream")
    public Mono<WaitReport> downstream(@RequestParam(defaultValue = "500") long delayMs) {
        long start = System.nanoTime();
        return downstream.awaitReactive(delayMs)
                .map(delay -> new WaitReport(
                        delay,
                        (System.nanoTime() - start) / 1_000_000,
                        Thread.currentThread().getName()));
    }

    /**
     * Waiting inside the database, which the thread model cannot help with: the
     * scarce resource is the connection, and the pool is capped at ten. Both
     * stacks converge here, and a benchmark that only measures this endpoint
     * concludes that reactive changes nothing.
     */
    @GetMapping("/work/db")
    public Mono<WaitReport> db(@RequestParam(defaultValue = "100") long delayMs) {
        return service.waitInDatabase(delayMs)
                .map(elapsed -> new WaitReport(delayMs, elapsed, Thread.currentThread().getName()));
    }

    @GetMapping("/runtime")
    public Mono<RuntimeReport> runtime() {
        return Mono.fromSupplier(inspector::report);
    }
}
