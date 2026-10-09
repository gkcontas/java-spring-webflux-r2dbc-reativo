package com.gkcontas.reactive.api.dto;

/**
 * Where the request id survived and where it did not.
 * <p>
 * {@code fromThreadLocal} is the one that matters: a filter that stores the id
 * in a {@code ThreadLocal} and a handler that reads it back are, in a reactive
 * flow, two different threads. The field comes back null, and the same code in
 * the servlet stack works - which is why this is the mistake that reaches
 * production.
 */
public record RequestIdReport(
        String fromReactorContext,
        String fromThreadLocal,
        String fromMdc,
        String filterThread,
        String handlerThread) {
}
