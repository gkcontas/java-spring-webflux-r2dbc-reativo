package com.gkcontas.reactive.context;

import org.slf4j.MDC;

/**
 * The request id in its three homes.
 * <p>
 * The reactive {@code Context} key is a plain string because
 * {@code ThreadLocalAccessor} identifies itself by key, and the accessor is what
 * bridges Context to MDC.
 * <p>
 * {@link #THREAD_LOCAL} exists to be observed failing. It is written by the
 * filter and read by the handler, which in a reactive flow are different
 * threads - and the same two lines of code work perfectly in the servlet stack,
 * which is exactly why the mistake survives code review.
 */
public final class RequestId {

    public static final String KEY = "requestId";

    public static final ThreadLocal<String> THREAD_LOCAL = new ThreadLocal<>();

    private RequestId() {
    }

    public static String fromMdc() {
        return MDC.get(KEY);
    }

    public static String fromThreadLocal() {
        return THREAD_LOCAL.get();
    }
}
