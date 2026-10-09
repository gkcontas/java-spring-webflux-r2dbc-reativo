package com.gkcontas.reactive.context;

import org.slf4j.MDC;

import io.micrometer.context.ThreadLocalAccessor;

/**
 * Bridges the reactive {@code Context} entry to MDC.
 * <p>
 * This is the piece that makes a log line inside a reactive flow carry the
 * request id. {@code Hooks.enableAutomaticContextPropagation()} asks the
 * registry to restore registered ThreadLocals around every operator boundary;
 * without an accessor registered for this key there is nothing to restore, and
 * {@code %X{requestId}} is empty in every line the handler writes.
 * <p>
 * The cost is real and belongs in the README: the restore happens at each
 * boundary, on every signal.
 */
public class MdcRequestIdAccessor implements ThreadLocalAccessor<String> {

    @Override
    public Object key() {
        return RequestId.KEY;
    }

    @Override
    public String getValue() {
        return MDC.get(RequestId.KEY);
    }

    @Override
    public void setValue(String value) {
        MDC.put(RequestId.KEY, value);
    }

    @Override
    public void setValue() {
        MDC.remove(RequestId.KEY);
    }
}
