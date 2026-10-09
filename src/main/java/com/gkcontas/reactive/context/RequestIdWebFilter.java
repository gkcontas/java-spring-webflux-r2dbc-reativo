package com.gkcontas.reactive.context;

import java.util.UUID;

import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.core.Ordered;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;

import io.micrometer.context.ContextRegistry;
import jakarta.annotation.PostConstruct;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.util.context.Context;

/**
 * Puts the request id where a reactive flow can still find it.
 * <p>
 * Two writes happen here, on purpose, and only one of them works downstream:
 * the {@code ThreadLocal}, which the handler will read back as null, and
 * {@code contextWrite}, which travels with the subscription. The exchange
 * attribute is a third option and the one WebFlux itself uses for this kind of
 * per-request state; the Context is the one that also reaches code with no
 * {@code ServerWebExchange} in hand.
 * <p>
 * {@code contextWrite} is written at the end of the chain and not the start,
 * because the Context propagates upstream from the subscriber. Placed first it
 * would decorate nothing.
 */
@Component
@ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.REACTIVE)
public class RequestIdWebFilter implements WebFilter, Ordered {

    public static final String HEADER = "X-Request-Id";
    public static final String FILTER_THREAD_ATTRIBUTE = "requestIdFilterThread";

    /**
     * Registers the accessor and turns the hook on from the application's own
     * wiring, not from {@code main}. A hook enabled in {@code main} is absent in
     * every {@code @SpringBootTest}, which does not call it - so the behaviour
     * under test would not be the behaviour that ships.
     */
    @PostConstruct
    void enableContextPropagation() {
        ContextRegistry.getInstance().registerThreadLocalAccessor(new MdcRequestIdAccessor());
        Hooks.enableAutomaticContextPropagation();
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        String requestId = exchange.getRequest().getHeaders().getFirst(HEADER);
        if (requestId == null || requestId.isBlank()) {
            requestId = UUID.randomUUID().toString();
        }
        String id = requestId;

        RequestId.THREAD_LOCAL.set(id);
        exchange.getAttributes().put(FILTER_THREAD_ATTRIBUTE, Thread.currentThread().getName());
        exchange.getResponse().getHeaders().set(HEADER, id);

        return chain.filter(exchange)
                .contextWrite(Context.of(RequestId.KEY, id))
                // The filter thread is reused for other requests, so leaving the
                // value behind would hand a later request someone else's id -
                // the other half of the ThreadLocal problem, and the half that
                // produces wrong data rather than missing data.
                .doFinally(signal -> RequestId.THREAD_LOCAL.remove());
    }

    @Override
    public int getOrder() {
        return Ordered.HIGHEST_PRECEDENCE;
    }
}
