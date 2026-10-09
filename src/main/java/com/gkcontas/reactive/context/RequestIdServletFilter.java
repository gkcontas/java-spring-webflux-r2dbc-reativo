package com.gkcontas.reactive.context;

import java.io.IOException;
import java.util.UUID;

import org.slf4j.MDC;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import jakarta.servlet.Filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

/**
 * The servlet equivalent, and the reason the reactive mistake is so easy to
 * make: here one thread serves the request from filter to handler, so MDC and a
 * {@code ThreadLocal} both just work. The {@code finally} is still required -
 * the thread goes back to a pool.
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
@ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.SERVLET)
public class RequestIdServletFilter implements Filter {

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        String requestId = ((HttpServletRequest) request).getHeader(RequestIdWebFilter.HEADER);
        if (requestId == null || requestId.isBlank()) {
            requestId = UUID.randomUUID().toString();
        }

        RequestId.THREAD_LOCAL.set(requestId);
        MDC.put(RequestId.KEY, requestId);
        ((HttpServletResponse) response).setHeader(RequestIdWebFilter.HEADER, requestId);
        request.setAttribute(RequestIdWebFilter.FILTER_THREAD_ATTRIBUTE, Thread.currentThread().getName());

        try {
            chain.doFilter(request, response);
        } finally {
            MDC.remove(RequestId.KEY);
            RequestId.THREAD_LOCAL.remove();
        }
    }
}
