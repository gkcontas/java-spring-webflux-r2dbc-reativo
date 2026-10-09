package com.gkcontas.reactive.runtime;

import java.lang.management.ManagementFactory;
import java.util.Set;

import org.springframework.boot.web.context.WebServerApplicationContext;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;
import org.springframework.transaction.TransactionManager;

import com.gkcontas.reactive.api.dto.RuntimeReport;

/**
 * Says which stack is up, and how many threads it has.
 * <p>
 * Every number in the README depends on this being right. An application with
 * both starters on the classpath can start WebFlux on Tomcat, pick the wrong
 * transaction manager, or run with an event loop sized by a system property
 * somebody forgot - and all three produce measurements that look plausible.
 */
@Component
public class RuntimeInspector {

    private final ApplicationContext context;

    public RuntimeInspector(ApplicationContext context) {
        this.context = context;
    }

    public RuntimeReport report() {
        return new RuntimeReport(
                context.getClass().getSimpleName().contains("Reactive") ? "REACTIVE" : "SERVLET",
                webServer(),
                transactionManager(),
                Runtime.getRuntime().availableProcessors(),
                eventLoopThreads(),
                ManagementFactory.getThreadMXBean().getThreadCount(),
                Thread.currentThread().getName());
    }

    private String webServer() {
        if (context instanceof WebServerApplicationContext webServerContext
                && webServerContext.getWebServer() != null) {
            return webServerContext.getWebServer().getClass().getSimpleName();
        }
        return "none";
    }

    private String transactionManager() {
        return context.getBeanProvider(TransactionManager.class)
                .stream()
                .map(manager -> manager.getClass().getSimpleName())
                .findFirst()
                .orElse("none");
    }

    /**
     * Counts the Netty worker threads by name. Reactor Netty sizes its loop at
     * {@code max(availableProcessors, 4)} unless
     * {@code reactor.netty.ioWorkerCount} says otherwise - and that property is
     * the knob the README turns to show that the ceiling of a blocking handler
     * is the loop count and nothing else.
     * <p>
     * The count is JVM-wide and the server loop shares its naming with reactor
     * Netty's HTTP client, so it is the right number in a running application
     * and a meaningless one in a test JVM that has already started a client or
     * another context. The integration tests assert on the web server instead.
     */
    private int eventLoopThreads() {
        Set<Thread> threads = Thread.getAllStackTraces().keySet();
        return (int) threads.stream()
                .map(Thread::getName)
                .filter(name -> name.startsWith("reactor-http-nio-")
                        || name.startsWith("reactor-http-epoll-"))
                .count();
    }
}
