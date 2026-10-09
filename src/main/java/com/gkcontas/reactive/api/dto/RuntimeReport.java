package com.gkcontas.reactive.api.dto;

/**
 * What stack is actually running. The project exists to compare two of them
 * inside one jar, and "which one is up" is the first thing a measurement has to
 * establish - not least because WebFlux on Tomcat is a thing that starts
 * without complaining.
 */
public record RuntimeReport(
        String webApplicationType,
        String webServer,
        String transactionManager,
        int availableProcessors,
        int eventLoopThreads,
        int liveThreads,
        String currentThread) {
}
