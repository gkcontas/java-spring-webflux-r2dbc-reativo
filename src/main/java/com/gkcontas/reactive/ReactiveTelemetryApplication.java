package com.gkcontas.reactive;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * One application, two web stacks. {@code spring.main.web-application-type}
 * decides which one starts: {@code reactive} (Netty + WebFlux + R2DBC) by
 * default, {@code servlet} (Tomcat + MVC + JDBC) under the {@code blocking}
 * profile. Everything below the controller - SQL, validation, error contract -
 * is shared, so a difference between the two runs can only come from the stack.
 */
@SpringBootApplication
public class ReactiveTelemetryApplication {

    public static void main(String[] args) {
        SpringApplication.run(ReactiveTelemetryApplication.class, args);
    }
}
