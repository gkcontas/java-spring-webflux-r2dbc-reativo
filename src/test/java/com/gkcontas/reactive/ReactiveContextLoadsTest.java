package com.gkcontas.reactive;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;

@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "spring.main.web-application-type=reactive")
class ReactiveContextLoadsTest extends IntegrationTestBase {

    @LocalServerPort
    int port;

    @Test
    void reactiveStackStartsOnARealPort() {
        assertThat(port).isPositive();
    }
}
