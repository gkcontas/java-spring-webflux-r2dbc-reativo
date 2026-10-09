package com.gkcontas.reactive;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;

import com.gkcontas.reactive.api.dto.ErrorResponse;
import com.gkcontas.reactive.api.dto.NewReadingRequest;
import com.gkcontas.reactive.api.dto.ReadingResponse;
import com.gkcontas.reactive.api.dto.RequestIdReport;
import com.gkcontas.reactive.api.dto.RuntimeReport;
import com.gkcontas.reactive.api.dto.StationResponse;
import com.gkcontas.reactive.api.dto.SummaryResponse;
import com.gkcontas.reactive.context.RequestIdWebFilter;

/**
 * The same API on the blocking stack: Tomcat, MVC, JDBC, the same PostgreSQL
 * container. What this suite is for is the claim that the two contracts are the
 * same - if the comparison is to mean anything, a client must not be able to
 * tell which one it is talking to, except where the difference is the point.
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "spring.main.web-application-type=servlet")
class BlockingStackIntegrationTest extends IntegrationTestBase {

    @Autowired
    TestRestTemplate rest;

    @Test
    @DisplayName("the stack under test is Tomcat with a JDBC transaction manager")
    void runsOnTomcatWithAJdbcTransactionManager() {
        RuntimeReport report = rest.getForObject("/runtime", RuntimeReport.class);

        assertThat(report).isNotNull();
        assertThat(report.webApplicationType()).isEqualTo("SERVLET");
        assertThat(report.webServer()).isEqualTo("TomcatWebServer");
        assertThat(report.transactionManager()).isEqualTo("JdbcTransactionManager");
        // eventLoopThreads is not asserted here. It counts threads named like a
        // Netty loop across the whole JVM, and this JVM also runs the reactive
        // suite and a reactor-netty HTTP client - both of which name their
        // threads the same way. The number is the one that matters in a running
        // jar and noise in a shared test JVM.
    }

    @Test
    @DisplayName("stations come back from JDBC, same payload as the reactive stack")
    void listsStations() {
        ResponseEntity<List<StationResponse>> response = rest.exchange(
                "/stations", HttpMethod.GET, null,
                new ParameterizedTypeReference<List<StationResponse>>() {
                });

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).hasSize(8);
        assertThat(response.getBody()).extracting(StationResponse::code).isSorted();
    }

    @Test
    @DisplayName("an unknown code is the same 404 with the same error contract")
    void unknownStationIsNotFound() {
        ResponseEntity<ErrorResponse> response =
                rest.getForEntity("/stations/NOPE-99", ErrorResponse.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().error()).isEqualTo("station_not_found");
    }

    @Test
    @DisplayName("validation gives the same 400 payload, from a different exception type")
    void validationFailsWithTheSameContract() {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        ResponseEntity<ErrorResponse> response = rest.exchange("/readings", HttpMethod.POST,
                new HttpEntity<>("""
                        {"stationCode": "", "recordedAt": null,
                         "temperatureC": 900, "humidityPct": 50}
                        """, headers),
                ErrorResponse.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().error())
                .as("MethodArgumentNotValidException here, WebExchangeBindException "
                        + "there, one contract")
                .isEqualTo("validation_failed");
        assertThat(response.getBody().details())
                .anyMatch(detail -> detail.contains("stationCode must not be blank"));
    }

    @Test
    @DisplayName("a reading is written through the JDBC transaction")
    void recordsAReading() {
        ResponseEntity<ReadingResponse> response = rest.postForEntity("/readings",
                new NewReadingRequest("SPO-01",
                        OffsetDateTime.of(2026, 7, 2, 12, 0, 0, 0, ZoneOffset.UTC),
                        new BigDecimal("21.00"), new BigDecimal("72.00")),
                ReadingResponse.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().id()).isPositive();
    }

    @Test
    @DisplayName("a thread-bound transaction rolls back the same way")
    void blockingTransactionRollsBack() {
        long before = rest.getForObject("/admin/readings/count", Long.class);

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        ResponseEntity<String> response = rest.exchange("/readings/failing", HttpMethod.POST,
                new HttpEntity<>(new NewReadingRequest("RIO-01",
                        OffsetDateTime.of(2026, 7, 4, 12, 0, 0, 0, ZoneOffset.UTC),
                        new BigDecimal("19.00"), new BigDecimal("80.00")), headers),
                String.class);

        assertThat(response.getStatusCode().is5xxServerError()).isTrue();
        assertThat(rest.getForObject("/admin/readings/count", Long.class)).isEqualTo(before);
    }

    @Test
    @DisplayName("the cursored export answers NDJSON, line per row - JDBC streams too")
    void cursoredExportStreamsNdjson() {
        ResponseEntity<String> response = rest.getForEntity(
                "/readings/stream?stationCode=BSB-01&limit=25&fetchSize=5", String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getHeaders().getContentType())
                .isEqualTo(MediaType.APPLICATION_NDJSON);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().lines().count()).isEqualTo(25);
    }

    @Test
    @DisplayName("/readings/all is the materialised version: one JSON array")
    void materialisedExportAnswersAnArray() {
        ResponseEntity<String> response = rest.getForEntity(
                "/readings/all?stationCode=BSB-01&limit=25", String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().trim()).startsWith("[").endsWith("]");
    }

    @Test
    @DisplayName("the fan-out is sequential, and that is the whole cost of having no operator")
    void fanOutIsSequential() {
        SummaryResponse summary = rest.getForObject(
                "/stations/summary?codes=BSB-01,BSB-02,SPO-01,SPO-02,RIO-01,POA-01",
                SummaryResponse.class);

        assertThat(summary).isNotNull();
        assertThat(summary.mode()).isEqualTo("SEQUENTIAL");
        assertThat(summary.summaries()).hasSize(6);
        assertThat(summary.elapsedMs())
                .as("six legs of 100 ms, one after another - no concurrency "
                        + "without introducing an executor")
                .isGreaterThanOrEqualTo(600);
    }

    @Test
    @DisplayName("the ThreadLocal the reactive stack loses is right where it was left")
    void threadLocalSurvivesInTheServletStack() {
        HttpHeaders headers = new HttpHeaders();
        headers.set(RequestIdWebFilter.HEADER, "req-blocking");
        ResponseEntity<RequestIdReport> response = rest.exchange("/context/request-id",
                HttpMethod.GET, new HttpEntity<>(headers), RequestIdReport.class);

        RequestIdReport report = response.getBody();
        assertThat(report).isNotNull();
        assertThat(report.fromThreadLocal()).isEqualTo("req-blocking");
        assertThat(report.fromMdc()).isEqualTo("req-blocking");
        assertThat(report.handlerThread())
                .as("one thread from filter to handler, which is why the reactive "
                        + "version of this code passes review")
                .isEqualTo(report.filterThread());
    }

    @Test
    @DisplayName("the reactive-only trap endpoints do not exist here")
    void trapEndpointsAreReactiveOnly() {
        assertThat(rest.getForEntity("/trap/blocking?delayMs=1", String.class)
                .getStatusCode())
                .isEqualTo(HttpStatus.NOT_FOUND);
    }
}
