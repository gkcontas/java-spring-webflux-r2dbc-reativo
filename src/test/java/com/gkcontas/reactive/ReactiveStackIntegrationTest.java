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
import org.springframework.http.MediaType;
import org.springframework.test.web.reactive.server.WebTestClient;

import com.gkcontas.reactive.api.dto.ErrorResponse;
import com.gkcontas.reactive.api.dto.NewReadingRequest;
import com.gkcontas.reactive.api.dto.ReadingResponse;
import com.gkcontas.reactive.api.dto.RequestIdReport;
import com.gkcontas.reactive.api.dto.RuntimeReport;
import com.gkcontas.reactive.api.dto.StationResponse;
import com.gkcontas.reactive.api.dto.SummaryResponse;
import com.gkcontas.reactive.context.RequestIdWebFilter;

/**
 * The reactive stack end to end: Netty, WebFlux, R2DBC and a real PostgreSQL.
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "spring.main.web-application-type=reactive",
                // The streaming assertions need more than the seeded 250 rows
                // per station to be interesting, and the summary endpoint delays
                // each leg by 100 ms, so the client has to wait longer than its
                // default five seconds.
                "spring.r2dbc.pool.max-size=10"
        })
class ReactiveStackIntegrationTest extends IntegrationTestBase {

    @Autowired
    WebTestClient client;

    @Test
    @DisplayName("the stack under test is Netty with a reactive transaction manager")
    void runsOnNettyWithAReactiveTransactionManager() {
        RuntimeReport report = client.get().uri("/runtime")
                .exchange()
                .expectStatus().isOk()
                .expectBody(RuntimeReport.class)
                .returnResult()
                .getResponseBody();

        assertThat(report).isNotNull();
        assertThat(report.webApplicationType()).isEqualTo("REACTIVE");
        assertThat(report.webServer())
                .as("not TomcatWebServer - the whole point of the explicit factory")
                .isEqualTo("NettyWebServer");
        assertThat(report.transactionManager()).isEqualTo("R2dbcTransactionManager");
        assertThat(report.eventLoopThreads())
                .as("one loop thread per core, which is the ceiling a blocking "
                        + "handler would be limited to")
                .isGreaterThan(0);
    }

    @Test
    @DisplayName("stations come back from R2DBC, ordered by code")
    void listsStations() {
        List<StationResponse> stations = client.get().uri("/stations")
                .exchange()
                .expectStatus().isOk()
                .expectBodyList(StationResponse.class)
                .returnResult()
                .getResponseBody();

        assertThat(stations).hasSize(8);
        assertThat(stations).extracting(StationResponse::code).isSorted();
        assertThat(stations).extracting(StationResponse::code).contains("BSB-01", "MAO-01");
    }

    @Test
    @DisplayName("an unknown code is a 404 with the shared error contract")
    void unknownStationIsNotFound() {
        ErrorResponse error = client.get().uri("/stations/NOPE-99")
                .exchange()
                .expectStatus().isNotFound()
                .expectBody(ErrorResponse.class)
                .returnResult()
                .getResponseBody();

        assertThat(error).isNotNull();
        assertThat(error.error()).isEqualTo("station_not_found");
        assertThat(error.message()).contains("NOPE-99");
    }

    @Test
    @DisplayName("validation failures are 400 with English messages")
    void validationFailsWithExplicitMessages() {
        ErrorResponse error = client.post().uri("/readings")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue("""
                        {"stationCode": "", "recordedAt": null,
                         "temperatureC": 900, "humidityPct": 50}
                        """)
                .exchange()
                .expectStatus().isBadRequest()
                .expectBody(ErrorResponse.class)
                .returnResult()
                .getResponseBody();

        assertThat(error).isNotNull();
        assertThat(error.error()).isEqualTo("validation_failed");
        assertThat(error.details()).anyMatch(detail -> detail.contains("stationCode must not be blank"));
        assertThat(error.details()).anyMatch(detail -> detail.contains("temperatureC must be at most"));
        assertThat(error.details())
                .as("WebExchangeBindException reached the same handler MVC uses")
                .anyMatch(detail -> detail.contains("recordedAt must not be null"));
    }

    @Test
    @DisplayName("a reading is written through the reactive transaction and read back")
    void recordsAReading() {
        ReadingResponse created = client.post().uri("/readings")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(new NewReadingRequest("BSB-01",
                        OffsetDateTime.of(2026, 7, 1, 12, 0, 0, 0, ZoneOffset.UTC),
                        new BigDecimal("24.50"), new BigDecimal("61.00")))
                .exchange()
                .expectStatus().isCreated()
                .expectBody(ReadingResponse.class)
                .returnResult()
                .getResponseBody();

        assertThat(created).isNotNull();
        assertThat(created.id()).isPositive();
        assertThat(created.temperatureC()).isEqualByComparingTo("24.50");
    }

    @Test
    @DisplayName("a reactive transaction rolls back, and the row is not there")
    void reactiveTransactionRollsBack() {
        long before = count();

        client.post().uri("/readings/failing")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(new NewReadingRequest("RIO-01",
                        OffsetDateTime.of(2026, 7, 3, 12, 0, 0, 0, ZoneOffset.UTC),
                        new BigDecimal("19.00"), new BigDecimal("80.00")))
                .exchange()
                .expectStatus().is5xxServerError();

        assertThat(count())
                .as("the insert happened and the rollback undid it - which only "
                        + "works because the connection lives in the reactive Context")
                .isEqualTo(before);
    }

    private long count() {
        Long value = client.get().uri("/admin/readings/count")
                .exchange()
                .expectStatus().isOk()
                .expectBody(Long.class)
                .returnResult()
                .getResponseBody();
        return value == null ? -1 : value;
    }

    @Test
    @DisplayName("the NDJSON export streams one object per line, not a JSON array")
    void streamsReadingsAsNdjson() {
        String body = client.get()
                .uri("/readings/stream?stationCode=BSB-01&limit=25&fetchSize=5")
                .exchange()
                .expectStatus().isOk()
                .expectHeader().contentTypeCompatibleWith(MediaType.APPLICATION_NDJSON)
                .expectBody(String.class)
                .returnResult()
                .getResponseBody();

        assertThat(body).isNotNull();
        assertThat(body.lines().count()).isEqualTo(25);
        assertThat(body.trim())
                .as("an array would mean WebFlux collected the Flux and the fetch "
                        + "size bought nothing")
                .doesNotStartWith("[");
    }

    @Test
    @DisplayName("/readings/all materialises the same rows into a JSON array")
    void materialisedExportAnswersAnArray() {
        String body = client.get().uri("/readings/all?stationCode=BSB-01&limit=25")
                .exchange()
                .expectStatus().isOk()
                .expectBody(String.class)
                .returnResult()
                .getResponseBody();

        assertThat(body).isNotNull();
        assertThat(body.trim())
                .as("collectList gives up streaming, and a Flux in the signature "
                        + "does not stop that")
                .startsWith("[").endsWith("]");
    }

    @Test
    @DisplayName("an unknown station is a 404 before the stream starts, not an empty 200")
    void streamChecksTheStationFirst() {
        client.get().uri("/readings/stream?stationCode=NOPE-99&limit=10")
                .exchange()
                .expectStatus().isNotFound();
    }

    @Test
    @DisplayName("flatMap runs the fan-out concurrently; concatMap pays the sum of the legs")
    void fanOutCostDependsOnTheOperator() {
        String codes = "BSB-01,BSB-02,SPO-01,SPO-02,RIO-01,POA-01";

        SummaryResponse concurrent = summary(codes, "flatMap");
        SummaryResponse sequential = summary(codes, "concatMap");

        assertThat(concurrent.summaries()).hasSize(6);
        assertThat(sequential.summaries()).hasSize(6);
        assertThat(sequential.summaries())
                .as("concatMap preserves request order")
                .extracting(summary -> summary.stationCode())
                .containsExactly("BSB-01", "BSB-02", "SPO-01", "SPO-02", "RIO-01", "POA-01");
        assertThat(concurrent.elapsedMs())
                .as("six legs of 100 ms, together")
                .isLessThan(sequential.elapsedMs());
        assertThat(sequential.elapsedMs())
                .as("six legs of 100 ms, one after another")
                .isGreaterThanOrEqualTo(600);
    }

    @Test
    @DisplayName("flatMapSequential keeps the order of concatMap at the speed of flatMap")
    void flatMapSequentialGetsBoth() {
        String codes = "BSB-01,BSB-02,SPO-01,SPO-02,RIO-01,POA-01";

        SummaryResponse ordered = summary(codes, "flatMapSequential");

        assertThat(ordered.summaries())
                .extracting(summary -> summary.stationCode())
                .containsExactly("BSB-01", "BSB-02", "SPO-01", "SPO-02", "RIO-01", "POA-01");
        assertThat(ordered.elapsedMs()).isLessThan(600);
    }

    @Test
    @DisplayName("an unknown mode is rejected rather than silently defaulted")
    void unknownModeIsRejected() {
        client.get().uri("/stations/summary?codes=BSB-01&mode=parallel")
                .exchange()
                .expectStatus().isBadRequest();
    }

    @Test
    @DisplayName("the request id survives in the Context and in MDC, and is lost in the ThreadLocal")
    void requestIdSurvivesOnlyWhereItIsSupposedTo() {
        RequestIdReport report = client.get().uri("/context/request-id")
                .header(RequestIdWebFilter.HEADER, "req-integration")
                .exchange()
                .expectStatus().isOk()
                .expectHeader().valueEquals(RequestIdWebFilter.HEADER, "req-integration")
                .expectBody(RequestIdReport.class)
                .returnResult()
                .getResponseBody();

        assertThat(report).isNotNull();
        assertThat(report.fromReactorContext()).isEqualTo("req-integration");
        assertThat(report.fromMdc())
                .as("restored by the registered ThreadLocalAccessor")
                .isEqualTo("req-integration");
        assertThat(report.fromThreadLocal())
                .as("the filter wrote it on another thread")
                .isNull();
        assertThat(report.handlerThread()).isNotEqualTo(report.filterThread());
    }

    @Test
    @DisplayName("block() on an event loop thread is refused by Reactor itself")
    void blockOnEventLoopIsRefused() {
        client.get().uri("/trap/block-call")
                .exchange()
                .expectStatus().is5xxServerError();
    }

    @Test
    @DisplayName("the blocking trap answers correctly - it is only wrong under load")
    void blockingTrapIsCorrectJustSlow() {
        client.get().uri("/trap/blocking?delayMs=50")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.thread")
                .value(thread -> assertThat((String) thread)
                        .as("the request was served on an event loop thread, and "
                                + "that thread slept")
                        .startsWith("reactor-http"));
    }

    @Test
    @DisplayName("the escape hatch moves the same call off the loop")
    void elasticTrapRunsOffTheLoop() {
        client.get().uri("/trap/blocking-elastic?delayMs=50")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.thread")
                .value(thread -> assertThat((String) thread).startsWith("boundedElastic"));
    }

    private SummaryResponse summary(String codes, String mode) {
        return client.get().uri("/stations/summary?codes=%s&mode=%s".formatted(codes, mode))
                .exchange()
                .expectStatus().isOk()
                .expectBody(SummaryResponse.class)
                .returnResult()
                .getResponseBody();
    }
}
