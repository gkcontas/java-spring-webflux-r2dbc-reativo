package com.gkcontas.reactive.api;

import java.util.List;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.BindingResult;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.bind.support.WebExchangeBindException;

import com.gkcontas.reactive.api.dto.ErrorResponse;
import com.gkcontas.reactive.domain.StationNotFoundException;

/**
 * One advice, both stacks - and two handlers, because the same annotations
 * raise unrelated exceptions in each.
 * <p>
 * {@code @RestControllerAdvice} works in WebFlux as well as in MVC, and that is
 * the easy part. The part that bites is the validation failure: MVC raises
 * {@code MethodArgumentNotValidException}, which extends {@code BindException};
 * WebFlux raises {@code WebExchangeBindException}, which extends
 * {@code ResponseStatusException}. Neither is a subclass of the other, so an
 * advice written for the servlet stack does not handle the reactive one.
 * <p>
 * What makes it worth a comment is how it fails. {@code WebExchangeBindException}
 * already carries a 400, so an unhandled validation error still answers 400 -
 * with Spring's default body instead of the project's error contract. The status
 * code is right, the payload is a different shape, and no test that only checks
 * the status will notice.
 * <p>
 * Both handlers delegate to the same formatting, driven by the
 * {@code BindingResult} that each exception carries.
 */
@RestControllerAdvice
public class TelemetryExceptionHandler {

    @ExceptionHandler(StationNotFoundException.class)
    public ResponseEntity<ErrorResponse> notFound(StationNotFoundException exception) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND)
                .body(ErrorResponse.of(404, "station_not_found", exception.getMessage()));
    }

    /** The servlet stack's validation failure. */
    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ErrorResponse> invalid(MethodArgumentNotValidException exception) {
        return invalidBody(exception.getBindingResult());
    }

    /** The reactive stack's, which shares no common supertype with the above. */
    @ExceptionHandler(WebExchangeBindException.class)
    public ResponseEntity<ErrorResponse> invalid(WebExchangeBindException exception) {
        return invalidBody(exception.getBindingResult());
    }

    private static ResponseEntity<ErrorResponse> invalidBody(BindingResult result) {
        return ResponseEntity.badRequest()
                .body(new ErrorResponse(400, "validation_failed",
                        "Request body is invalid", fieldErrors(result)));
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<ErrorResponse> badRequest(IllegalArgumentException exception) {
        return ResponseEntity.badRequest()
                .body(ErrorResponse.of(400, "bad_request", exception.getMessage()));
    }

    private static List<String> fieldErrors(BindingResult result) {
        return result.getFieldErrors().stream()
                .map(error -> "%s: %s".formatted(error.getField(), messageOf(error)))
                .sorted()
                .toList();
    }

    private static String messageOf(FieldError error) {
        return error.getDefaultMessage() == null ? "is invalid" : error.getDefaultMessage();
    }
}
