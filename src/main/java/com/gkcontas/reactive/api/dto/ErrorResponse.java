package com.gkcontas.reactive.api.dto;

import java.util.List;

/** The same error contract in both stacks. */
public record ErrorResponse(int status, String error, String message, List<String> details) {

    public static ErrorResponse of(int status, String error, String message) {
        return new ErrorResponse(status, error, message, List.of());
    }
}
