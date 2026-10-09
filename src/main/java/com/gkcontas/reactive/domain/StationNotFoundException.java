package com.gkcontas.reactive.domain;

/** Raised by both stacks, so the 404 contract has a single source. */
public class StationNotFoundException extends RuntimeException {

    private final String code;

    public StationNotFoundException(String code) {
        super("No station with code " + code);
        this.code = code;
    }

    public String getCode() {
        return code;
    }
}
