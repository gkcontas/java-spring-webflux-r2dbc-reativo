package com.gkcontas.reactive.api;

import java.util.Arrays;
import java.util.List;

/** Parses the comma-separated {@code codes} parameter the same way in both stacks. */
public final class StationCodes {

    private StationCodes() {
    }

    public static List<String> split(String codes) {
        return Arrays.stream(codes.split(","))
                .map(String::trim)
                .filter(code -> !code.isEmpty())
                .toList();
    }
}
