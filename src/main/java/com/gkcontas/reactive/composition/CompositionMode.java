package com.gkcontas.reactive.composition;

/**
 * The three ways to compose a fan-out, and what each one trades.
 *
 * <ul>
 *   <li>{@code FLAT_MAP} - legs run together, results come back in completion
 *       order. Fast, and the order of the answer is not the order of the
 *       request.</li>
 *   <li>{@code CONCAT_MAP} - one leg at a time, order preserved. The total is
 *       the sum of the legs, which is the cost people discover in production
 *       after reaching for the operator that "keeps the order".</li>
 *   <li>{@code FLAT_MAP_SEQUENTIAL} - legs run together, results are buffered
 *       and emitted in request order. What {@code concatMap} is usually reached
 *       for by mistake, and it costs memory for the buffered out-of-order
 *       results.</li>
 * </ul>
 */
public enum CompositionMode {
    FLAT_MAP,
    CONCAT_MAP,
    FLAT_MAP_SEQUENTIAL;

    public static CompositionMode parse(String raw) {
        if (raw == null || raw.isBlank()) {
            return FLAT_MAP;
        }
        String normalised = raw.trim().replace("-", "_").toUpperCase();
        for (CompositionMode mode : values()) {
            if (mode.name().equals(normalised) || mode.name().replace("_", "").equals(normalised)) {
                return mode;
            }
        }
        throw new IllegalArgumentException(
                "mode must be one of flatMap, concatMap, flatMapSequential");
    }
}
