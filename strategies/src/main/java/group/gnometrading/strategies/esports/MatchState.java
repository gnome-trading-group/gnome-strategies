package group.gnometrading.strategies.esports;

/**
 * Whether a match is being played, as one source said it at {@code observedNanos}. Status and source codes are the
 * ones the game_state metric stream records, which the Python strategy recorded too, so either can replay the other's
 * sessions.
 */
record MatchState(int status, int source, long observedNanos) {

    static final int UNKNOWN = 0;
    static final int NOT_STARTED = 1;
    static final int LIVE = 2;
    static final int ENDED = 3;

    static final int NO_SOURCE = 0;
    static final int POLYMARKET = 1;
    static final int KALSHI = 2;
    static final int POLYMARKET_EVENT = 3;

    static final MatchState NONE = new MatchState(UNKNOWN, NO_SOURCE, 0);

    static int statusCode(String status) {
        return switch (status) {
            case "not_started" -> NOT_STARTED;
            case "live" -> LIVE;
            case "ended" -> ENDED;
            default -> UNKNOWN;
        };
    }

    static int sourceCode(String source) {
        return switch (source) {
            case "polymarket" -> POLYMARKET;
            case "kalshi" -> KALSHI;
            case "polymarket_event" -> POLYMARKET_EVENT;
            default -> NO_SOURCE;
        };
    }

    static String sourceName(int source) {
        return switch (source) {
            case POLYMARKET -> "polymarket";
            case KALSHI -> "kalshi";
            case POLYMARKET_EVENT -> "polymarket_event";
            default -> "no source";
        };
    }

    /** Ended once any source says so, else live once any does, else not started once any has answered. */
    static MatchState combine(MatchState[] states) {
        for (int status : new int[] {ENDED, LIVE, NOT_STARTED}) {
            MatchState first = null;
            for (MatchState s : states) {
                if (s != null && s.status == status && (first == null || s.observedNanos < first.observedNanos)) {
                    first = s;
                }
            }
            if (first != null) {
                return first;
            }
        }
        return NONE;
    }
}
