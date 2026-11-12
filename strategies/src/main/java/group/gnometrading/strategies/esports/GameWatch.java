package group.gnometrading.strategies.esports;

/**
 * Whether the session's match is being played. Read on the strategy thread on every decision, so implementations
 * answer from state already in memory: the live one from what its feed threads last published, a replay from the rows
 * a session recorded.
 */
interface GameWatch {

    /** The combined state as of {@code nowNanos}. */
    MatchState state(long nowNanos);

    /** Whether any source has reported the match live by {@code nowNanos}, whether or not a decision saw it then. */
    boolean wasLive(long nowNanos);

    /** When a source that has reported the match last answered; any source's answer before one has; 0 if none. */
    long heardNanos(long nowNanos);

    void close();
}
