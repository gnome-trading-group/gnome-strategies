package group.gnometrading.strategies;

/** What a strategy can ask of the session running it. Every method is safe to call from any thread. */
public interface StrategyControl {

    /**
     * Ends the session. The strategy's callbacks stop at once, its quotes are cancelled, and if {@code flatten} is
     * set, what it holds is sold (or bought back) with market orders until it is flat or {@code timeoutMillis} has
     * passed. The session then reports why it exited and shuts down as STOPPED.
     *
     * <p>Only the first request counts; later ones are ignored.
     *
     * @return true if this request is the one the session acts on
     */
    boolean requestExit(String reason, boolean flatten, long timeoutMillis);

    /** True once an exit has been requested. */
    boolean isExiting();
}
