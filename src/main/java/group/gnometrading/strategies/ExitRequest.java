package group.gnometrading.strategies;

/** A strategy's request to end its session; see {@link StrategyControl#requestExit}. */
record ExitRequest(String reason, boolean flatten, long timeoutNanos) {}
