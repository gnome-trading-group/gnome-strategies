package group.gnometrading.strategies;

/** Told once a strategy's exit has wound down, on the strategy's thread; whatever it does next must not block. */
public interface ExitListener {

    void onExitComplete(ExitResult result);
}
