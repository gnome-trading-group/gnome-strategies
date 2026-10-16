package group.gnometrading.strategies;

import java.util.List;

/**
 * How a strategy's exit ended.
 *
 * @param held every listing still holding a position, whether the strategy chose to keep it or couldn't close it
 * @param timedOut whether the time ran out with orders still working or, when flattening, positions still held
 */
public record ExitResult(String reason, boolean flatten, boolean timedOut, List<HeldPosition> held) {

    /** @param netQuantity signed, in size units */
    public record HeldPosition(int listingId, long netQuantity) {}

    /** Asked to close everything and couldn't: someone has to decide what to do with what's left. */
    public boolean leftPositions() {
        return flatten && !held.isEmpty();
    }
}
