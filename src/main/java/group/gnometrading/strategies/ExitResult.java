package group.gnometrading.strategies;

import java.util.List;

/**
 * How a strategy's exit ended.
 *
 * @param held every listing still holding a position, whether the strategy chose to keep it or couldn't close it
 * @param timedOut whether the time ran out with orders still working or, when flattening, positions still held
 */
public record ExitResult(String reason, boolean flatten, boolean timedOut, List<HeldPosition> held) {

    /** Why a position was still held when the exit ended. */
    public enum Why {
        /** The strategy asked to keep its positions. */
        KEPT,
        /** Smaller than the venue's minimum order, so no single order can close it. */
        TOO_SMALL,
        /** The venue or the OMS kept refusing the close. */
        REJECTED,
        /** Still closing when the time ran out. */
        TIMED_OUT
    }

    /** @param netQuantity signed, in size units */
    public record HeldPosition(int listingId, long netQuantity, Why why) {}

    /** Asked to close everything and couldn't: someone has to decide what to do with what's left. */
    public boolean leftPositions() {
        return flatten && !held.isEmpty();
    }
}
