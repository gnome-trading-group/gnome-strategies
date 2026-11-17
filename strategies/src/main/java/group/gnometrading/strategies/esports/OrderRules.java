package group.gnometrading.strategies.esports;

import group.gnometrading.sm.ListingSpec;

/**
 * Each leg's order rules from its listing's spec: sizes in lots, at least the smallest size, and worth at least the
 * smallest notional at the order's price. The OMS refuses an order breaking any of them, so every order is shaped
 * here first. Sizes are scaled contracts (1e6 per contract) and money price units (1e9 per dollar), as the OMS checks
 * them, so a size is never put off its lot by floating point.
 *
 * <p>A leg with no spec, or a spec with no lot, trades whole contracts: an order the venue might refuse costs more
 * than a fraction left unhedged. A leg can also be held to whole contracts after its venue refused a fractional one.
 */
final class OrderRules {

    static final long WHOLE = 1_000_000L;
    static final long NONE = Long.MAX_VALUE;

    private static final double PRICE_SCALE = 1e9;
    private static final double SIZE_SCALE = 1e6;

    private final long[] lot;
    private final long[] minSize;
    private final long[] minNotional;
    private final boolean[] wholeOnly;

    OrderRules(int legs) {
        lot = new long[legs];
        minSize = new long[legs];
        minNotional = new long[legs];
        wholeOnly = new boolean[legs];
        for (int i = 0; i < legs; i++) {
            lot[i] = WHOLE;
        }
    }

    void set(int leg, ListingSpec spec) {
        lot[leg] = spec != null && spec.lotSize() > 0 ? spec.lotSize() : WHOLE;
        minSize[leg] = spec != null && spec.minSize() > 0 ? spec.minSize() : 0;
        minNotional[leg] = spec != null && spec.minNotional() > 0 ? spec.minNotional() : 0;
    }

    void holdToWholeContracts(int leg) {
        wholeOnly[leg] = true;
    }

    long lot(int leg) {
        return wholeOnly[leg] ? Math.max(lot[leg], WHOLE) : lot[leg];
    }

    /** {@code size} rounded down to the leg's lot. */
    long floor(int leg, long size) {
        long step = lot(leg);
        return size <= 0 ? 0 : size - size % step;
    }

    /** Whether the venue takes {@code size} at {@code price} on the leg: on its lot, and at least its minimums. */
    boolean valid(int leg, long size, double price) {
        return size > 0 && size % lot(leg) == 0 && size >= minSize[leg] && worthEnough(leg, size, price);
    }

    /** The smallest order the leg takes at {@code price}; NaN prices leave the notional out. */
    long smallest(int leg, double price) {
        long need = Math.max(lot(leg), minSize[leg]);
        if (minNotional[leg] > 0 && price > 0) {
            long scaledPrice = Math.round(price * PRICE_SCALE);
            need = Math.max(need, ceilDiv(minNotional[leg] * WHOLE, scaledPrice));
        }
        long step = lot(leg);
        return ceilDiv(need, step) * step;
    }

    /** The smallest step up from an order of {@code size} at {@code price}: a lot, or the whole first order. */
    long increment(int leg, long size, double price) {
        return size > 0 && valid(leg, size, price) ? lot(leg) : smallest(leg, price) - Math.max(size, 0);
    }

    static long scaled(double contracts) {
        return (long) Math.floor(contracts * SIZE_SCALE + 1e-3);
    }

    static double contracts(long scaled) {
        return scaled / SIZE_SCALE;
    }

    private boolean worthEnough(int leg, long size, double price) {
        if (minNotional[leg] <= 0) {
            return true;
        }
        long scaledPrice = Math.round(price * PRICE_SCALE);
        return scaledPrice * size / WHOLE >= minNotional[leg];
    }

    private static long ceilDiv(long dividend, long divisor) {
        return -Math.floorDiv(-dividend, divisor);
    }
}
