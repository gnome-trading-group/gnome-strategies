package group.gnometrading.strategies.esports;

/**
 * The orders that buy up to a number of contracts of the short team, no dearer than the cheapest route's cost plus
 * {@code maxSlippage}: one order per leg and direction, sized to the levels the book shows within that cap.
 *
 * <p>Crossing ({@code crossToCap}), every order is limited at the cap itself, and what the book doesn't show within it
 * goes to the cheapest route's order: the venue then fills at the best prices it has up to the cap. A completion
 * limited at the level it was costed on misses whenever that level goes first, which in a fast market is most of the
 * time, and the unhedged position waits for the next try. Without crossing, each order is limited at the worst level
 * it was costed on, and sized to what the book shows.
 *
 * <p>Every order is then shaped to its leg's {@link OrderRules}: rounded down to the lot, and dropped below the
 * smallest size or notional. What that takes off goes to the next route within the cap that can take it on its own
 * lot, so a whole-contract venue's fraction lands on a fractional one when the session has one. A remainder no route
 * can take is either left for the next hedge or, when it is at least {@code roundUp} of the smallest step any route
 * takes, bought as that step: never so far past the gap that the other team would need a hedge back.
 */
final class Completion {

    private final int[] leg;
    private final int[] direction;
    private final long[] size;
    private final double[] limit;
    private final long[] used;
    private int count;

    Completion(int capacity, int ladderCapacity) {
        leg = new int[capacity];
        direction = new int[capacity];
        size = new long[capacity];
        limit = new double[capacity];
        used = new long[ladderCapacity];
    }

    int count() {
        return count;
    }

    int leg(int at) {
        return leg[at];
    }

    int direction(int at) {
        return direction[at];
    }

    double contracts(int at) {
        return OrderRules.contracts(size[at]);
    }

    long scaledContracts(int at) {
        return size[at];
    }

    /** The order's limit on its listing: the highest ask bought, or the lowest bid sold into. */
    double limit(int at) {
        return limit[at];
    }

    /**
     * The smallest order any route within the cap takes, scaled; {@link OrderRules#NONE} without a route. A gap under
     * it can't be hedged by any order the venues accept.
     */
    static long smallestOrder(RouteLadder ladder, double maxSlippage, boolean crossToCap, OrderRules rules) {
        if (ladder.isEmpty()) {
            return OrderRules.NONE;
        }
        double cap = ladder.cost(0) + maxSlippage;
        long smallest = OrderRules.NONE;
        for (int i = 0; i < ladder.count() && withinCap(ladder, i, cap); i++) {
            double price = crossToCap ? capLimit(ladder.direction(i), cap) : ladder.orderPrice(i);
            smallest = Math.min(smallest, rules.smallest(ladder.leg(i), price));
        }
        return smallest;
    }

    /**
     * Plans the orders for {@code wanted} contracts. {@code overshootLimit} (scaled) bounds a rounded-up remainder: what
     * it buys past {@code wanted} stays under it.
     */
    void plan(
            RouteLadder ladder,
            double wanted,
            double maxSlippage,
            boolean crossToCap,
            OrderRules rules,
            double roundUp,
            long overshootLimit) {
        count = 0;
        long want = OrderRules.scaled(wanted);
        if (ladder.isEmpty() || want <= 0) {
            return;
        }
        double cap = ladder.cost(0) + maxSlippage;
        long left = takeVisible(ladder, cap, want);
        if (crossToCap) {
            limitAtCap(ladder, cap, left);
        }
        long remainder = shapeToRules(rules);
        remainder = moveRemainder(ladder, cap, crossToCap, rules, remainder);
        if (remainder > 0 && roundUp > 0) {
            roundUpRemainder(ladder, cap, crossToCap, rules, remainder, roundUp, overshootLimit);
        }
        dropEmpty();
    }

    /** Takes the levels the book shows within the cap; returns what they leave. */
    private long takeVisible(RouteLadder ladder, double cap, long wanted) {
        long left = wanted;
        for (int i = 0; i < ladder.count(); i++) {
            used[i] = 0;
        }
        for (int i = 0; i < ladder.count() && left > 0 && withinCap(ladder, i, cap); i++) {
            long taken = Math.min(OrderRules.scaled(ladder.size(i)), left);
            if (taken > 0 && take(ladder.leg(i), ladder.direction(i), taken, ladder.orderPrice(i)) >= 0) {
                used[i] = taken;
                left -= taken;
            }
        }
        return left;
    }

    private void limitAtCap(RouteLadder ladder, double cap, long left) {
        if (left > 0) {
            take(ladder.leg(0), ladder.direction(0), left, ladder.orderPrice(0));
        }
        for (int i = 0; i < count; i++) {
            limit[i] = capLimit(direction[i], cap);
        }
    }

    /** Rounds each order down to its lot and empties those under the leg's minimums; returns what that took off. */
    private long shapeToRules(OrderRules rules) {
        long removed = 0;
        for (int i = 0; i < count; i++) {
            long shaped = rules.floor(leg[i], size[i]);
            if (!rules.valid(leg[i], shaped, limit[i])) {
                shaped = 0;
            }
            removed += size[i] - shaped;
            size[i] = shaped;
        }
        return removed;
    }

    /** Offers {@code remainder} to each route within the cap, cheapest first, on its own lot; returns what is left. */
    private long moveRemainder(RouteLadder ladder, double cap, boolean crossToCap, OrderRules rules, long remainder) {
        long left = remainder;
        for (int i = 0; i < ladder.count() && left > 0 && withinCap(ladder, i, cap); i++) {
            long room = crossToCap ? left : Math.min(left, visibleRoom(ladder, i));
            if (room <= 0) {
                continue;
            }
            int at = find(ladder.leg(i), ladder.direction(i));
            long have = at >= 0 ? size[at] : 0;
            double price = priceWith(ladder, i, at, cap, crossToCap);
            long total = rules.floor(ladder.leg(i), have + room);
            if (total > have && rules.valid(ladder.leg(i), total, price)) {
                left -= grow(ladder, i, at, total - have, price);
            }
        }
        return left;
    }

    /** Buys the smallest step any route within the cap takes, if the remainder is enough of it and won't overshoot. */
    private void roundUpRemainder(
            RouteLadder ladder,
            double cap,
            boolean crossToCap,
            OrderRules rules,
            long remainder,
            double roundUp,
            long overshootLimit) {
        int best = smallestStepRoute(ladder, cap, crossToCap, rules);
        if (best < 0) {
            return;
        }
        int at = find(ladder.leg(best), ladder.direction(best));
        long have = at >= 0 ? size[at] : 0;
        double price = priceWith(ladder, best, at, cap, crossToCap);
        long step = rules.increment(ladder.leg(best), have, price);
        if (remainder < roundUp * step
                || step - remainder >= overshootLimit
                || !rules.valid(ladder.leg(best), have + step, price)) {
            return;
        }
        grow(ladder, best, at, step, price);
    }

    /** The route within the cap whose order grows by the least to stay valid; the cheapest on a tie, -1 for none. */
    private int smallestStepRoute(RouteLadder ladder, double cap, boolean crossToCap, OrderRules rules) {
        int best = -1;
        long bestStep = OrderRules.NONE;
        for (int i = 0; i < ladder.count() && withinCap(ladder, i, cap); i++) {
            int at = find(ladder.leg(i), ladder.direction(i));
            long have = at >= 0 ? size[at] : 0;
            long step = rules.increment(ladder.leg(i), have, priceWith(ladder, i, at, cap, crossToCap));
            boolean fits = crossToCap || visibleRoom(ladder, i) >= step;
            if (step > 0 && fits && step < bestStep) {
                best = i;
                bestStep = step;
            }
        }
        return best;
    }

    /** Adds {@code amount} to route {@code entry}'s order (made if {@code at} is -1); returns what was added. */
    private long grow(RouteLadder ladder, int entry, int at, long amount, double price) {
        int order = at >= 0 ? at : take(ladder.leg(entry), ladder.direction(entry), 0, price);
        if (order < 0) {
            return 0;
        }
        size[order] += amount;
        limit[order] = price;
        used[entry] += amount;
        return amount;
    }

    /** The limit an order on {@code at} would need to also reach route {@code entry}. */
    private double priceWith(RouteLadder ladder, int entry, int at, double cap, boolean crossToCap) {
        int routeDirection = ladder.direction(entry);
        if (crossToCap) {
            return capLimit(routeDirection, cap);
        }
        double price = ladder.orderPrice(entry);
        if (at < 0) {
            return price;
        }
        return routeDirection == RouteLadder.BUY ? Math.max(limit[at], price) : Math.min(limit[at], price);
    }

    private long visibleRoom(RouteLadder ladder, int entry) {
        return OrderRules.scaled(ladder.size(entry)) - used[entry];
    }

    private void dropEmpty() {
        int kept = 0;
        for (int i = 0; i < count; i++) {
            if (size[i] > 0) {
                leg[kept] = leg[i];
                direction[kept] = direction[i];
                size[kept] = size[i];
                limit[kept] = limit[i];
                kept++;
            }
        }
        count = kept;
    }

    private int find(int legIndex, int routeDirection) {
        for (int i = 0; i < count; i++) {
            if (leg[i] == legIndex && direction[i] == routeDirection) {
                return i;
            }
        }
        return -1;
    }

    /** Adds to the leg's order in that direction, or starts one; returns its index, or -1 when full. */
    private int take(int legIndex, int routeDirection, long amount, double price) {
        int at = find(legIndex, routeDirection);
        if (at >= 0) {
            size[at] += amount;
            limit[at] = routeDirection == RouteLadder.BUY ? Math.max(limit[at], price) : Math.min(limit[at], price);
            return at;
        }
        if (count == leg.length) {
            return -1;
        }
        leg[count] = legIndex;
        direction[count] = routeDirection;
        size[count] = amount;
        limit[count] = price;
        return count++;
    }

    private static boolean withinCap(RouteLadder ladder, int entry, double cap) {
        return ladder.cost(entry) <= cap + 1e-12;
    }

    private static double capLimit(int routeDirection, double cap) {
        return routeDirection == RouteLadder.BUY ? cap : 1 - cap;
    }
}
