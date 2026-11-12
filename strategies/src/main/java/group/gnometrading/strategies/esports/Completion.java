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
 */
final class Completion {

    private static final double EPS = 1e-9;

    private final int[] leg;
    private final int[] direction;
    private final double[] contracts;
    private final double[] limit;
    private int count;

    Completion(int capacity) {
        leg = new int[capacity];
        direction = new int[capacity];
        contracts = new double[capacity];
        limit = new double[capacity];
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
        return contracts[at];
    }

    /** The order's limit on its listing: the highest ask bought, or the lowest bid sold into. */
    double limit(int at) {
        return limit[at];
    }

    /** Plans the orders; {@code minSize} is each leg's smallest order, by leg index. */
    void plan(RouteLadder ladder, double wanted, double maxSlippage, boolean crossToCap, double[] minSize) {
        count = 0;
        if (ladder.isEmpty() || wanted < 1) {
            return;
        }
        double cap = ladder.cost(0) + maxSlippage;
        double left = takeVisible(ladder, cap, Math.floor(wanted + EPS));
        if (crossToCap) {
            limitAtCap(ladder, cap, left);
        }
        dropTooSmall(minSize);
    }

    /** Takes the levels the book shows within the cap; returns what they leave. */
    private double takeVisible(RouteLadder ladder, double cap, double wanted) {
        double left = wanted;
        for (int i = 0; i < ladder.count() && left > 0; i++) {
            if (ladder.cost(i) > cap + 1e-12) {
                break;
            }
            double taken = Math.min(ladder.size(i), left);
            take(ladder.leg(i), ladder.direction(i), taken, ladder.orderPrice(i));
            left -= taken;
        }
        return left;
    }

    private void limitAtCap(RouteLadder ladder, double cap, double left) {
        if (left > 0) {
            take(ladder.leg(0), ladder.direction(0), left, ladder.orderPrice(0));
        }
        for (int i = 0; i < count; i++) {
            limit[i] = direction[i] == RouteLadder.BUY ? cap : 1 - cap;
        }
    }

    private void dropTooSmall(double[] minSize) {
        int kept = 0;
        for (int i = 0; i < count; i++) {
            double whole = Math.floor(contracts[i] + EPS);
            if (whole >= Math.max(1.0, minSize[leg[i]])) {
                leg[kept] = leg[i];
                direction[kept] = direction[i];
                contracts[kept] = whole;
                limit[kept] = limit[i];
                kept++;
            }
        }
        count = kept;
    }

    private void take(int legIndex, int routeDirection, double amount, double price) {
        for (int i = 0; i < count; i++) {
            if (leg[i] == legIndex && direction[i] == routeDirection) {
                contracts[i] += amount;
                limit[i] = routeDirection == RouteLadder.BUY ? Math.max(limit[i], price) : Math.min(limit[i], price);
                return;
            }
        }
        if (count == leg.length) {
            return;
        }
        leg[count] = legIndex;
        direction[count] = routeDirection;
        contracts[count] = amount;
        limit[count] = price;
        count++;
    }
}
