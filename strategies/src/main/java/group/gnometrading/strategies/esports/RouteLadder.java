package group.gnometrading.strategies.esports;

/**
 * Every way to hold one team, cheapest first: one entry per book level. A team is held by buying its listing at the
 * asks, or, on a venue that books a sale of an unheld contract as the opposite side (Kalshi books it as NO), by selling
 * the rival team's listing at its bids: a sale at b pays when the rival loses, so it costs 1 - b per contract.
 *
 * <p>An entry's all-in cost adds the venue's taker fee curve, {@code rate * c * (1 - c)}, which is the same for a sale
 * at b as for a buy at 1 - b; the venues' fee rounding is left out. Prices are plain probabilities in [0, 1] and sizes
 * contracts. Filled in place and sorted without allocating, so it can be rebuilt on every book update.
 */
final class RouteLadder {

    static final int BUY = 1;
    static final int SELL = -1;

    private final double[] cost;
    private final double[] allIn;
    private final double[] size;
    private final int[] leg;
    private final int[] direction;
    private final double[] orderPrice;
    private int count;

    RouteLadder(int capacity) {
        cost = new double[capacity];
        allIn = new double[capacity];
        size = new double[capacity];
        leg = new int[capacity];
        direction = new int[capacity];
        orderPrice = new double[capacity];
    }

    void clear() {
        count = 0;
    }

    int count() {
        return count;
    }

    boolean isEmpty() {
        return count == 0;
    }

    /** What one contract paying out on the team costs at entry {@code i}, before the fee. */
    double cost(int at) {
        return cost[at];
    }

    double allIn(int at) {
        return allIn[at];
    }

    double size(int at) {
        return size[at];
    }

    /** The leg the order goes to: the team's own listing for a buy, the rival's for a sale. */
    int leg(int at) {
        return leg[at];
    }

    int direction(int at) {
        return direction[at];
    }

    /** The price on the leg's listing: the ask bought, or the bid sold into. */
    double orderPrice(int at) {
        return orderPrice[at];
    }

    /** Adds the routes through one venue: the own listing's asks, and if allowed the rival listing's bids. */
    void addVenue(Leg own, Leg rival, double feeRate, boolean sellRival) {
        for (int i = 0; i < own.askCount(); i++) {
            if (own.askSize(i) > 0) {
                add(own.askPrice(i), own.askSize(i), own.index(), BUY, own.askPrice(i), feeRate);
            }
        }
        if (sellRival) {
            for (int i = 0; i < rival.bidCount(); i++) {
                if (rival.bidSize(i) > 0) {
                    add(1 - rival.bidPrice(i), rival.bidSize(i), rival.index(), SELL, rival.bidPrice(i), feeRate);
                }
            }
        }
    }

    void add(double routeCost, double routeSize, int legIndex, int routeDirection, double price, double feeRate) {
        if (count == cost.length) {
            return;
        }
        cost[count] = routeCost;
        allIn[count] = routeCost + feeRate * routeCost * (1 - routeCost);
        size[count] = routeSize;
        leg[count] = legIndex;
        direction[count] = routeDirection;
        orderPrice[count] = price;
        count++;
    }

    /** Cheapest all-in first; on a tie a sale before a buy, as the Python strategy ordered them. */
    void sort() {
        for (int i = 1; i < count; i++) {
            for (int j = i; j > 0 && before(j, j - 1); j--) {
                swap(j, j - 1);
            }
        }
    }

    private boolean before(int first, int second) {
        return allIn[first] < allIn[second] || (allIn[first] == allIn[second] && direction[first] < direction[second]);
    }

    private void swap(int first, int second) {
        double held = cost[first];
        cost[first] = cost[second];
        cost[second] = held;
        held = allIn[first];
        allIn[first] = allIn[second];
        allIn[second] = held;
        held = size[first];
        size[first] = size[second];
        size[second] = held;
        int heldInt = leg[first];
        leg[first] = leg[second];
        leg[second] = heldInt;
        heldInt = direction[first];
        direction[first] = direction[second];
        direction[second] = heldInt;
        held = orderPrice[first];
        orderPrice[first] = orderPrice[second];
        orderPrice[second] = held;
    }
}
