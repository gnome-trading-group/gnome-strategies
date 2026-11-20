package group.gnometrading.strategies.esports;

import group.gnometrading.sm.PriceGrid;

/**
 * The bid to rest on one team so that, if it fills, buying the other team elsewhere at today's prices still locks
 * min_edge per pair: the highest price on the listing's grid whose cost with the maker fee leaves min_edge below $1
 * after the deepest hedge level the size reaches, one grid step under the best ask so it rests rather than takes.
 *
 * <p>The size is what the hedge ladder offers within {@code depthBand} of an anchor level, up to the quote size. The
 * anchor is the level where the hedge first adds up to the smallest order the quoted listing takes, so a level too
 * small to hedge a fill on its own (a flickering contract a tick better) neither shrinks the bid below what the venue
 * accepts nor takes it down. With {@code anchorAtMinSize} off, the anchor is the cheapest level.
 */
final class MakerQuote {

    private static final double EPS = 1e-9;
    private static final double PRICE_SCALE = 1e9;

    private double price;
    private double size;

    double price() {
        return price;
    }

    double size() {
        return size;
    }

    /** Replaces the computed bid, once the caller has put it on the grid in force. */
    void override(double newPrice, double newSize) {
        price = newPrice;
        size = newSize;
    }

    /** Fills price and size and returns true, or returns false when there is no bid to rest. */
    boolean compute(
            RouteLadder hedge,
            double quoteSize,
            double minEdge,
            double makerRate,
            PriceGrid grid,
            double bestAsk,
            double depthBand,
            double minOrder,
            boolean anchorAtMinSize) {
        if (hedge.isEmpty()) {
            return false;
        }
        int anchor = anchorAtMinSize ? anchor(hedge, minOrder) : 0;
        double floor = hedge.allIn(anchor) + depthBand;
        double total = 0;
        double worst = hedge.allIn(0);
        for (int i = 0; i < hedge.count(); i++) {
            if (hedge.allIn(i) > floor + 1e-12 || total >= quoteSize) {
                break;
            }
            total += Math.min(hedge.size(i), quoteSize - total);
            worst = hedge.allIn(i);
        }
        long bid = highestBid(1 - worst - minEdge, makerRate, grid);
        if (!Double.isNaN(bestAsk)) {
            bid = Math.min(bid, grid.down(grid.ceil(Math.round(bestAsk * PRICE_SCALE)), 1));
        }
        double contracts = Math.floor(total + EPS);
        if (bid < grid.min() || contracts < 1) {
            return false;
        }
        price = bid / PRICE_SCALE;
        size = contracts;
        return true;
    }

    /** The first level where the hedge adds up to {@code minOrder}, or the last level if it never does. */
    private static int anchor(RouteLadder hedge, double minOrder) {
        double reached = 0;
        for (int i = 0; i < hedge.count(); i++) {
            reached += hedge.size(i);
            if (reached + EPS >= minOrder) {
                return i;
            }
        }
        return hedge.count() - 1;
    }

    /** The highest grid price whose cost with the maker fee stays within {@code budget}, scaled; 0 for none. */
    private static long highestBid(double budget, double makerRate, PriceGrid grid) {
        if (budget < grid.min() / PRICE_SCALE) {
            return 0;
        }
        long bid = grid.floor(Math.min(Math.round(budget * PRICE_SCALE), grid.max()));
        while (bid > 0 && cost(bid, makerRate) > budget + 1e-12) {
            bid = grid.down(bid, 1);
        }
        return bid;
    }

    private static double cost(long scaledBid, double makerRate) {
        double bid = scaledBid / PRICE_SCALE;
        return bid + makerRate * bid * (1 - bid);
    }
}
