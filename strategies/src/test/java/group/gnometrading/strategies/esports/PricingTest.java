package group.gnometrading.strategies.esports;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/** The ladder, bid and hedge maths: the Python strategy's decision and quoting tests, plus the hedging changes. */
class PricingTest {

    private static final double EPS = 1e-9;

    private static Leg leg(int index, double[][] asks, double[][] bids) {
        Leg leg = new Leg(index, 0, index % 2, 100 + index, 4, 1100 + index);
        leg.set(column(bids, 0), column(bids, 1), column(asks, 0), column(asks, 1), false);
        return leg;
    }

    private static double[] column(double[][] levels, int column) {
        double[] out = new double[levels.length];
        for (int i = 0; i < levels.length; i++) {
            out[i] = levels[i][column];
        }
        return out;
    }

    private static RouteLadder ladder(Leg own, Leg rival, double rate, boolean sellRival) {
        RouteLadder ladder = new RouteLadder(32);
        ladder.addVenue(own, rival, rate, sellRival);
        ladder.sort();
        return ladder;
    }

    private static double fee(double rate, double price) {
        return rate * price * (1 - price);
    }

    @Test
    void aSaleCostsOneMinusTheBidAndSortsByAllInCost() {
        RouteLadder ladder = ladder(
                leg(0, new double[][] {{0.60, 10}}, new double[][] {}),
                leg(1, new double[][] {}, new double[][] {{0.45, 7}}),
                0.07,
                true);

        assertEquals(2, ladder.count());
        assertEquals(0.55, ladder.cost(0), EPS);
        assertEquals(RouteLadder.SELL, ladder.direction(0));
        assertEquals(1, ladder.leg(0));
        assertEquals(0.45, ladder.orderPrice(0), EPS);
        assertEquals(0.55 + fee(0.07, 0.55), ladder.allIn(0), EPS);
        assertEquals(RouteLadder.BUY, ladder.direction(1));
    }

    @Test
    void noSaleRouteWhereTheTeamsMirrorOneMarket() {
        RouteLadder ladder = ladder(
                leg(0, new double[][] {{0.60, 10}}, new double[][] {}),
                leg(1, new double[][] {}, new double[][] {{0.45, 7}}),
                0.05,
                false);
        assertEquals(1, ladder.count());
        assertEquals(RouteLadder.BUY, ladder.direction(0));
    }

    @Test
    void theBidLeavesMinEdgeAfterTheHedge() {
        RouteLadder hedge = ladder(
                leg(0, new double[][] {{0.40, 50}, {0.405, 30}, {0.45, 500}}, new double[][] {}),
                leg(1, new double[][] {}, new double[][] {}),
                0.0695,
                false);
        MakerQuote quote = new MakerQuote();

        assertTrue(quote.compute(hedge, 200, 0.01, 0.0, 0.01, 0.60, 0.01, 1, true));
        assertEquals(80, quote.size(), EPS);
        assertEquals(0.56, quote.price(), EPS);
        assertTrue(quote.price() <= 1 - (0.405 + fee(0.0695, 0.405)) - 0.01);
    }

    @Test
    void theBidStaysUnderTheAsk() {
        RouteLadder hedge = ladder(
                leg(0, new double[][] {{0.30, 50}}, new double[][] {}),
                leg(1, new double[][] {}, new double[][] {}),
                0.0695,
                false);
        MakerQuote quote = new MakerQuote();
        assertTrue(quote.compute(hedge, 200, 0.01, 0.0, 0.01, 0.55, 0.01, 1, true));
        assertEquals(0.54, quote.price(), EPS);
    }

    @Test
    void noBidWithoutAHedge() {
        assertFalse(new MakerQuote().compute(new RouteLadder(4), 200, 0.01, 0.0, 0.01, 0.5, 0.01, 1, true));
    }

    @Test
    void aSingleContractATickBetterNeitherShrinksNorPullsTheBid() {
        // Kalshi 18:31:37 on Lynx: a 1-lot flickering at 0.38 over 45 at 0.39, against a five-contract minimum.
        RouteLadder hedge = ladder(
                leg(0, new double[][] {{0.38, 1}, {0.39, 45}}, new double[][] {}),
                leg(1, new double[][] {}, new double[][] {}),
                0.07,
                false);
        RouteLadder without = ladder(
                leg(0, new double[][] {{0.39, 45}}, new double[][] {}),
                leg(1, new double[][] {}, new double[][] {}),
                0.07,
                false);
        MakerQuote anchored = new MakerQuote();
        MakerQuote cheapest = new MakerQuote();
        MakerQuote plain = new MakerQuote();

        assertTrue(anchored.compute(hedge, 200, 0.01, 0.0, 0.01, 0.70, 0.01, 5, true));
        assertTrue(cheapest.compute(hedge, 200, 0.01, 0.0, 0.01, 0.70, 0.01, 5, false));
        assertTrue(plain.compute(without, 200, 0.01, 0.0, 0.01, 0.70, 0.01, 5, true));

        assertEquals(46, anchored.size(), EPS);
        assertEquals(plain.price(), anchored.price(), EPS, "the bid is the one the 45 alone would make");
        assertEquals(1, cheapest.size(), EPS, "anchored on the 1-lot, the bid would be too small to rest");
    }

    @Test
    void aCrossingHedgeIsLimitedAtTheCapAndTakesWhatTheBookDoesNotShow() {
        RouteLadder ladder = ladder(
                leg(0, new double[][] {{0.35, 40}}, new double[][] {}),
                leg(1, new double[][] {}, new double[][] {}),
                0.07,
                false);
        Completion plan = new Completion(8);

        plan.plan(ladder, 50, 0.05, true, new double[] {0, 0});

        assertEquals(1, plan.count());
        assertEquals(0, plan.leg(0));
        assertEquals(RouteLadder.BUY, plan.direction(0));
        assertEquals(50, plan.contracts(0), EPS);
        assertEquals(0.40, plan.limit(0), EPS);
    }

    @Test
    void withoutCrossingAHedgeIsLimitedAtTheLevelItWasCostedOn() {
        RouteLadder ladder = ladder(
                leg(0, new double[][] {{0.35, 40}, {0.36, 5}, {0.50, 100}}, new double[][] {}),
                leg(1, new double[][] {}, new double[][] {}),
                0.07,
                false);
        Completion plan = new Completion(8);

        plan.plan(ladder, 50, 0.05, false, new double[] {0, 0});

        assertEquals(1, plan.count());
        assertEquals(45, plan.contracts(0), EPS, "only what the book shows within the cap");
        assertEquals(0.36, plan.limit(0), EPS);
    }

    @Test
    void aCrossingSaleOfTheRivalIsLimitedAtOneMinusTheCap() {
        RouteLadder ladder = ladder(
                leg(0, new double[][] {}, new double[][] {}),
                leg(1, new double[][] {}, new double[][] {{0.61, 18}}),
                0.07,
                true);
        Completion plan = new Completion(8);

        plan.plan(ladder, 18, 0.05, true, new double[] {0, 0});

        assertEquals(RouteLadder.SELL, plan.direction(0));
        assertEquals(1, plan.leg(0));
        assertEquals(0.56, plan.limit(0), EPS, "sells down to 1 - (0.39 + 0.05)");
    }

    @Test
    void ordersBelowTheListingsSmallestAreLeftOut() {
        RouteLadder ladder = new RouteLadder(8);
        ladder.add(0.35, 3, 0, RouteLadder.BUY, 0.35, 0.07);
        ladder.add(0.36, 40, 1, RouteLadder.BUY, 0.36, 0.07);
        ladder.sort();
        Completion plan = new Completion(8);

        plan.plan(ladder, 20, 0.05, false, new double[] {5, 0});

        assertEquals(1, plan.count());
        assertEquals(1, plan.leg(0));
        assertEquals(17, plan.contracts(0), EPS);
    }

    @Test
    void theMoveWindowForgetsOldPrices() {
        MoveWindow window = new MoveWindow(5, 16);
        window.add(0, 0.60);
        window.add(2 * MakerHarness.SECOND, 0.50);
        assertEquals(0.10, window.range(), EPS);
        window.add(6 * MakerHarness.SECOND, 0.52);
        assertEquals(0.02, window.range(), EPS);
    }

    @Test
    void aPriceThatSitsStillKeepsItsPlaceInTheWindow() {
        MoveWindow window = new MoveWindow(5, 4);
        window.add(0, 0.50);
        for (int i = 1; i <= 100; i++) {
            window.add(i * MakerHarness.SECOND / 10, 0.50);
        }
        window.add(11 * MakerHarness.SECOND, 0.51);
        assertEquals(0.01, window.range(), EPS, "the repeated 0.50 was last seen at 10 s, inside the window");
    }
}
