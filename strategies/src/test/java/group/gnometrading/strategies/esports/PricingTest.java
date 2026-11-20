package group.gnometrading.strategies.esports;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import group.gnometrading.sm.ListingSpec;
import group.gnometrading.sm.PriceGrid;
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

    /** Rules by leg: {lot, smallest size, smallest notional in dollars}, in contracts; null for a leg with no spec. */
    private static OrderRules rules(double[]... legs) {
        OrderRules rules = new OrderRules(legs.length);
        for (int i = 0; i < legs.length; i++) {
            double[] leg = legs[i];
            rules.set(
                    i,
                    leg == null
                            ? null
                            : new ListingSpec(
                                    100 + i,
                                    PriceGrid.single(10_000_000L, 1_000_000_000L),
                                    Math.round(leg[0] * 1e6),
                                    Math.round(leg[2] * 1e9),
                                    1,
                                    Math.round(leg[1] * 1e6)));
        }
        return rules;
    }

    private static final double[] WHOLE = {1, 0, 0};
    private static final double[] CENTS = {0.01, 0, 0};
    private static final PriceGrid CENT_GRID = PriceGrid.single(10_000_000L, 1_000_000_000L);
    private static final PriceGrid TAPERED = PriceGrid.of(
            new long[] {0, 100_000_000L, 900_000_000L},
            new long[] {100_000_000L, 900_000_000L, 1_000_000_000L},
            new long[] {1_000_000L, 10_000_000L, 1_000_000L});

    private static Completion plan(RouteLadder ladder, double wanted, boolean cross, OrderRules rules) {
        return plan(ladder, wanted, cross, rules, 0, OrderRules.NONE);
    }

    private static Completion plan(
            RouteLadder ladder, double wanted, boolean cross, OrderRules rules, double roundUp, long overshootLimit) {
        Completion plan = new Completion(8, 32);
        plan.plan(ladder, wanted, 0.05, cross, rules, roundUp, overshootLimit);
        return plan;
    }

    private static double total(Completion plan) {
        double total = 0;
        for (int i = 0; i < plan.count(); i++) {
            total += plan.contracts(i);
        }
        return total;
    }

    private static double on(Completion plan, int leg) {
        for (int i = 0; i < plan.count(); i++) {
            if (plan.leg(i) == leg) {
                return plan.contracts(i);
            }
        }
        return 0;
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

        assertTrue(quote.compute(hedge, 200, 0.01, 0.0, CENT_GRID, 0.60, 0.01, 1, true));
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
        assertTrue(quote.compute(hedge, 200, 0.01, 0.0, CENT_GRID, 0.55, 0.01, 1, true));
        assertEquals(0.54, quote.price(), EPS);
    }

    @Test
    void aTaperedGridBidsInTenthsOfACentInItsTails() {
        RouteLadder hedge = ladder(
                leg(0, new double[][] {{0.9345, 50}}, new double[][] {}),
                leg(1, new double[][] {}, new double[][] {}),
                0.0,
                false);
        MakerQuote tapered = new MakerQuote();
        MakerQuote cents = new MakerQuote();

        assertTrue(tapered.compute(hedge, 200, 0.01, 0.0, TAPERED, Double.NaN, 0.01, 1, true));
        assertTrue(cents.compute(hedge, 200, 0.01, 0.0, CENT_GRID, Double.NaN, 0.01, 1, true));

        assertEquals(0.055, tapered.price(), EPS);
        assertEquals(0.05, cents.price(), EPS);
    }

    @Test
    void theBidStaysOneGridStepUnderAnAskOnABandEdge() {
        RouteLadder hedge = ladder(
                leg(0, new double[][] {{0.30, 50}}, new double[][] {}),
                leg(1, new double[][] {}, new double[][] {}),
                0.0,
                false);
        MakerQuote quote = new MakerQuote();

        assertTrue(quote.compute(hedge, 200, 0.01, 0.0, TAPERED, 0.10, 0.01, 1, true));

        assertEquals(0.099, quote.price(), EPS);
    }

    @Test
    void noBidWithoutAHedge() {
        assertFalse(new MakerQuote().compute(new RouteLadder(4), 200, 0.01, 0.0, CENT_GRID, 0.5, 0.01, 1, true));
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

        assertTrue(anchored.compute(hedge, 200, 0.01, 0.0, CENT_GRID, 0.70, 0.01, 5, true));
        assertTrue(cheapest.compute(hedge, 200, 0.01, 0.0, CENT_GRID, 0.70, 0.01, 5, false));
        assertTrue(plain.compute(without, 200, 0.01, 0.0, CENT_GRID, 0.70, 0.01, 5, true));

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
        Completion plan = plan(ladder, 50, true, rules(WHOLE, WHOLE));

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
        Completion plan = plan(ladder, 50, false, rules(WHOLE, WHOLE));

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
        Completion plan = plan(ladder, 18, true, rules(WHOLE, WHOLE));

        assertEquals(RouteLadder.SELL, plan.direction(0));
        assertEquals(1, plan.leg(0));
        assertEquals(0.56, plan.limit(0), EPS, "sells down to 1 - (0.39 + 0.05)");
    }

    @Test
    void anOrderBelowItsLegsSmallestMovesToTheNextRoute() {
        RouteLadder ladder = twoLegs(3, 40);

        Completion plan = plan(ladder, 20, false, rules(new double[] {1, 5, 0}, WHOLE));

        assertEquals(1, plan.count());
        assertEquals(1, plan.leg(0));
        assertEquals(20, plan.contracts(0), EPS, "the 3 too few for leg 0 go to leg 1's visible 40");
        assertEquals(0.36, plan.limit(0), EPS);
    }

    /** Leg 0 asks 0.35, leg 1 asks 0.36: both buys of the same team. */
    private static RouteLadder twoLegs(double size0, double size1) {
        RouteLadder ladder = new RouteLadder(8);
        ladder.add(0.35, size0, 0, RouteLadder.BUY, 0.35, 0.07);
        ladder.add(0.36, size1, 1, RouteLadder.BUY, 0.36, 0.07);
        ladder.sort();
        return ladder;
    }

    @Test
    void anOddAmountIsHedgedToTheCentOnAFractionalLeg() {
        Completion plan = plan(twoLegs(100, 100), 2.7675, true, rules(CENTS, CENTS));

        assertEquals(1, plan.count());
        assertEquals(0, plan.leg(0));
        assertEquals(2.76, plan.contracts(0), EPS, "the 0.0075 under a lot stays");
    }

    @Test
    void anOddAmountIsHedgedToTheContractOnWholeLegs() {
        Completion plan = plan(twoLegs(100, 100), 2.7675, true, rules(WHOLE, WHOLE));

        assertEquals(1, plan.count());
        assertEquals(2, plan.contracts(0), EPS);
    }

    @Test
    void theFractionAWholeLegCantTakeGoesToAFractionalOne() {
        Completion plan = plan(twoLegs(100, 100), 2.7675, true, rules(WHOLE, CENTS));

        assertEquals(2, on(plan, 0), EPS, "the cheaper whole-contract leg takes the whole contracts");
        assertEquals(0.76, on(plan, 1), EPS, "the fraction moves to the fractional leg");
        for (int i = 0; i < plan.count(); i++) {
            assertEquals(0.40, plan.limit(i), EPS, "both crossing to the cap");
        }
    }

    @Test
    void withoutCrossingTheFractionMovesOnlyIntoWhatTheNextBookShows() {
        Completion shown = plan(twoLegs(2.7675, 0.5), 2.7675, false, rules(WHOLE, CENTS));
        assertEquals(2, on(shown, 0), EPS);
        assertEquals(0.5, on(shown, 1), EPS, "only the 0.5 leg 1 shows");
    }

    @Test
    void anOrderWorthLessThanItsLegsSmallestNotionalMovesOn() {
        RouteLadder ladder = new RouteLadder(8);
        ladder.add(0.10, 100, 0, RouteLadder.BUY, 0.10, 0.07);
        ladder.add(0.11, 100, 1, RouteLadder.BUY, 0.11, 0.07);
        ladder.sort();
        OrderRules rules = rules(new double[] {0.01, 0, 1.0}, CENTS);

        Completion plan = plan(ladder, 6, false, rules);

        assertEquals(0, on(plan, 0), EPS, "6 at 0.10 is $0.60, under leg 0's $1");
        assertEquals(6, on(plan, 1), EPS);
        assertEquals(10_000_000, rules.smallest(0, 0.10), "10 contracts make $1 at 0.10");
    }

    @Test
    void aLegWithNoSpecTradesWholeContracts() {
        OrderRules rules = rules(null, null);

        assertEquals(OrderRules.WHOLE, rules.smallest(0, 0.5));
        assertEquals(2, total(plan(twoLegs(100, 100), 2.7675, true, rules)), EPS);
    }

    @Test
    void aRemainderAtLeastHalfAContractIsBoughtAsOneWhenRoundingUp() {
        RouteLadder ladder = twoLegs(100, 100);

        assertEquals(3, total(plan(ladder, 2.7675, true, rules(WHOLE, WHOLE), 0.5, OrderRules.NONE)), EPS);
        assertEquals(3, total(plan(ladder, 2.5, true, rules(WHOLE, WHOLE), 0.5, OrderRules.NONE)), EPS, "at it");
        assertEquals(2, total(plan(ladder, 2.4, true, rules(WHOLE, WHOLE), 0.5, OrderRules.NONE)), EPS, "under it");
        assertEquals(2, total(plan(ladder, 2.7675, true, rules(WHOLE, WHOLE), 0, OrderRules.NONE)), EPS, "off");
        assertEquals(
                0.77,
                total(plan(ladder, 0.7675, true, rules(WHOLE, CENTS), 0.5, OrderRules.NONE)),
                EPS,
                "the 0.0075 left on a cent lot is three quarters of one");
    }

    @Test
    void aRoundUpThatWouldCallForAHedgeOfTheOtherTeamIsLeftOut() {
        RouteLadder ladder = twoLegs(100, 100);
        long otherTeamsSmallest = 100_000L;

        assertEquals(
                2,
                total(plan(ladder, 2.7675, true, rules(WHOLE, WHOLE), 0.5, otherTeamsSmallest)),
                EPS,
                "buying 0.2325 past the gap would leave the other team short more than its smallest hedge");
        assertEquals(3, total(plan(ladder, 2.95, true, rules(WHOLE, WHOLE), 0.5, otherTeamsSmallest)), EPS);
    }

    @Test
    void theSmallestOrderIsTheLeastAnyRouteWithinTheCapTakes() {
        assertEquals(OrderRules.WHOLE, Completion.smallestOrder(twoLegs(100, 100), 0.05, true, rules(WHOLE, WHOLE)));
        assertEquals(10_000, Completion.smallestOrder(twoLegs(100, 100), 0.05, true, rules(WHOLE, CENTS)));
        RouteLadder farApart = new RouteLadder(8);
        farApart.add(0.35, 100, 0, RouteLadder.BUY, 0.35, 0.07);
        farApart.add(0.45, 100, 1, RouteLadder.BUY, 0.45, 0.07);
        farApart.sort();
        assertEquals(
                OrderRules.WHOLE,
                Completion.smallestOrder(farApart, 0.05, true, rules(WHOLE, CENTS)),
                "the fractional leg is past the cap");
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
