package group.gnometrading.strategies.esports;

import static group.gnometrading.strategies.esports.MakerHarness.BOOKS;
import static group.gnometrading.strategies.esports.MakerHarness.SECOND;
import static group.gnometrading.strategies.esports.MakerHarness.quotes;
import static group.gnometrading.strategies.esports.MakerHarness.takes;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import group.gnometrading.schemas.OrderRole;
import group.gnometrading.schemas.OrderStatus;
import group.gnometrading.schemas.OrderType;
import group.gnometrading.schemas.RejectReason;
import group.gnometrading.schemas.Side;
import group.gnometrading.strategies.esports.MakerHarness.Sent;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** The maker's quoting, hedging and play gate, on the Python tests' fixtures. */
class EsportsPmMakerTest {

    private static final double EPS = 1e-9;
    private static final long START_NS = 1_794_499_200L * SECOND; // 2026-11-12T16:00:00Z
    private static final String START = "2026-11-12T16:00:00+00:00";
    private static final List<Object> NOT_STARTED = List.of(START_NS - 600 * SECOND, "not_started", "polymarket");
    private static final List<Object> LIVE = List.of(START_NS + 60 * SECOND, "live", "polymarket");
    private static final List<Object> ENDED = List.of(START_NS + 7200 * SECOND, "ended", "kalshi");
    private static final long ENDED_NS = START_NS + 7200 * SECOND;

    private static MakerHarness maker(Object... keyValues) {
        Map<String, Object> args = new HashMap<>();
        for (int i = 0; i < keyValues.length; i += 2) {
            args.put((String) keyValues[i], keyValues[i + 1]);
        }
        return new MakerHarness(args);
    }

    /** A session on the given venues' legs ({@link MakerHarness#LEGS} indexes) and specs. */
    private static MakerHarness on(int[] venues, Map<Integer, double[]> specs, Object... keyValues) {
        Map<String, Object> args = new HashMap<>();
        for (int i = 0; i < keyValues.length; i += 2) {
            args.put((String) keyValues[i], keyValues[i + 1]);
        }
        List<Map<String, Object>> legs = new ArrayList<>();
        for (int venue : venues) {
            legs.add(MakerHarness.LEGS.get(venue));
        }
        args.put("legs", legs);
        return new MakerHarness(args, specs);
    }

    private static final int[] PM_US_ONLY = {0, 2};
    private static final int[] KALSHI_ONLY = {0, 1};
    private static final int[] ALL_THREE = {0, 1, 2};

    /** Fills {@code contracts} of the team A bid and returns the hedges sent. */
    private static List<Sent> fillTeamA(MakerHarness s, Object[][] books, double contracts) {
        s.feed(books, 0);
        s.hold(101, contracts);
        return takes(s.report(101, OrderStatus.PARTIALLY_FILLED, contracts, 0));
    }

    private static double hedged(List<Sent> hedges, long securityId) {
        return hedges.stream()
                .filter(x -> x.securityId() == securityId)
                .mapToDouble(Sent::takeContracts)
                .sum();
    }

    private static MakerHarness session(Object... keyValues) {
        MakerHarness harness = maker(keyValues);
        return harness;
    }

    private static Object[][] with(Object[][] books, int listingId, double[][] asks, double[][] bids) {
        Object[][] out = books.clone();
        for (int i = 0; i < out.length; i++) {
            if ((Integer) out[i][0] == listingId) {
                out[i] = new Object[] {listingId, asks, bids};
            }
        }
        return out;
    }

    // ---- quoting ------------------------------------------------------------------------------------------------

    @Test
    void bidsEachTeamPostOnlyFromTheCheapestHedge() {
        MakerHarness s = maker("min_edge", 0.01);
        List<Sent> sent = s.feed(BOOKS, 0);
        Map<Long, double[]> q = quotes(sent);

        // Team A's hedge is team B: Polymarket US asks 0.48x60 is the cheapest all-in.
        double hedgeB = 0.48 + 0.0695 * 0.48 * 0.52;
        assertEquals(Math.floor((1 - hedgeB - 0.01) * 100) / 100, q.get(1101L)[0], EPS);
        assertEquals(60, q.get(1101L)[1], EPS);
        assertTrue(sent.stream().filter(x -> x.securityId() == 1101L).allMatch(Sent::postOnly));
        assertTrue(q.containsKey(1102L));
    }

    @Test
    void unchangedBooksSendNothing() {
        MakerHarness s = maker();
        s.feed(BOOKS, 0);
        assertEquals(List.of(), s.feed(BOOKS, 0));
    }

    @Test
    void aFillPullsTheBidsAndHedgesAtOnceThroughTheSlippageCap() {
        MakerHarness s = maker();
        s.feed(BOOKS, 0);
        s.hold(101, 20);

        List<Sent> out = s.report(101, OrderStatus.PARTIALLY_FILLED, 20, 0);

        List<Sent> pulls =
                out.stream().filter(x -> x.exchangeId() == MakerHarness.PMI).toList();
        assertEquals(2, pulls.size());
        assertTrue(pulls.stream().allMatch(Sent::isPull));
        List<Sent> hedge = takes(out);
        assertEquals(1, hedge.size());
        assertEquals(1302L, hedge.get(0).securityId());
        assertEquals(Side.Bid, hedge.get(0).takeSide());
        assertEquals(OrderType.LIMIT, hedge.get(0).takeOrderType());
        assertEquals(20, hedge.get(0).takeContracts(), EPS);
        assertEquals(0.53, hedge.get(0).limit(), EPS, "0.48 best ask plus 0.05 completion_slippage");
    }

    @Test
    void aHedgeIsTaggedWithTheFillThatOpenedItsGap() {
        MakerHarness s = maker();
        s.feed(BOOKS, 0);
        s.hold(101, 20);

        Sent hedge = takes(s.reportOrder(101, 31L, OrderStatus.PARTIALLY_FILLED, 20, 0))
                .get(0);

        assertEquals(OrderRole.HEDGE, hedge.takeRole());
        assertEquals(31L, hedge.episodeId());
        List<Sent> bids = s.feed(BOOKS, 1).stream().filter(Sent::isBid).toList();
        assertTrue(bids.stream().allMatch(b -> b.takeRole() == OrderRole.NONE && b.episodeId() == 0));
    }

    @Test
    void withoutCrossingTheHedgeIsLimitedAtTheLevelItWasCostedOn() {
        MakerHarness s = maker("cross_completions", false);
        s.feed(BOOKS, 0);
        s.hold(101, 20);

        Sent hedge = takes(s.report(101, OrderStatus.PARTIALLY_FILLED, 20, 0)).get(0);

        assertEquals(0.48, hedge.limit(), EPS);
    }

    @Test
    void aRejectedBidIsNotResentUnchanged() {
        MakerHarness s = maker();
        assertTrue(quotes(s.feed(BOOKS, 0)).containsKey(1101L));

        List<Sent> again = s.report(101, OrderStatus.REJECTED, 0, 0);

        assertFalse(quotes(again).containsKey(1101L));
    }

    @Test
    void aJumpingHedgePullsTheBidUntilItSettles() {
        MakerHarness s = maker("calm_move", 0.03, "calm_seconds", 5.0);
        assertTrue(quotes(s.feed(BOOKS, 0)).containsKey(1101L));
        Object[][] moved = with(BOOKS, 302, new double[][] {{0.40, 60}}, new double[][] {{0.38, 100}});

        assertEquals(0, quotes(s.feed(moved, SECOND)).get(1101L)[1], EPS, "pulled");
        assertTrue(quotes(s.feed(moved, 7 * SECOND)).get(1101L)[1] > 0, "back once the hedge sat still");
        assertEquals(Map.of(), quotes(s.feed(moved, 7 * SECOND)));
    }

    @Test
    void withoutCalmMoveTheBidFollowsTheHedge() {
        MakerHarness s = maker();
        s.feed(BOOKS, 0);
        Object[][] moved = with(BOOKS, 302, new double[][] {{0.40, 60}}, new double[][] {{0.38, 100}});
        assertTrue(quotes(s.feed(moved, SECOND)).get(1101L)[1] > 0);
    }

    @Test
    void aMarketTheMakerVenueDoesntListEndsTheSessionInsteadOfFailingIt() {
        MakerHarness s = maker("legs", MakerHarness.LEGS.subList(1, 3));
        assertTrue(s.feed(BOOKS, 0).stream().noneMatch(Sent::isBid));
        assertEquals(List.of("polymarket doesn't list this market; nothing to quote"), s.exits);
    }

    // ---- hedging ------------------------------------------------------------------------------------------------

    @Test
    void anUnfilledHedgeIsPlannedAgainOnTheNextHedgeBookNotASecondLater() {
        MakerHarness s = maker();
        s.feed(BOOKS, 0);
        s.hold(101, 20);
        s.report(101, OrderStatus.PARTIALLY_FILLED, 20, 0);

        List<Sent> after = s.report(302, OrderStatus.CANCELED, 0, 10_000_000);
        assertEquals(List.of(), takes(after), "the same book would only miss again");
        List<Sent> quoted = s.book(101, new double[][] {{0.60, 100}}, new double[][] {{0.41, 100}}, 20_000_000, false);
        assertEquals(List.of(), takes(quoted), "a book on the quoted venue says nothing about the hedge");

        List<Sent> moved =
                takes(s.book(302, new double[][] {{0.50, 60}}, new double[][] {{0.44, 100}}, 30_000_000, false));
        assertEquals(1, moved.size(), "planned again 30 ms after the fill, on the hedge book's next update");
        assertEquals(0.55, moved.get(0).limit(), EPS);
    }

    @Test
    void aRejectedHedgeWaitsAndIsNotResentUnchanged() {
        MakerHarness s = maker("cross_completions", false, "completion_retry_seconds", 1.0);
        s.feed(BOOKS, 0);
        s.hold(101, 20);
        s.report(101, OrderStatus.PARTIALLY_FILLED, 20, 0);
        s.report(302, OrderStatus.REJECTED, 0, 0);

        assertEquals(List.of(), takes(s.feed(new Object[][] {BOOKS[5]}, 5 * SECOND)));
        List<Sent> moved =
                takes(s.book(302, new double[][] {{0.47, 60}}, new double[][] {{0.44, 100}}, 6 * SECOND, false));
        assertEquals(1, moved.size());
        assertEquals(1302L, moved.get(0).securityId());
        assertEquals(20, moved.get(0).takeContracts(), EPS);
        assertEquals(0.47, moved.get(0).limit(), EPS);
    }

    @Test
    void theHedgeSellsTheRivalOnKalshiWhenThatIsCheapest() {
        MakerHarness s = maker();
        Object[][] books = with(BOOKS, 201, new double[][] {{0.70, 100}}, new double[][] {{0.60, 100}});
        books = with(books, 302, new double[][] {{0.55, 60}}, new double[][] {{0.44, 100}});
        s.feed(books, 0);
        s.hold(101, 20);

        Sent hedge = takes(s.report(101, OrderStatus.PARTIALLY_FILLED, 20, 0)).get(0);

        assertEquals(1201L, hedge.securityId(), "sell team A on Kalshi: B for 0.40");
        assertEquals(Side.Ask, hedge.takeSide());
        assertEquals(0.55, hedge.limit(), EPS, "down to 1 - (0.40 + 0.05)");
    }

    // ---- lots, minimums and remainders ---------------------------------------------------------------------------

    @Test
    void anOddFillIsHedgedToTheCentWherePolymarketUsIsTheHedge() {
        List<Sent> hedges = fillTeamA(on(PM_US_ONLY, MakerHarness.venueSpecs(1)), BOOKS, 2.7675);

        assertEquals(1, hedges.size());
        assertEquals(2.77, hedged(hedges, 1302L), EPS, "2.76, and the 0.0075 left is most of a cent");
    }

    @Test
    void withoutRoundingUpAFractionalHedgeLeavesOnlyWhatIsUnderALot() {
        List<Sent> hedges =
                fillTeamA(on(PM_US_ONLY, MakerHarness.venueSpecs(1), "round_up_remainder", 0.0), BOOKS, 2.7675);

        assertEquals(2.76, hedged(hedges, 1302L), EPS);
    }

    @Test
    void anOddFillOnWholeContractKalshiIsRoundedUpToTheNextContract() {
        List<Sent> hedges = fillTeamA(on(KALSHI_ONLY, MakerHarness.venueSpecs(1)), BOOKS, 2.7675);

        assertEquals(1, hedges.size());
        assertEquals(3, hedged(hedges, 1202L), EPS, "2, plus the 0.7675 left as a third contract");
    }

    @Test
    void withoutRoundingUpWholeContractKalshiHedgesTheWholeContractsAsToday() {
        List<Sent> hedges =
                fillTeamA(on(KALSHI_ONLY, MakerHarness.venueSpecs(1), "round_up_remainder", 0.0), BOOKS, 2.7675);

        assertEquals(2, hedged(hedges, 1202L), EPS);
    }

    @Test
    void anOddFillOnFractionalKalshiIsHedgedToTheCent() {
        List<Sent> hedges = fillTeamA(on(KALSHI_ONLY, MakerHarness.venueSpecs(0.01)), BOOKS, 2.7675);

        assertEquals(2.77, hedged(hedges, 1202L), EPS);
    }

    @Test
    void onThreeVenuesWholeKalshiTakesTheContractsAndPolymarketUsTheFraction() {
        Object[][] books = with(BOOKS, 202, new double[][] {{0.45, 100}}, new double[][] {{0.42, 100}});

        List<Sent> hedges = fillTeamA(on(ALL_THREE, MakerHarness.venueSpecs(1)), books, 2.7675);

        assertEquals(2, hedged(hedges, 1202L), EPS, "Kalshi is cheapest, in whole contracts");
        assertEquals(0.77, hedged(hedges, 1302L), EPS, "the fraction goes to Polymarket US, to the cent");
    }

    @Test
    void aGapUnderAContractIsHedgedOnlyWhereAVenueTradesFractions() {
        assertEquals(0.3, hedged(fillTeamA(on(ALL_THREE, MakerHarness.venueSpecs(1)), BOOKS, 0.3), 1302L), EPS);
        assertEquals(List.of(), fillTeamA(maker(), BOOKS, 0.3), "whole-contract hedge venues can't take 0.3");
    }

    @Test
    void aBidWorthLessThanTheVenuesSmallestNotionalIsNotSent() {
        Map<Integer, double[]> specs = MakerHarness.venueSpecs(1);
        specs.put(101, new double[] {0.01, 5, 4.5});
        specs.put(102, new double[] {0.01, 5, 4.5});
        MakerHarness s = on(ALL_THREE, specs, "quote_size", 10.0);

        Map<Long, double[]> q = quotes(s.feed(BOOKS, 0));

        assertEquals(0.49, q.get(1101L)[0], EPS);
        assertEquals(10, q.get(1101L)[1], EPS, "10 at 0.49 is $4.90");
        assertFalse(q.containsKey(1102L), "10 at 0.41 is $4.10, under $4.50");
    }

    @Test
    void aFractionalHedgeTheVenueRefusesIsNeverSentFractionalAgain() {
        MakerHarness s = on(KALSHI_ONLY, MakerHarness.venueSpecs(0.01), "round_up_remainder", 0.0);
        assertEquals(2.76, hedged(fillTeamA(s, BOOKS, 2.7675), 1202L), EPS);

        s.report(202, OrderStatus.REJECTED, 0, 0, RejectReason.EXCHANGE_REJECTED);
        List<Sent> again =
                takes(s.book(202, new double[][] {{0.49, 100}}, new double[][] {{0.42, 100}}, SECOND, false));

        assertEquals(1, again.size());
        assertEquals(2, again.get(0).takeContracts(), EPS, "Kalshi now takes whole contracts only");
    }

    // ---- play gate ----------------------------------------------------------------------------------------------

    @Test
    void theMakerBidsOnlyWhileTheMatchIsLive() {
        MakerHarness s = session("game_states", List.of(NOT_STARTED, LIVE), "scheduled_start", START);
        assertEquals(Map.of(), quotes(s.feed(BOOKS, START_NS)));
        assertTrue(quotes(s.feed(BOOKS, START_NS + 61 * SECOND)).containsKey(1101L));
    }

    @Test
    void theMakerPullsItsBidsForGoodNearADecidedResult() {
        MakerHarness s = session("game_states", List.of(LIVE), "scheduled_start", START);
        assertTrue(quotes(s.feed(BOOKS, START_NS + 61 * SECOND)).containsKey(1101L));

        List<Sent> pulled = s.book(
                201, new double[][] {{0.995, 100}}, new double[][] {{0.985, 100}}, START_NS + 62 * SECOND, false);

        assertFalse(pulled.isEmpty());
        assertTrue(pulled.stream().allMatch(Sent::isPull));
        assertEquals(Map.of(), quotes(s.feed(BOOKS, START_NS + 63 * SECOND)));
    }

    @Test
    void oneStrayQuoteOnAThinBookDoesntTakeTheBidsDown() {
        MakerHarness s = session("game_states", List.of(LIVE), "scheduled_start", START);
        s.feed(BOOKS, START_NS + 61 * SECOND);
        List<Sent> out = s.book(201, new double[][] {{0.99, 1}}, new double[][] {}, START_NS + 62 * SECOND, false);
        assertTrue(out.stream().noneMatch(Sent::isPull));
    }

    @Test
    void theMakerPullsItsBidsForGoodWhenAVenueIsSweptToTheResult() {
        MakerHarness s = session("game_states", List.of(LIVE), "scheduled_start", START);
        assertTrue(quotes(s.feed(BOOKS, START_NS + 61 * SECOND)).containsKey(1101L));
        s.book(201, new double[][] {{0.01, 500}}, new double[][] {}, START_NS + 62 * SECOND, false);

        List<Sent> pulled =
                s.book(202, new double[][] {}, new double[][] {{0.99, 500}}, START_NS + 62 * SECOND + 10, false);

        assertFalse(pulled.isEmpty());
        assertTrue(pulled.stream().allMatch(Sent::isPull));
        assertEquals(Map.of(), quotes(s.feed(BOOKS, START_NS + 63 * SECOND)));
    }

    @Test
    void oneSweptBookAloneDoesntTakeTheBidsDown() {
        MakerHarness s = session("game_states", List.of(LIVE), "scheduled_start", START);
        s.feed(BOOKS, START_NS + 61 * SECOND);
        List<Sent> out = s.book(201, new double[][] {{0.01, 500}}, new double[][] {}, START_NS + 62 * SECOND, false);
        assertTrue(out.stream().noneMatch(Sent::isPull));
    }

    @Test
    void booksSweptOnAVenueWhereTheTeamsShareOneBookDontTakeTheBidsDown() {
        MakerHarness s = session("game_states", List.of(LIVE), "scheduled_start", START);
        s.feed(BOOKS, START_NS + 61 * SECOND);
        s.book(301, new double[][] {{0.01, 500}}, new double[][] {}, START_NS + 62 * SECOND, false);
        List<Sent> out =
                s.book(302, new double[][] {}, new double[][] {{0.99, 500}}, START_NS + 62 * SECOND + 10, false);
        assertTrue(out.stream().noneMatch(Sent::isPull));
    }

    @Test
    void booksSweptShortOfTheGuardDontTakeTheBidsDown() {
        MakerHarness s = session("game_states", List.of(LIVE), "scheduled_start", START);
        s.feed(BOOKS, START_NS + 61 * SECOND);
        s.book(201, new double[][] {{0.05, 500}}, new double[][] {}, START_NS + 62 * SECOND, false);
        List<Sent> out =
                s.book(202, new double[][] {}, new double[][] {{0.95, 500}}, START_NS + 62 * SECOND + 10, false);
        assertTrue(out.stream().noneMatch(Sent::isPull));
    }

    @Test
    void sweptBooksOnTwoVenuesDontAddUpToAResult() {
        MakerHarness s = session("game_states", List.of(LIVE), "scheduled_start", START);
        s.feed(BOOKS, START_NS + 61 * SECOND);
        s.book(201, new double[][] {{0.01, 500}}, new double[][] {}, START_NS + 62 * SECOND, false);
        List<Sent> out =
                s.book(302, new double[][] {}, new double[][] {{0.99, 500}}, START_NS + 62 * SECOND + 10, false);
        assertTrue(out.stream().noneMatch(Sent::isPull));
    }

    @Test
    void theMakerPullsItsBidsAtTheEndThenLeavesOnceFillsHaveHadTime() {
        MakerHarness s = session("game_states", List.of(NOT_STARTED, LIVE, ENDED), "scheduled_start", START);
        s.feed(BOOKS, ENDED_NS - SECOND);

        List<Sent> atEnd = s.feed(BOOKS, ENDED_NS + SECOND);
        assertTrue(atEnd.stream().anyMatch(Sent::isPull) && atEnd.stream().noneMatch(Sent::isBid));
        assertEquals(List.of(), s.exits, "not gone yet");

        s.feed(BOOKS, ENDED_NS + 31 * SECOND);
        assertEquals(List.of("the match is over (kalshi)"), s.exits);
    }

    @Test
    void anUnequalPositionIsCompletedBeforeTheSessionLeaves() {
        MakerHarness s = session("game_states", List.of(LIVE, ENDED), "scheduled_start", START);
        s.feed(BOOKS, ENDED_NS - SECOND);
        s.hold(101, 20);

        List<Sent> out = s.feed(BOOKS, ENDED_NS + 40 * SECOND);

        assertEquals(1, takes(out).size());
        assertEquals(List.of(), s.exits);
    }

    @Test
    void itLeavesEndedMaxSecondsAfterTheEndWhateverItHolds() {
        MakerHarness s =
                session("game_states", List.of(LIVE, ENDED), "scheduled_start", START, "ended_max_seconds", 600.0);
        s.hold(302, 30);
        s.feed(BOOKS, ENDED_NS + 601 * SECOND);
        assertEquals(List.of("the match is over (kalshi); left after ended_max_seconds"), s.exits);
    }

    @Test
    void aMatchThatWasLiveIsNeverGivenUpOn() {
        List<Object> blip = List.of(START_NS + 4 * 3600 * SECOND, "not_started", "kalshi");
        MakerHarness s = session(
                "game_states", List.of(NOT_STARTED, LIVE, blip), "scheduled_start", START, "never_live_hours", 3.0);
        s.feed(BOOKS, START_NS + 3600 * SECOND);
        assertTrue(s.feed(BOOKS, START_NS + 5 * 3600 * SECOND).stream().noneMatch(Sent::isBid));
        assertEquals(List.of(), s.exits);
    }

    @Test
    void itEndsAMatchNeverLivePastItsStartPlusNeverLiveHours() {
        MakerHarness s =
                session("game_states", List.of(NOT_STARTED), "scheduled_start", START, "never_live_hours", 3.0);
        s.feed(BOOKS, START_NS + 2 * 3600 * SECOND);
        assertEquals(List.of(), s.exits);
        s.feed(BOOKS, START_NS + 3 * 3600 * SECOND);
        assertEquals(List.of("the match wasn't live by its scheduled start plus never_live_hours"), s.exits);
    }

    @Test
    void itEndsAtOnceWhenTheMatchEndsWithoutBeingPlayed() {
        List<Object> forfeit = List.of(START_NS - 120 * SECOND, "ended", "polymarket_event");
        MakerHarness s = session("game_states", List.of(NOT_STARTED, forfeit), "scheduled_start", START);
        assertTrue(s.feed(BOOKS, START_NS).stream().noneMatch(Sent::isBid));
        assertEquals(List.of("the match ended without being played (polymarket_event)"), s.exits);
    }

    @Test
    void aRestartedSessionStillCompletesWhatItHoldsWhenItFirstHearsTheEnd() {
        MakerHarness s = session("game_states", List.of(ENDED), "scheduled_start", START);
        s.hold(101, 20);
        List<Sent> out = s.feed(BOOKS, ENDED_NS + SECOND);
        assertEquals(1, takes(out).size());
        assertEquals(List.of(), s.exits);
    }

    @Test
    void completionIsNeverHeldBackByTheMatchState() {
        List<Object> notLive = List.of(START_NS + 300 * SECOND, "not_started", "polymarket");
        MakerHarness s = session("game_states", List.of(NOT_STARTED, LIVE, notLive), "scheduled_start", START);
        s.feed(BOOKS, START_NS + 100 * SECOND);
        s.hold(101, 30);
        s.hold(302, 10);

        List<Sent> out = s.report(101, OrderStatus.PARTIALLY_FILLED, 30, START_NS + 400 * SECOND);

        assertEquals(20, takes(out).get(0).takeContracts(), EPS);
    }

    @Test
    void aFeedGoneQuietStopsQuoting() {
        LiveGameWatch quiet = new LiveGameWatch(url -> null, () -> START_NS);
        quiet.set(MatchState.POLYMARKET, MatchState.LIVE);
        MakerHarness stale = session("stale_seconds", 60.0);
        stale.strategy.follow(quiet);
        assertTrue(stale.feed(BOOKS, START_NS + 61 * SECOND).stream().noneMatch(Sent::isBid));

        MakerHarness fresh = session("stale_seconds", 60.0);
        fresh.strategy.follow(quiet);
        assertTrue(quotes(fresh.feed(BOOKS, START_NS + 30 * SECOND)).containsKey(1101L));
    }

    @Test
    void withoutAFeedTheSessionQuotesAtOnce() {
        assertTrue(quotes(maker().feed(BOOKS, 0)).containsKey(1101L));
    }

    @Test
    void theMatchStateIsRecordedOnEachChange() {
        MakerHarness s = session("game_states", List.of(NOT_STARTED, LIVE), "scheduled_start", START);
        assertEquals(EsportsPmMaker.CLOSED, s.strategy.gameGate(START_NS));
        assertEquals(EsportsPmMaker.OPEN, s.strategy.gameGate(START_NS + 61 * SECOND));
    }
}
