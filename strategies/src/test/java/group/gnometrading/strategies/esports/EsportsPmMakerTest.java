package group.gnometrading.strategies.esports;

import static group.gnometrading.strategies.esports.MakerHarness.BOOKS;
import static group.gnometrading.strategies.esports.MakerHarness.SECOND;
import static group.gnometrading.strategies.esports.MakerHarness.quotes;
import static group.gnometrading.strategies.esports.MakerHarness.takes;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import group.gnometrading.schemas.OrderStatus;
import group.gnometrading.schemas.OrderType;
import group.gnometrading.schemas.Side;
import group.gnometrading.strategies.esports.MakerHarness.Sent;
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
