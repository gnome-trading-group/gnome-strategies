package group.gnometrading.strategies.esports;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Reading the venues' feeds into one match state, live and replayed. */
class GameWatchTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static JsonNode json(String text) {
        try {
            return MAPPER.readTree(text);
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Test
    void theCombinedStateIsEndedThenLiveThenNotStarted() {
        MatchState live = new MatchState(MatchState.LIVE, MatchState.POLYMARKET, 5);
        MatchState notStarted = new MatchState(MatchState.NOT_STARTED, MatchState.KALSHI, 3);
        MatchState ended = new MatchState(MatchState.ENDED, MatchState.KALSHI, 9);

        assertEquals(live, MatchState.combine(new MatchState[] {null, live, notStarted, null}));
        assertEquals(ended, MatchState.combine(new MatchState[] {null, live, ended, null}));
        assertEquals(MatchState.NONE, MatchState.combine(new MatchState[4]));
    }

    @Test
    void theVenuesStatesReadAsOneVocabulary() {
        assertEquals(MatchState.ENDED, LiveGameWatch.polymarketStatus(json("{\"live\":true,\"ended\":true}")));
        assertEquals(MatchState.LIVE, LiveGameWatch.polymarketStatus(json("{\"live\":true}")));
        assertEquals(MatchState.NOT_STARTED, LiveGameWatch.polymarketStatus(json("{\"live\":false}")));
        assertEquals(MatchState.ENDED, LiveGameWatch.kalshiStatus(json("{\"status\":\"Complete\"}")));
        assertEquals(MatchState.LIVE, LiveGameWatch.kalshiStatus(json("{\"status\":\"x\",\"is_live\":true}")));
        assertEquals(MatchState.NOT_STARTED, LiveGameWatch.kalshiStatus(json("{\"status\":\"unplayed\"}")));
        assertTrue(LiveGameWatch.polymarketEventEnded(json("{\"ended\":false,\"closed\":true}")));
        assertFalse(LiveGameWatch.polymarketEventEnded(json("{\"ended\":false,\"closed\":false}")));
    }

    @Test
    void aReplayReadsTheStateAsOfEachMoment() {
        ReplayGameWatch watch = new ReplayGameWatch(List.of(
                List.of(300L, "ended", "kalshi"),
                List.of(100L, "not_started", "polymarket"),
                List.of(200L, "live", "polymarket")));

        assertEquals(MatchState.NONE, watch.state(99));
        assertEquals(MatchState.NOT_STARTED, watch.state(100).status());
        assertEquals(new MatchState(MatchState.LIVE, MatchState.POLYMARKET, 200), watch.state(250));
        assertEquals(MatchState.ENDED, watch.state(1_000).status());
        assertFalse(watch.wasLive(199));
        assertTrue(watch.wasLive(301), "live between two callbacks still counts as played");
        assertEquals(42, watch.heardNanos(42));
    }

    @Test
    void aMatchLiveThenEndedWasLive() {
        LiveGameWatch watch = new LiveGameWatch(url -> null, () -> 7);
        watch.set(MatchState.POLYMARKET, MatchState.LIVE);
        watch.set(MatchState.POLYMARKET, MatchState.ENDED);
        assertTrue(watch.wasLive(0));
        assertEquals(MatchState.ENDED, watch.state(0).status());
    }

    @Test
    void freshnessCountsOnlySourcesThatHaveReportedTheMatch() {
        long[] now = {100};
        LiveGameWatch watch = new LiveGameWatch(url -> null, () -> now[0]);
        watch.set(MatchState.KALSHI, -1);
        assertEquals(100, watch.heardNanos(0), "before any source reports, any answer counts");

        now[0] = 200;
        watch.set(MatchState.POLYMARKET, MatchState.LIVE);
        now[0] = 300;
        watch.set(MatchState.KALSHI, -1);
        assertEquals(200, watch.heardNanos(0), "Kalshi answers but hasn't reported the match");
    }

    @Test
    void theTextHeartbeatIsAnsweredAndCountsAsAnAnswer() {
        LiveGameWatch watch = new LiveGameWatch(url -> null, () -> 5);
        assertTrue(watch.onSportsMessage("ping", 1));
        assertEquals(5, watch.heardNanos(0));
        assertFalse(watch.onSportsMessage("not json", 1));
    }

    @Test
    void theSportsSocketIsReadForTheMatchsGameOnly() {
        LiveGameWatch watch = new LiveGameWatch(url -> null, () -> 5);
        watch.onSportsMessage("{\"gameId\":2,\"live\":true}", 1);
        assertEquals(MatchState.NONE, watch.state(0));

        watch.onSportsMessage("[{\"gameId\":3,\"ended\":true},{\"gameId\":1,\"live\":true}]", 1);
        assertEquals(new MatchState(MatchState.LIVE, MatchState.POLYMARKET, 5), watch.state(0));
    }

    @Test
    void kalshisLiveDataIsFoundByTickerAndPolled() throws Exception {
        List<String> asked = new ArrayList<>();
        LiveGameWatch watch = new LiveGameWatch(
                url -> {
                    asked.add(url);
                    if (url.contains("/milestones?")) {
                        return json("{\"milestones\":[{\"id\":\"m-other\",\"related_event_tickers\":[\"OTHER\"]},"
                                + "{\"id\":\"m-1\",\"type\":\"esports_match\","
                                + "\"related_event_tickers\":[\"KXDOTA2GAME-X\"]}]}");
                    }
                    return json("{\"live_data\":{\"details\":{\"status\":\"live\"}}}");
                },
                () -> 9);

        String url = watch.kalshiLiveDataUrl("KXDOTA2GAME-X");
        assertEquals(LiveGameWatch.KALSHI_API + "/live_data/esports_match/milestone/m-1", url);
        assertTrue(watch.pollKalshi(url));
        assertEquals(new MatchState(MatchState.LIVE, MatchState.KALSHI, 9), watch.state(0));
        assertTrue(asked.get(0).contains("related_event_ticker=KXDOTA2GAME-X"));
    }

    @Test
    void aFailedPollKeepsTheLastState() {
        LiveGameWatch watch = new LiveGameWatch(
                url -> {
                    throw new IllegalStateException("HTTP 503");
                },
                () -> 9);
        watch.set(MatchState.KALSHI, MatchState.LIVE);
        assertFalse(watch.pollKalshi("https://example/live"));
        assertEquals(MatchState.LIVE, watch.state(0).status());
    }

    @Test
    void theGameIdIsReadFromThePolymarketEvent() throws Exception {
        LiveGameWatch watch = new LiveGameWatch(url -> json("[{\"gameId\":12345}]"), () -> 1);
        assertEquals(12345, watch.gameId("cs2-a-b-2026-11-12"));
    }
}
