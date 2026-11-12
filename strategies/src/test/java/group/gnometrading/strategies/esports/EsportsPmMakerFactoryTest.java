package group.gnometrading.strategies.esports;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import group.gnometrading.SecurityMaster;
import group.gnometrading.strategies.StrategyAgent;
import group.gnometrading.strategies.StrategyFactory;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** The session config gnome-launcher's esports_game rule writes builds the maker by its class name. */
class EsportsPmMakerFactoryTest {

    @Test
    void theLaunchersArgumentsBuildTheMakerWithTheRestDefaulted() {
        MakerHarness.FakeFacts facts = new MakerHarness.FakeFacts(MakerHarness.minSizes(5));
        SecurityMaster securityMaster = mock(SecurityMaster.class);
        when(securityMaster.getListing(anyInt())).thenAnswer(call -> facts.listing(call.getArgument(0)));
        when(securityMaster.getFeeTimeline(anyInt())).thenAnswer(call -> facts.feeTimeline(call.getArgument(0)));

        // As the launcher writes them (STRATEGY_ARGS_JSON), with the rule's own and without a Kalshi event ticker.
        Map<String, Object> args = Map.of(
                "legs",
                MakerHarness.LEGS,
                "league",
                "dota2",
                "subject_id",
                120353L,
                "scheduled_start",
                "2026-11-12T16:45:00+00:00",
                "max_locked_usd",
                5000,
                "game_states",
                List.of());

        StrategyAgent agent = StrategyFactory.createWithOwnBuffers(
                EsportsPmMaker.class.getName(), 16, listingId -> null, securityMaster, args);

        assertInstanceOf(EsportsPmMaker.class, agent);
    }
}
