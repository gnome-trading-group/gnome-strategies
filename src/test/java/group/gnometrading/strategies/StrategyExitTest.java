package group.gnometrading.strategies;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import group.gnometrading.oms.position.Position;
import group.gnometrading.schemas.Intent;
import group.gnometrading.schemas.IntentDecoder;
import group.gnometrading.schemas.Mbp10Schema;
import group.gnometrading.schemas.OrderExecutionReport;
import group.gnometrading.schemas.OrderType;
import group.gnometrading.schemas.Schema;
import group.gnometrading.schemas.Side;
import group.gnometrading.sequencer.GlobalSequence;
import group.gnometrading.sequencer.SequencedPoller;
import group.gnometrading.sequencer.SequencedRingBuffer;
import group.gnometrading.sm.Exchange;
import group.gnometrading.sm.Listing;
import group.gnometrading.sm.Security;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class StrategyExitTest {

    private static final long SECOND = 1_000_000_000L;
    private static final long UNIT = 1_000_000L;

    /** What one intent asked for, decoded as it was published. */
    private record Sent(
            int exchangeId,
            long securityId,
            long bidSize,
            long askSize,
            long takeSize,
            Side takeSide,
            OrderType takeOrderType) {

        boolean isCancel() {
            return bidSize == IntentDecoder.bidSizeNullValue()
                    && askSize == IntentDecoder.askSizeNullValue()
                    && takeSize == IntentDecoder.takeSizeNullValue();
        }
    }

    private static final class TestStrategy extends StrategyAgent {
        int marketData;

        TestStrategy(SequencedRingBuffer<Intent> intents, Map<Integer, Position> positions, long[] clock) {
            super(
                    7,
                    new SequencedRingBuffer<>(Mbp10Schema::new, new GlobalSequence()),
                    new SequencedRingBuffer<>(OrderExecutionReport::new, new GlobalSequence()),
                    intents,
                    positions::get,
                    null,
                    () -> clock[0]);
        }

        @Override
        protected void onMarketData(Schema data) {
            marketData++;
        }

        @Override
        protected void onExecutionReport(OrderExecutionReport report) {}
    }

    private final long[] clock = {100 * SECOND};
    private final Map<Integer, Position> positions = new HashMap<>();
    private final List<Sent> sent = new ArrayList<>();
    private final List<ExitResult> results = new ArrayList<>();
    private SequencedPoller intentPoller;
    private TestStrategy strategy;

    @BeforeEach
    void setUp() {
        SequencedRingBuffer<Intent> intents = new SequencedRingBuffer<>(Intent::new, new GlobalSequence());
        Intent decoded = new Intent();
        intentPoller = intents.createPoller((seq, templateId, buffer, length) -> {
            decoded.wrap(buffer);
            IntentDecoder d = decoded.decoder;
            sent.add(new Sent(
                    d.exchangeId(),
                    d.securityId(),
                    d.bidSize(),
                    d.askSize(),
                    d.takeSize(),
                    d.takeSide(),
                    d.takeOrderType()));
        });
        position(1, 0);
        position(2, 0);
        strategy = new TestStrategy(intents, positions, clock);
        strategy.configureExit(List.of(listing(1, 10, 100), listing(2, 20, 200)), results::add);
    }

    @Test
    void anExitCancelsEveryQuoteAndFinishesOnceNothingIsWorking() throws Exception {
        positions.get(1).leavesBuyQty = UNIT;
        positions.get(2).netQuantity = 3 * UNIT;

        assertTrue(strategy.requestExit("match went live", false, 60_000));
        pass();

        assertEquals(2, sent.size());
        assertTrue(sent.stream().allMatch(Sent::isCancel));
        assertEquals(List.of(10, 20), sent.stream().map(Sent::exchangeId).toList());
        assertEquals(List.of(100L, 200L), sent.stream().map(Sent::securityId).toList());
        assertTrue(results.isEmpty(), "a bid is still working");

        positions.get(1).leavesBuyQty = 0;
        pass();

        assertEquals(1, results.size());
        ExitResult result = results.get(0);
        assertEquals("match went live", result.reason());
        assertFalse(result.timedOut());
        assertEquals(List.of(new ExitResult.HeldPosition(2, 3 * UNIT)), result.held());
        assertFalse(result.leftPositions(), "keeping a position was the strategy's choice");
        assertEquals(2, sent.size(), "nothing is sold without flatten");
    }

    @Test
    void theStrategyHearsNothingOnceItHasAskedToExit() throws Exception {
        strategy.requestExit("done", false, 60_000);
        strategy.submitMarketData(new Mbp10Schema());
        pass();

        assertEquals(0, strategy.marketData);
    }

    @Test
    void flattenSellsALongWithAMarketOrderSizedToThePosition() throws Exception {
        positions.get(1).netQuantity = 5 * UNIT;

        strategy.requestExit("done", true, 60_000);
        pass();
        assertEquals(2, sent.size(), "the cancels go first");

        clock[0] += WindDown.RETRY_INTERVAL_NANOS;
        pass();
        Sent close = sent.get(2);
        assertEquals(
                new Sent(
                        10,
                        100,
                        IntentDecoder.bidSizeNullValue(),
                        IntentDecoder.askSizeNullValue(),
                        5 * UNIT,
                        Side.Ask,
                        OrderType.MARKET),
                close);

        positions.get(1).netQuantity = 0;
        pass();
        assertEquals(1, results.size());
        assertFalse(results.get(0).timedOut());
        assertTrue(results.get(0).held().isEmpty());
    }

    @Test
    void flattenBuysBackAShort() throws Exception {
        positions.get(2).netQuantity = -2 * UNIT;

        strategy.requestExit("done", true, 60_000);
        pass();
        clock[0] += WindDown.RETRY_INTERVAL_NANOS;
        pass();

        Sent close = sent.get(2);
        assertEquals(20, close.exchangeId());
        assertEquals(Side.Bid, close.takeSide());
        assertEquals(2 * UNIT, close.takeSize());
    }

    @Test
    void aCloseIsNotRepeatedWhileItWorksAndIsResentForWhatAPartialFillLeft() throws Exception {
        positions.get(1).netQuantity = 5 * UNIT;
        strategy.requestExit("done", true, 60_000);
        pass();
        clock[0] += WindDown.RETRY_INTERVAL_NANOS;
        pass();
        assertEquals(3, sent.size());

        positions.get(1).leavesSellQty = 5 * UNIT;
        clock[0] += 5 * WindDown.RETRY_INTERVAL_NANOS;
        pass();
        assertEquals(3, sent.size(), "the close is still working");

        positions.get(1).leavesSellQty = 0;
        positions.get(1).netQuantity = 2 * UNIT;
        pass();
        assertEquals(4, sent.size());
        assertEquals(2 * UNIT, sent.get(3).takeSize());

        pass();
        assertEquals(4, sent.size(), "the OMS gets a retry interval to take the close in before it is judged");
    }

    @Test
    void runningOutOfTimeReportsWhatIsStillHeld() throws Exception {
        positions.get(1).netQuantity = 5 * UNIT;
        strategy.requestExit("done", true, 3_000);
        pass();

        clock[0] += 3 * SECOND;
        pass();

        assertEquals(1, results.size());
        ExitResult result = results.get(0);
        assertTrue(result.timedOut());
        assertTrue(result.leftPositions());
        assertEquals(List.of(new ExitResult.HeldPosition(1, 5 * UNIT)), result.held());

        clock[0] += 10 * SECOND;
        pass();
        assertEquals(1, results.size(), "the session is told once");
    }

    @Test
    void onlyTheFirstRequestCounts() throws Exception {
        assertTrue(strategy.requestExit("first", false, 60_000));
        assertFalse(strategy.requestExit("second", true, 60_000));
        assertTrue(strategy.isExiting());
        pass();

        assertEquals("first", results.get(0).reason());
    }

    @Test
    void anExitRequestedFromAnotherThreadIsPickedUp() throws Exception {
        Thread other = new Thread(() -> strategy.requestExit("from the feed", false, 60_000));
        other.start();
        other.join();
        pass();

        assertEquals("from the feed", results.get(0).reason());
    }

    @Test
    void withoutAnExitConfiguredTheStrategyIsOnlySilenced() throws Exception {
        SequencedRingBuffer<Intent> intents = new SequencedRingBuffer<>(Intent::new, new GlobalSequence());
        TestStrategy unconfigured = new TestStrategy(intents, positions, clock);
        unconfigured.requestExit("done", true, 0);
        unconfigured.submitMarketData(new Mbp10Schema());
        unconfigured.doWork();

        assertEquals(0, unconfigured.marketData);
        assertEquals(0, intents.createPoller((s, t, b, l) -> {}).poll(), "nothing to cancel or close without listings");
    }

    private void pass() throws Exception {
        strategy.doWork();
        intentPoller.poll();
    }

    private void position(int listingId, long net) {
        Position position = new Position();
        position.init(listingId);
        position.netQuantity = net;
        positions.put(listingId, position);
    }

    private static Listing listing(int listingId, int exchangeId, int securityId) {
        return new Listing(
                listingId,
                new Exchange(exchangeId, "EX" + exchangeId, "Exchange", "us-east-1", null),
                new Security(securityId, "SEC", null, null, null, "USD", "USD", "USD", false, false, 0, 0, true, 0),
                String.valueOf(securityId),
                "SEC");
    }
}
