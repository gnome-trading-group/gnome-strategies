package group.gnometrading.strategies;

import static org.junit.jupiter.api.Assertions.assertEquals;

import group.gnometrading.latency.LatencyGrid;
import group.gnometrading.latency.LatencyKey;
import group.gnometrading.latency.MinuteHistograms;
import group.gnometrading.latency.MinuteSummary;
import group.gnometrading.schemas.Intent;
import group.gnometrading.schemas.IntentDecoder;
import group.gnometrading.schemas.Mbp10Schema;
import group.gnometrading.schemas.OrderExecutionReport;
import group.gnometrading.schemas.StreamRecord;
import group.gnometrading.schemas.TriggerKind;
import group.gnometrading.sequencer.GlobalSequence;
import group.gnometrading.sequencer.SequencedPoller;
import group.gnometrading.sequencer.SequencedRingBuffer;
import java.util.ArrayList;
import java.util.List;
import org.agrona.concurrent.EpochNanoClock;
import org.junit.jupiter.api.Test;

/** Every intent carries the event the strategy was handling when it published it. */
class StrategyTriggerTest {

    /** Ticks a microsecond per read, so each stamp in the chain is a distinct, known time. */
    private static final class StepClock implements EpochNanoClock {
        long now = 1_000_000;

        @Override
        public long nanoTime() {
            return now += 1_000;
        }
    }

    /** Publishes one intent per event it handles, and one more from outside any event when asked to. */
    private static final class PublishingStrategy extends StrategyAgent {
        private final Intent intent = new Intent();

        PublishingStrategy(GlobalSequence sequence, EpochNanoClock clock) {
            super(
                    7,
                    new SequencedRingBuffer<>(Mbp10Schema::new, sequence, 64, clock),
                    new SequencedRingBuffer<>(OrderExecutionReport::new, sequence, 64, clock),
                    new SequencedRingBuffer<>(Intent::new, sequence, 64, clock),
                    null,
                    null,
                    clock);
        }

        @Override
        protected void onMarketData(StreamRecord data) {
            publishIntent(intent);
        }

        @Override
        protected void onExecutionReport(OrderExecutionReport report) {
            publishIntent(intent);
        }

        void publishOutsideAnEvent() {
            publishIntent(intent);
        }
    }

    private record Published(
            long triggerSequence,
            TriggerKind kind,
            long triggerTimestamp,
            long originTimestamp,
            long dispatchTimestamp,
            long sent) {}

    @Test
    void anIntentCarriesTheMarketDataItWasAReactionTo() throws Exception {
        final StepClock clock = new StepClock();
        final PublishingStrategy strategy = new PublishingStrategy(new GlobalSequence(), clock);
        final Collector intents = new Collector(strategy);
        final Mbp10Schema book = new Mbp10Schema();
        book.encoder.timestampRecv(555);

        strategy.submitMarketData(book);
        strategy.doWork();

        // Clock reads in order: the book's publish (1,001,000), dispatch (1,002,000), the intent's send (1,003,000).
        assertEquals(
                List.of(new Published(1, TriggerKind.MARKET_DATA, 1_001_000, 555, 1_002_000, 1_003_000)),
                intents.drain());
    }

    @Test
    void anIntentCarriesTheExecutionReportItWasAReactionTo() throws Exception {
        final StepClock clock = new StepClock();
        final GlobalSequence sequence = new GlobalSequence();
        final PublishingStrategy strategy = new PublishingStrategy(sequence, clock);
        final Collector intents = new Collector(strategy);
        final OrderExecutionReport report = new OrderExecutionReport();
        report.encoder.timestampRecv(777);

        strategy.submitExecReport(report);
        strategy.doWork();

        assertEquals(
                List.of(new Published(1, TriggerKind.EXEC_REPORT, 1_001_000, 777, 1_002_000, 1_003_000)),
                intents.drain());
    }

    @Test
    void anIntentPublishedOutsideAnyEventHasNoTrigger() throws Exception {
        final StepClock clock = new StepClock();
        final PublishingStrategy strategy = new PublishingStrategy(new GlobalSequence(), clock);
        final Collector intents = new Collector(strategy);
        strategy.submitMarketData(new Mbp10Schema());
        strategy.doWork();

        strategy.publishOutsideAnEvent();

        final Published outside = intents.drain().get(1);
        assertEquals(TriggerKind.OTHER, outside.kind());
        assertEquals(IntentDecoder.triggerSequenceNullValue(), outside.triggerSequence());
        assertEquals(IntentDecoder.triggerTimestampNullValue(), outside.triggerTimestamp());
        assertEquals(IntentDecoder.triggerOriginTimestampNullValue(), outside.originTimestamp());
        assertEquals(IntentDecoder.dispatchTimestampNullValue(), outside.dispatchTimestamp());
    }

    @Test
    void queueAndDecisionAreMeasuredByTheTriggersExchangeAndKind() throws Exception {
        final StepClock clock = new StepClock();
        final PublishingStrategy strategy = new PublishingStrategy(new GlobalSequence(), clock);
        final String[] kinds = {"NONE", "MARKET_DATA", "EXEC_REPORT", "STREAM", "RISK", "OTHER"};
        final MinuteHistograms histograms = new MinuteHistograms(3 * LatencyGrid.size(1, 1, kinds.length));
        final int[] exchanges = {3};
        final String[] names = {"KALSHI"};
        final String[] noAction = {""};
        strategy.measureLatency(
                new LatencyGrid(histograms, 7, "strat.transit", exchanges, names, noAction, kinds),
                new LatencyGrid(histograms, 1, "strat.queue", exchanges, names, noAction, kinds),
                new LatencyGrid(histograms, 2, "strat.decision", exchanges, names, noAction, kinds));
        final OrderExecutionReport report = new OrderExecutionReport();
        report.encoder.exchangeId(3).timestampRecv(1_000_600);

        strategy.submitExecReport(report);
        // Clock reads: the report's publish (1,001,000), dispatch (1,002,000), the send (1,003,000), the pass's roll.
        strategy.doWork();
        clock.now += MinuteHistograms.MINUTE_NANOS;
        strategy.doWork();

        final MinuteSummary summary = new MinuteSummary(histograms.capacity());
        assertEquals(true, histograms.summarize(summary));
        final List<String> measured = new ArrayList<>();
        for (int key = 0; key < histograms.keys(); key++) {
            if (summary.count(key) > 0) {
                final LatencyKey label = histograms.label(key);
                measured.add(label.metric() + " " + label.exchange() + " " + label.kind() + " " + summary.max(key));
            }
        }
        // Transit: the report's gateway receipt (1,000,600) to its arrival on the ring (1,001,000).
        assertEquals(
                List.of(
                        "strat.transit KALSHI EXEC_REPORT 400",
                        "strat.queue KALSHI EXEC_REPORT 1000",
                        "strat.decision KALSHI EXEC_REPORT 1000"),
                measured);
        assertEquals("execReports", strategy.activity().name(2));
        assertEquals(1, strategy.activity().get(2), "one execution report handled");
        assertEquals("intents", strategy.activity().name(3));
        assertEquals(1, strategy.activity().get(3), "one intent sent");
    }

    /** Reads every intent the strategy publishes; made before anything is published, so it sees them all. */
    private static final class Collector {
        final List<Published> intents = new ArrayList<>();
        private final Intent intent = new Intent();
        private final SequencedPoller poller;

        Collector(PublishingStrategy strategy) {
            this.poller = strategy.getIntentBuffer().createPoller((globalSequence, templateId, buffer, length) -> {
                intent.buffer.putBytes(0, buffer, 0, length);
                final IntentDecoder decoder = intent.decoder;
                intents.add(new Published(
                        decoder.triggerSequence(),
                        decoder.triggerKind(),
                        decoder.triggerTimestamp(),
                        decoder.triggerOriginTimestamp(),
                        decoder.dispatchTimestamp(),
                        decoder.timestampSend()));
            });
        }

        List<Published> drain() throws Exception {
            poller.poll();
            return intents;
        }
    }
}
