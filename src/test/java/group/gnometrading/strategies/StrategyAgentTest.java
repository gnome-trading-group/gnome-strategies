package group.gnometrading.strategies;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import group.gnometrading.schemas.Intent;
import group.gnometrading.schemas.Mbp10Schema;
import group.gnometrading.schemas.Mbp1Schema;
import group.gnometrading.schemas.OrderExecutionReport;
import group.gnometrading.schemas.Schema;
import group.gnometrading.schemas.SchemaType;
import group.gnometrading.sequencer.GlobalSequence;
import group.gnometrading.sequencer.SequencedRingBuffer;
import group.gnometrading.sequencer.metrics.ColumnarMetricRecorder;
import group.gnometrading.sequencer.metrics.MetricRecorder;
import group.gnometrading.sequencer.metrics.MetricStream;
import group.gnometrading.sequencer.metrics.RecordBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

class StrategyAgentTest {

    /** Remembers the schema type and best bid of every market data update it is handed. */
    private static final class RecordingStrategy extends StrategyAgent {
        final List<SchemaType> types = new ArrayList<>();
        final List<Long> bids = new ArrayList<>();
        int registrations;
        MetricStream bidStream;
        int bidColumn;

        RecordingStrategy() {
            super(
                    0,
                    new SequencedRingBuffer<>(Mbp10Schema::new, new GlobalSequence()),
                    new SequencedRingBuffer<>(OrderExecutionReport::new, new GlobalSequence()),
                    new SequencedRingBuffer<>(Intent::new, new GlobalSequence()),
                    null,
                    null);
        }

        @Override
        protected void registerMetrics(MetricRecorder metrics) {
            registrations++;
            bidStream = metrics.createStream("bids");
            bidColumn = bidStream.addLongColumn("bid");
            bidStream.freeze();
        }

        @Override
        protected void onMarketData(Schema data) {
            types.add(data.schemaType);
            long bid = data instanceof Mbp1Schema mbp1
                    ? mbp1.decoder.bidPrice0()
                    : ((Mbp10Schema) data).decoder.bidPrice0();
            bids.add(bid);
            if (bidStream != null) {
                bidStream.begin();
                bidStream.setLong(bidColumn, bid);
                bidStream.commit();
            }
        }

        @Override
        protected void onExecutionReport(OrderExecutionReport report) {}
    }

    @Test
    void mbp10AndMbp1UpdatesBothReachTheStrategy() throws Exception {
        RecordingStrategy strategy = new RecordingStrategy();
        Mbp10Schema mbp10 = new Mbp10Schema();
        mbp10.encoder.bidPrice0(100);
        Mbp1Schema mbp1 = new Mbp1Schema();
        mbp1.encoder.bidPrice0(200);

        strategy.submitMarketData(mbp10);
        strategy.submitMarketData(mbp1);
        strategy.doWork();

        assertEquals(List.of(SchemaType.MBP_10, SchemaType.MBP_1), strategy.types);
        assertEquals(List.of(100L, 200L), strategy.bids);
    }

    @Test
    void theStrategyDeclaresItsMetricsOnceAndRecordsFromItsCallbacks() throws Exception {
        RecordingStrategy strategy = new RecordingStrategy();
        ColumnarMetricRecorder recorder = new ColumnarMetricRecorder(4);
        strategy.configureMetrics(recorder);
        Mbp10Schema mbp10 = new Mbp10Schema();
        mbp10.encoder.bidPrice0(100);

        strategy.submitMarketData(mbp10);
        strategy.doWork();

        RecordBuffer buffer = recorder.getBuffers().get(0);
        assertEquals("bids", buffer.getName());
        assertEquals(1, buffer.getCount());
        assertEquals(100L, Arrays.copyOf(buffer.getLongColumn(0), 1)[0]);
        assertThrows(IllegalStateException.class, () -> strategy.configureMetrics(MetricRecorder.discarding()));
        assertEquals(1, strategy.registrations);
    }
}
