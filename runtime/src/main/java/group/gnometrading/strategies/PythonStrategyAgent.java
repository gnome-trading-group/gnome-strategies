package group.gnometrading.strategies;

import group.gnometrading.SecurityMaster;
import group.gnometrading.md.StreamDirectory;
import group.gnometrading.oms.position.PositionView;
import group.gnometrading.schemas.Intent;
import group.gnometrading.schemas.Mbp10Schema;
import group.gnometrading.schemas.OrderExecutionReport;
import group.gnometrading.schemas.StreamRecord;
import group.gnometrading.sequencer.GlobalSequence;
import group.gnometrading.sequencer.SequencedRingBuffer;
import group.gnometrading.sequencer.metrics.MetricRecorder;
import java.util.List;

/**
 * Wraps a Python strategy callback as a {@link StrategyAgent}.
 *
 * <p>Python strategies cannot directly extend a Java abstract class via JPype. Instead, they
 * implement the {@link PythonStrategyCallback} interface (which JPype can proxy), and this class
 * bridges the gap by extending {@link StrategyAgent} and delegating to the callback.
 *
 * <p>Python strategies report their simulated processing latency via
 * {@link PythonStrategyCallback#simulateProcessingTime()}; the time spent in Python is never charged
 * unless the backtest config measures wall-clock processing time.
 *
 * <p>For live/paper trading, use {@link #createWithBuffers} with pre-wired ring buffers from the
 * orchestrator. For backtest replay, use {@link #create} which allocates its own ring buffers.
 */
public final class PythonStrategyAgent extends StrategyAgent {

    /**
     * Callback interface implemented by the Python strategy proxy.
     *
     * <p>Returns lists of {@link Intent} objects which are then published to the intent ring
     * buffer by {@link PythonStrategyAgent}.
     */
    public interface PythonStrategyCallback {
        /** Called on each book record (a venue listing's market data). Returns intents to submit. */
        List<Intent> onMarketData(StreamRecord data);

        /** Called on each record of any other stream (a vendor feed, say). Returns intents to submit; none by default. */
        default List<Intent> onStreamEvent(int streamId, StreamRecord record) {
            return List.of();
        }

        /** Called once, before any data, with the streams the strategy reads and the listings they carry. */
        default void onStreams(StreamDirectory streams) {}

        /** Called on each execution report. Returns intents to submit in response. */
        List<Intent> onExecutionReport(OrderExecutionReport report);

        /** Simulated processing latency in nanoseconds; 0 means none. */
        long simulateProcessingTime();

        /**
         * Called once at construction time with the position view, the security master, and the control through
         * which the strategy can end its session.
         */
        void onInit(PositionView positionView, SecurityMaster securityMaster, StrategyControl control);

        /** Called once, after {@link #onInit}, with the recorder the strategy declares its metric streams in. */
        void registerMetrics(MetricRecorder recorder);
    }

    private static volatile PythonStrategyCallback globalCallback;

    private final PythonStrategyCallback callback;

    private PythonStrategyAgent(
            int strategyId,
            SequencedRingBuffer<?> marketDataBuffer,
            SequencedRingBuffer<OrderExecutionReport> execReportBuffer,
            SequencedRingBuffer<Intent> intentBuffer,
            PositionView positionView,
            SecurityMaster securityMaster,
            PythonStrategyCallback callback) {
        super(strategyId, marketDataBuffer, execReportBuffer, intentBuffer, positionView, securityMaster);
        this.callback = callback;
        callback.onInit(positionView, securityMaster, this);
    }

    /**
     * Sets the global Python callback before {@code Orchestrator.main()} is called.
     *
     * <p>JPype sets this static reference from Python before invoking the orchestrator so that
     * {@link #createWithBuffers} can retrieve it during {@code configure()}.
     */
    public static void setCallback(PythonStrategyCallback cb) {
        globalCallback = cb;
    }

    /** Returns the globally registered Python callback. */
    public static PythonStrategyCallback getCallback() {
        return globalCallback;
    }

    /**
     * Creates a {@link PythonStrategyAgent} with pre-wired ring buffers from the orchestrator.
     *
     * <p>Used for live and paper trading, where the orchestrator's {@code configure()} method
     * owns the ring buffers and passes them in.
     */
    public static PythonStrategyAgent createWithBuffers(
            int strategyId,
            SequencedRingBuffer<?> marketDataBuffer,
            SequencedRingBuffer<OrderExecutionReport> execReportBuffer,
            SequencedRingBuffer<Intent> intentBuffer,
            PositionView positionView,
            SecurityMaster securityMaster,
            PythonStrategyCallback callback) {
        return new PythonStrategyAgent(
                strategyId, marketDataBuffer, execReportBuffer, intentBuffer, positionView, securityMaster, callback);
    }

    /**
     * Creates a {@link PythonStrategyAgent} with its own ring buffers.
     *
     * <p>Used for backtest replay, where the agent manages its own buffers independently of
     * any orchestrator.
     */
    public static PythonStrategyAgent create(
            int strategyId, PositionView positionView, SecurityMaster securityMaster, PythonStrategyCallback callback) {
        GlobalSequence seq = new GlobalSequence();
        SequencedRingBuffer<Mbp10Schema> mdBuffer = new SequencedRingBuffer<>(Mbp10Schema::new, seq);
        SequencedRingBuffer<OrderExecutionReport> erBuffer = new SequencedRingBuffer<>(OrderExecutionReport::new, seq);
        SequencedRingBuffer<Intent> intentBuffer = new SequencedRingBuffer<>(Intent::new, seq);
        return new PythonStrategyAgent(
                strategyId, mdBuffer, erBuffer, intentBuffer, positionView, securityMaster, callback);
    }

    @Override
    protected void onMarketData(StreamRecord data) {
        List<Intent> intents = callback.onMarketData(data);
        publishIntents(intents);
    }

    @Override
    protected void onStreamEvent(int streamId, StreamRecord record) {
        List<Intent> intents = callback.onStreamEvent(streamId, record);
        publishIntents(intents);
    }

    @Override
    protected void onStreamsConfigured(StreamDirectory directory) {
        callback.onStreams(directory);
    }

    @Override
    protected void onExecutionReport(OrderExecutionReport report) {
        List<Intent> intents = callback.onExecutionReport(report);
        publishIntents(intents);
    }

    @Override
    protected void registerMetrics(MetricRecorder metrics) {
        callback.registerMetrics(metrics);
    }

    @Override
    public long simulateProcessingTime() {
        return callback.simulateProcessingTime();
    }

    private void publishIntents(List<Intent> intents) {
        if (intents == null) {
            return;
        }
        for (Intent intent : intents) {
            publishIntent(intent);
        }
    }
}
