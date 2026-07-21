package group.gnometrading.strategies;

import group.gnometrading.SecurityMaster;
import group.gnometrading.oms.position.PositionView;
import group.gnometrading.schemas.Intent;
import group.gnometrading.schemas.Mbp10Schema;
import group.gnometrading.schemas.OrderExecutionReport;
import group.gnometrading.schemas.Schema;
import group.gnometrading.sequencer.GlobalSequence;
import group.gnometrading.sequencer.SequencedRingBuffer;
import java.util.List;

/**
 * Wraps a Python strategy callback as a {@link StrategyAgent}.
 *
 * <p>Python strategies cannot directly extend a Java abstract class via JPype. Instead, they
 * implement the {@link PythonStrategyCallback} interface (which JPype can proxy), and this class
 * bridges the gap by extending {@link StrategyAgent} and delegating to the callback.
 *
 * <p>Python strategies explicitly report their simulated processing latency via
 * {@link PythonStrategyCallback#simulateProcessingTime()} because Python interpreter overhead is
 * significant compared to native Java execution.
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
        /** Called on each market data update. Returns intents to submit. */
        List<Intent> onMarketData(Schema data);

        /** Called on each execution report. Returns intents to submit in response. */
        List<Intent> onExecutionReport(OrderExecutionReport report);

        /** Simulated processing latency in nanoseconds (accounts for Python overhead). */
        long simulateProcessingTime();

        /** Called once at construction time with the position view and security master. */
        void onInit(PositionView positionView, SecurityMaster securityMaster);
    }

    private static volatile PythonStrategyCallback globalCallback;

    private final PythonStrategyCallback callback;

    private PythonStrategyAgent(
            SequencedRingBuffer<?> marketDataBuffer,
            SequencedRingBuffer<OrderExecutionReport> execReportBuffer,
            SequencedRingBuffer<Intent> intentBuffer,
            PositionView positionView,
            SecurityMaster securityMaster,
            PythonStrategyCallback callback) {
        super(marketDataBuffer, execReportBuffer, intentBuffer, positionView, securityMaster);
        this.callback = callback;
        callback.onInit(positionView, securityMaster);
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
            SequencedRingBuffer<?> marketDataBuffer,
            SequencedRingBuffer<OrderExecutionReport> execReportBuffer,
            SequencedRingBuffer<Intent> intentBuffer,
            PositionView positionView,
            SecurityMaster securityMaster,
            PythonStrategyCallback callback) {
        return new PythonStrategyAgent(
                marketDataBuffer, execReportBuffer, intentBuffer, positionView, securityMaster, callback);
    }

    /**
     * Creates a {@link PythonStrategyAgent} with its own ring buffers.
     *
     * <p>Used for backtest replay, where the agent manages its own buffers independently of
     * any orchestrator.
     */
    public static PythonStrategyAgent create(
            PositionView positionView, SecurityMaster securityMaster, PythonStrategyCallback callback) {
        GlobalSequence seq = new GlobalSequence();
        SequencedRingBuffer<Mbp10Schema> mdBuffer = new SequencedRingBuffer<>(Mbp10Schema::new, seq);
        SequencedRingBuffer<OrderExecutionReport> erBuffer = new SequencedRingBuffer<>(OrderExecutionReport::new, seq);
        SequencedRingBuffer<Intent> intentBuffer = new SequencedRingBuffer<>(Intent::new, seq);
        return new PythonStrategyAgent(mdBuffer, erBuffer, intentBuffer, positionView, securityMaster, callback);
    }

    @Override
    protected void onMarketData(Schema data) {
        List<Intent> intents = callback.onMarketData(data);
        publishIntents(intents);
    }

    @Override
    protected void onExecutionReport(OrderExecutionReport report) {
        List<Intent> intents = callback.onExecutionReport(report);
        publishIntents(intents);
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
