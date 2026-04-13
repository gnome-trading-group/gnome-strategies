package group.gnometrading.strategies;

import group.gnometrading.concurrent.GnomeAgent;
import group.gnometrading.oms.position.PositionView;
import group.gnometrading.schemas.Intent;
import group.gnometrading.schemas.Mbp10Decoder;
import group.gnometrading.schemas.Mbp10Schema;
import group.gnometrading.schemas.OrderExecutionReport;
import group.gnometrading.schemas.OrderExecutionReportDecoder;
import group.gnometrading.schemas.Schema;
import group.gnometrading.sequencer.SequencedEventHandler;
import group.gnometrading.sequencer.SequencedPoller;
import group.gnometrading.sequencer.SequencedRingBuffer;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * Abstract base class for trading strategies.
 *
 * <p>Implements {@link GnomeAgent} so the same strategy can run identically in production
 * (via {@link group.gnometrading.concurrent.GnomeAgentRunner} on a dedicated thread) and in
 * backtest (with {@link #doWork()} called synchronously by the backtest driver).
 *
 * <p>The strategy reads from two ring buffers: a combined market data buffer and an exec report
 * buffer. It publishes trading decisions as {@link Intent} objects to an outbound intent buffer.
 *
 * <p>Position state is available via {@link #getPositionView()}.
 *
 * <p>The backtest driver measures wall-clock time of {@link #doWork()} via {@code System.nanoTime()}
 * and uses that as processing latency for Java strategies. Python strategies override
 * {@link #simulateProcessingTime()} to provide a fixed constant instead.
 */
public abstract class StrategyAgent implements GnomeAgent, SequencedEventHandler {

    private final SequencedRingBuffer<?> marketDataBuffer;
    private final SequencedRingBuffer<OrderExecutionReport> execReportBuffer;
    private final SequencedRingBuffer<Intent> intentBuffer;
    private final SequencedPoller marketDataPoller;
    private final SequencedPoller execReportPoller;
    private final PositionView positionView;

    // Pre-allocated flyweights for zero-alloc reads
    private final Mbp10Schema mbp10 = new Mbp10Schema();
    private final OrderExecutionReport execReport = new OrderExecutionReport();

    protected StrategyAgent(
            SequencedRingBuffer<?> marketDataBuffer,
            SequencedRingBuffer<OrderExecutionReport> execReportBuffer,
            SequencedRingBuffer<Intent> intentBuffer,
            PositionView positionView) {
        this.marketDataBuffer = marketDataBuffer;
        this.execReportBuffer = execReportBuffer;
        this.intentBuffer = intentBuffer;
        this.positionView = positionView;
        this.marketDataPoller = marketDataBuffer.createPoller(this);
        this.execReportPoller = execReportBuffer.createPoller(this);
    }

    @Override
    public void onStart() {
        // Disambiguates GnomeAgent.onStart() from Disruptor EventHandlerBase.onStart()
    }

    /** Read-only access to position state for this strategy. */
    protected final PositionView getPositionView() {
        return positionView;
    }

    /**
     * {@inheritDoc}
     *
     * <p>Polls exec report buffer first, then market data. Subclasses should not override this;
     * implement logic in {@link #onMarketData} and {@link #onExecutionReport} instead.
     */
    @Override
    public final int doWork() throws Exception {
        int work = 0;
        // Exec reports first so the strategy can react to fills before processing new market data
        work += execReportPoller.poll();
        work += marketDataPoller.poll();
        return work;
    }

    /**
     * {@inheritDoc}
     *
     * <p>Dispatches to {@link #onMarketData} or {@link #onExecutionReport} based on templateId.
     */
    @Override
    public final void onSequencedEvent(long globalSequence, int templateId, UnsafeBuffer buffer, int length)
            throws Exception {
        if (templateId == OrderExecutionReportDecoder.TEMPLATE_ID) {
            execReport.buffer.putBytes(0, buffer, 0, length);
            execReport.wrap(execReport.buffer);
            onExecutionReport(execReport);
        } else if (templateId == Mbp10Decoder.TEMPLATE_ID) {
            mbp10.buffer.putBytes(0, buffer, 0, length);
            mbp10.wrap(mbp10.buffer);
            onMarketData(mbp10);
        }
    }

    /** Called when a market data update arrives. Implement trading logic here. */
    protected abstract void onMarketData(Schema data);

    /** Called when an execution report arrives. React to fills here. */
    protected abstract void onExecutionReport(OrderExecutionReport report);

    /**
     * Publish a trading intent to the outbound buffer for OMS processing.
     *
     * <p>Call this from {@link #onMarketData} or {@link #onExecutionReport} to submit, amend,
     * or cancel orders.
     */
    protected final void publishIntent(Intent intent) {
        intentBuffer.publishRaw(intent.buffer, intent.messageHeaderDecoder.templateId(), intent.totalMessageSize());
    }

    /**
     * Returns the intent ring buffer so the backtest driver can create a poller to drain it.
     *
     * <p>Not intended for strategy subclasses — exposed for the backtest infrastructure.
     */
    public final SequencedRingBuffer<Intent> getIntentBuffer() {
        return intentBuffer;
    }

    /**
     * Writes a market data event into the strategy's inbound buffer.
     *
     * <p>In production this is done by a gateway/socket-reader agent. In backtest it is called
     * directly by the driver before {@link #doWork()}.
     */
    public final void submitMarketData(Schema schema) {
        marketDataBuffer.publishRaw(schema.buffer, schema.messageHeaderDecoder.templateId(), schema.totalMessageSize());
    }

    /**
     * Writes an execution report into the strategy's inbound buffer.
     *
     * <p>In production this is written by the OMS agent. In backtest it is called directly by
     * the driver before {@link #doWork()}.
     */
    public final void submitExecReport(OrderExecutionReport report) {
        execReportBuffer.publishRaw(report.buffer, report.messageHeaderDecoder.templateId(), report.totalMessageSize());
    }

    /**
     * Override to provide a fixed processing latency instead of measured wall-clock time.
     *
     * <p>Returns 0 by default, which tells the backtest driver to use the actual measured
     * execution time of {@link #doWork()}. Python strategies override this with a constant
     * since the Python interpreter overhead makes wall-clock measurement inaccurate for
     * modeling production Java execution time.
     *
     * @return fixed processing latency in nanoseconds, or 0 to use measured time
     */
    public long simulateProcessingTime() {
        return 0;
    }
}
