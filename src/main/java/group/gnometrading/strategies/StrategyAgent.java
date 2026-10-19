package group.gnometrading.strategies;

import group.gnometrading.SecurityMaster;
import group.gnometrading.concurrent.GnomeAgent;
import group.gnometrading.concurrent.ThreadProfile;
import group.gnometrading.oms.position.PositionView;
import group.gnometrading.schemas.Intent;
import group.gnometrading.schemas.Mbp10Decoder;
import group.gnometrading.schemas.Mbp10Schema;
import group.gnometrading.schemas.Mbp1Decoder;
import group.gnometrading.schemas.Mbp1Schema;
import group.gnometrading.schemas.OrderExecutionReport;
import group.gnometrading.schemas.OrderExecutionReportDecoder;
import group.gnometrading.schemas.Schema;
import group.gnometrading.sequencer.SequencedEventHandler;
import group.gnometrading.sequencer.SequencedPoller;
import group.gnometrading.sequencer.SequencedRingBuffer;
import group.gnometrading.sequencer.metrics.MetricRecorder;
import group.gnometrading.sm.Listing;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.agrona.concurrent.EpochNanoClock;
import org.agrona.concurrent.SystemEpochNanoClock;
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
 * <p>A strategy ends its own session through {@link #requestExit}: from then on its callbacks are no longer called,
 * and the agent takes it out of the market (see {@link WindDown}) before telling the session, which shuts down.
 *
 * <p>A strategy records metrics by declaring streams in {@link #registerMetrics}. Whoever runs it decides where they
 * go: memory for a backtest, the session journal for a live or paper session.
 *
 * <p>In a backtest, {@link #simulateProcessingTime()} is the strategy's processing latency. The
 * driver uses measured wall-clock time instead only when the backtest config turns that on.
 */
public abstract class StrategyAgent implements GnomeAgent, SequencedEventHandler, StrategyControl {

    private final SequencedRingBuffer<?> marketDataBuffer;
    private final SequencedRingBuffer<OrderExecutionReport> execReportBuffer;
    private final SequencedRingBuffer<Intent> intentBuffer;
    private final SequencedPoller marketDataPoller;
    private final SequencedPoller execReportPoller;
    private final PositionView positionView;
    private final SecurityMaster securityMaster;
    private final int strategyId;

    // Pre-allocated flyweights for zero-alloc reads
    private final Mbp10Schema mbp10 = new Mbp10Schema();
    private final Mbp1Schema mbp1 = new Mbp1Schema();
    private final OrderExecutionReport execReport = new OrderExecutionReport();

    private final EpochNanoClock nanoClock;

    private final AtomicReference<ExitRequest> exitRequest = new AtomicReference<>();
    // Set before the agent starts; without it (a backtest) an exit only silences the strategy.
    private WindDown windDown;
    // The strategy thread's own copy of exitRequest, so a pass reads the volatile once.
    private boolean exiting;
    private boolean metricsConfigured;

    protected StrategyAgent(
            int strategyId,
            SequencedRingBuffer<?> marketDataBuffer,
            SequencedRingBuffer<OrderExecutionReport> execReportBuffer,
            SequencedRingBuffer<Intent> intentBuffer,
            PositionView positionView,
            SecurityMaster securityMaster) {
        this(
                strategyId,
                marketDataBuffer,
                execReportBuffer,
                intentBuffer,
                positionView,
                securityMaster,
                SystemEpochNanoClock.INSTANCE);
    }

    protected StrategyAgent(
            int strategyId,
            SequencedRingBuffer<?> marketDataBuffer,
            SequencedRingBuffer<OrderExecutionReport> execReportBuffer,
            SequencedRingBuffer<Intent> intentBuffer,
            PositionView positionView,
            SecurityMaster securityMaster,
            EpochNanoClock nanoClock) {
        this.strategyId = strategyId;
        this.marketDataBuffer = marketDataBuffer;
        this.execReportBuffer = execReportBuffer;
        this.intentBuffer = intentBuffer;
        this.positionView = positionView;
        this.securityMaster = securityMaster;
        this.nanoClock = nanoClock;
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

    /** Read-only access to the security master for listing lookups. */
    protected final SecurityMaster getSecurityMaster() {
        return securityMaster;
    }

    /**
     * {@inheritDoc}
     *
     * <p>Polls exec report buffer first, then market data. Subclasses should not override this;
     * implement logic in {@link #onMarketData} and {@link #onExecutionReport} instead.
     */
    @Override
    public final int doWork() throws Exception {
        if (!exiting && exitRequest.get() != null) {
            exiting = true;
        }
        int work = 0;
        // Exec reports first so the strategy can react to fills before processing new market data
        work += execReportPoller.poll();
        work += marketDataPoller.poll();
        if (exiting && windDown != null && !windDown.isFinished()) {
            windDown.step(exitRequest.get(), nanoClock.nanoTime());
            work++;
        }
        return work;
    }

    /**
     * Lets the strategy end its session: wires the listings it trades, so an exit can take it out of each, and
     * whoever shuts the session down once that's done. Called by the orchestrator before the agent starts.
     */
    public final void configureExit(List<Listing> listings, ExitListener listener) {
        this.windDown = new WindDown(listings, positionView, this::publishIntent, listener);
    }

    /**
     * Gives the strategy the recorder its metrics go to, and has it declare its streams there through {@link
     * #registerMetrics}. Called once before the agent starts, by the backtest driver or the orchestrator.
     */
    public final void configureMetrics(MetricRecorder recorder) {
        if (metricsConfigured) {
            throw new IllegalStateException("Strategy " + strategyId + " already has its metrics configured");
        }
        metricsConfigured = true;
        registerMetrics(recorder);
    }

    /**
     * Declares the strategy's metric streams: create each, add its columns and freeze it, keeping the streams to
     * record into from the callbacks. Rows go wherever the recorder sends them, so the same code records in a
     * backtest and a session. Does nothing unless overridden.
     */
    protected void registerMetrics(MetricRecorder metrics) {}

    @Override
    public final boolean requestExit(String reason, boolean flatten, long timeoutMillis) {
        return exitRequest.compareAndSet(
                null, new ExitRequest(reason, flatten, TimeUnit.MILLISECONDS.toNanos(Math.max(0, timeoutMillis))));
    }

    @Override
    public final boolean isExiting() {
        return exitRequest.get() != null;
    }

    @Override
    public final ThreadProfile threadProfile() {
        return ThreadProfile.HOT_PATH;
    }

    /**
     * {@inheritDoc}
     *
     * <p>Dispatches to {@link #onMarketData} or {@link #onExecutionReport} based on templateId.
     */
    @Override
    public final void onSequencedEvent(long globalSequence, int templateId, UnsafeBuffer buffer, int length)
            throws Exception {
        // Still drained once the strategy is exiting, so nothing writing to these buffers waits on them; the
        // wind-down reads the closes' outcomes.
        if (exiting) {
            if (templateId == OrderExecutionReportDecoder.TEMPLATE_ID && windDown != null) {
                execReport.wrap(buffer);
                windDown.onExecutionReport(execReport);
            }
            return;
        }
        if (templateId == OrderExecutionReportDecoder.TEMPLATE_ID) {
            execReport.wrap(buffer);
            onExecutionReport(execReport);
        } else if (templateId == Mbp10Decoder.TEMPLATE_ID) {
            mbp10.wrap(buffer);
            onMarketData(mbp10);
        } else if (templateId == Mbp1Decoder.TEMPLATE_ID) {
            mbp1.wrap(buffer);
            onMarketData(mbp1);
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
        intent.encoder.timestampSend(nanoClock.nanoTime());
        intent.encoder.strategyId((short) strategyId);
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
     * Returns the market data ring buffer so the backtest driver can feed the OMS price buffer from the same
     * events the strategy sees, as the live orchestrator does.
     *
     * <p>Not intended for strategy subclasses — exposed for the backtest infrastructure.
     */
    public final SequencedRingBuffer<?> getMarketDataBuffer() {
        return marketDataBuffer;
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
     * The simulated time between this strategy seeing an event and its orders leaving, used by the
     * backtest driver. A fixed value keeps runs deterministic.
     *
     * <p>Ignored when the backtest config measures wall-clock processing time instead.
     *
     * @return processing latency in nanoseconds; 0 (the default) means none
     */
    public long simulateProcessingTime() {
        return 0;
    }
}
