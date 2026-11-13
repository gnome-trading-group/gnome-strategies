package group.gnometrading.strategies.check;

import group.gnometrading.SecurityMaster;
import group.gnometrading.oms.position.PositionView;
import group.gnometrading.schemas.Intent;
import group.gnometrading.schemas.OrderExecutionReport;
import group.gnometrading.schemas.StreamRecord;
import group.gnometrading.sequencer.SequencedRingBuffer;
import group.gnometrading.strategies.StrategyAgent;

/** Trades nothing: shows a session ran a class that exists only at its strategy commit, with its args. */
public final class PlatformCheck extends StrategyAgent {

    private static final System.Logger LOG = System.getLogger(PlatformCheck.class.getName());
    private static final long REPORT_EVERY = 1_000;

    private final String marker;
    private long records;

    public PlatformCheck(
            int strategyId,
            SequencedRingBuffer<?> marketDataBuffer,
            SequencedRingBuffer<OrderExecutionReport> execReportBuffer,
            SequencedRingBuffer<Intent> intentBuffer,
            PositionView positionView,
            SecurityMaster securityMaster,
            String marker) {
        super(strategyId, marketDataBuffer, execReportBuffer, intentBuffer, positionView, securityMaster);
        this.marker = marker;
        LOG.log(System.Logger.Level.INFO, "platform check {0} built from its strategy commit", marker);
    }

    @Override
    protected void onMarketData(StreamRecord data) {
        records++;
        if (records == 1 || records % REPORT_EVERY == 0) {
            LOG.log(System.Logger.Level.INFO, "platform check {0}: {1} market data records", marker, records);
        }
    }

    @Override
    protected void onExecutionReport(OrderExecutionReport report) {}
}
