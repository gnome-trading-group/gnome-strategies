package group.gnometrading.strategies.load;

import group.gnometrading.SecurityMaster;
import group.gnometrading.md.StreamDirectory;
import group.gnometrading.oms.position.PositionView;
import group.gnometrading.schemas.Intent;
import group.gnometrading.schemas.IntentDecoder;
import group.gnometrading.schemas.OrderExecutionReport;
import group.gnometrading.schemas.OrderType;
import group.gnometrading.schemas.Side;
import group.gnometrading.schemas.StreamRecord;
import group.gnometrading.sequencer.SequencedRingBuffer;
import group.gnometrading.sm.Listing;
import group.gnometrading.strategies.StrategyAgent;
import group.gnometrading.utils.SampledEpochNanoClock;
import org.agrona.concurrent.EpochNanoClock;

/**
 * A strategy that trades nothing, for measuring what a session can carry: it reads every record, spends a set time on
 * each ({@code work_micros}, busy, as a strategy's own computation would), and optionally exercises the order path.
 *
 * <p>With {@code order_every_seconds} set, each of the first {@code order_listings} listings to send market data in
 * turn (default 1; a turn whose listing hasn't arrived yet passes idle) gets a post-only bid at {@code order_price} (far below any real touch, so it rests and never fills), then
 * {@code order_every_seconds} later has it cancelled, and so on. Actions are spread evenly across the listings, and
 * each is published while handling a market-data record, so the session measures its tick-to-order path on it.
 *
 * <p>Config args are snake_case to match the launcher's.
 */
@SuppressWarnings("checkstyle:ParameterName")
public final class NoopStrategy extends StrategyAgent {

    private static final double PRICE_SCALE = 1e9;
    private static final double SIZE_SCALE = 1e6;
    private static final long SECOND_NANOS = 1_000_000_000L;

    private final long workNanos;
    private final long actionIntervalNanos;
    private final int[] orderListings;
    private final int[] orderExchanges;
    private final int[] orderSecurities;
    private final boolean[] resting;
    private final long orderPrice;
    private final long orderSize;
    private final EpochNanoClock clock;
    private final Intent intent = new Intent();
    private int knownOrderListings;
    private int nextListing;
    private long nextActionNanos;

    public NoopStrategy(
            int strategyId,
            SequencedRingBuffer<?> marketDataBuffer,
            SequencedRingBuffer<OrderExecutionReport> execReportBuffer,
            SequencedRingBuffer<Intent> intentBuffer,
            PositionView positionView,
            SecurityMaster securityMaster,
            Long work_micros,
            Double order_every_seconds,
            Integer order_listings,
            Double order_price,
            Double order_size) {
        this(
                strategyId,
                marketDataBuffer,
                execReportBuffer,
                intentBuffer,
                positionView,
                securityMaster,
                SampledEpochNanoClock.INSTANCE,
                work_micros,
                order_every_seconds,
                order_listings,
                order_price,
                order_size);
    }

    NoopStrategy(
            int strategyId,
            SequencedRingBuffer<?> marketDataBuffer,
            SequencedRingBuffer<OrderExecutionReport> execReportBuffer,
            SequencedRingBuffer<Intent> intentBuffer,
            PositionView positionView,
            SecurityMaster securityMaster,
            EpochNanoClock clock,
            Long work_micros,
            Double order_every_seconds,
            Integer order_listings,
            Double order_price,
            Double order_size) {
        super(strategyId, marketDataBuffer, execReportBuffer, intentBuffer, positionView, securityMaster, clock);
        this.clock = clock;
        this.workNanos = work_micros == null ? 0 : Math.max(0, work_micros) * 1_000;
        final int listings = order_every_seconds == null || order_every_seconds <= 0
                ? 0
                : order_listings == null ? 1 : Math.max(0, order_listings);
        this.orderListings = new int[listings];
        this.orderExchanges = new int[listings];
        this.orderSecurities = new int[listings];
        this.resting = new boolean[listings];
        this.actionIntervalNanos =
                listings == 0 ? 0 : Math.max(1, Math.round(order_every_seconds * SECOND_NANOS / listings));
        this.orderPrice = Math.round((order_price == null ? 0.01 : order_price) * PRICE_SCALE);
        this.orderSize = Math.round((order_size == null ? 1.0 : order_size) * SIZE_SCALE);
    }

    @Override
    protected void onMarketData(StreamRecord data) {
        if (workNanos > 0) {
            spin(workNanos);
        }
        if (orderListings.length == 0) {
            return;
        }
        final int listingId = streams().listingId(data.streamId());
        if (listingId != StreamDirectory.NONE) {
            learn(listingId);
        }
        final long now = clock.nanoTime();
        if (now < nextActionNanos) {
            return;
        }
        nextActionNanos += actionIntervalNanos;
        if (nextActionNanos <= now) {
            // Behind by a whole interval (a stall, or the first action): space the next from now rather than catching
            // up in a burst.
            nextActionNanos = now + actionIntervalNanos;
        }
        // A slot whose listing hasn't sent anything yet keeps its turn, idle, so each listing's period stays the same.
        if (nextListing < knownOrderListings) {
            act(nextListing);
        }
        nextListing = (nextListing + 1) % orderListings.length;
    }

    private void learn(final int listingId) {
        if (knownOrderListings == orderListings.length) {
            return;
        }
        for (int i = 0; i < knownOrderListings; i++) {
            if (orderListings[i] == listingId) {
                return;
            }
        }
        final Listing listing = streams().listing(listingId);
        orderListings[knownOrderListings] = listingId;
        orderExchanges[knownOrderListings] = listing.exchange().exchangeId();
        orderSecurities[knownOrderListings] = listing.security().securityId();
        knownOrderListings++;
    }

    private void act(final int slot) {
        resting[slot] = !resting[slot];
        intent.encoder
                .exchangeId(orderExchanges[slot])
                .securityId(orderSecurities[slot])
                .bidPrice(resting[slot] ? orderPrice : IntentDecoder.bidPriceNullValue())
                .bidSize(resting[slot] ? orderSize : IntentDecoder.bidSizeNullValue())
                .askPrice(IntentDecoder.askPriceNullValue())
                .askSize(IntentDecoder.askSizeNullValue())
                .takeSize(IntentDecoder.takeSizeNullValue())
                .takeSide(Side.NULL_VAL)
                .takeOrderType(OrderType.NULL_VAL)
                .takeLimitPrice(IntentDecoder.takeLimitPriceNullValue());
        intent.encoder.flags().clear();
        intent.encoder.flags().postOnly(resting[slot]);
        publishIntent(intent);
    }

    // The real clock, not the strategy's: this stands in for computation, which takes wall time in a test too.
    private static void spin(final long nanos) {
        final long until = System.nanoTime() + nanos;
        while (System.nanoTime() < until) {
            Thread.onSpinWait();
        }
    }

    @Override
    protected void onExecutionReport(OrderExecutionReport report) {}
}
