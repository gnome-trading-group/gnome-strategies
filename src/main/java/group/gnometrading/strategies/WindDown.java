package group.gnometrading.strategies;

import group.gnometrading.oms.position.Position;
import group.gnometrading.oms.position.PositionView;
import group.gnometrading.schemas.Intent;
import group.gnometrading.schemas.IntentDecoder;
import group.gnometrading.schemas.OrderType;
import group.gnometrading.schemas.Side;
import group.gnometrading.sm.Listing;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * Takes a strategy out of the market once it has asked to exit: cancels its quotes, then, if asked, closes what it
 * holds with market orders, until nothing is working (and it is flat) or the time runs out. Driven by
 * {@link #step} on every pass of the strategy's loop; allocation-free until it finishes.
 */
final class WindDown {

    // Long enough for the OMS to have taken in the last close before it is judged, so a close is never doubled.
    static final long RETRY_INTERVAL_NANOS = 1_000_000_000L;

    private final int[] listingIds;
    private final int[] exchangeIds;
    private final int[] securityIds;
    private final long[] lastCloseNanos;
    private final PositionView positions;
    private final Consumer<Intent> publisher;
    private final ExitListener listener;
    private final Intent intent = new Intent();

    private String reason;
    private boolean flatten;
    private long deadlineNanos;
    private boolean started;
    private boolean finished;

    WindDown(List<Listing> listings, PositionView positions, Consumer<Intent> publisher, ExitListener listener) {
        this.listingIds = new int[listings.size()];
        this.exchangeIds = new int[listings.size()];
        this.securityIds = new int[listings.size()];
        this.lastCloseNanos = new long[listings.size()];
        for (int i = 0; i < listings.size(); i++) {
            Listing listing = listings.get(i);
            listingIds[i] = listing.listingId();
            exchangeIds[i] = listing.exchange().exchangeId();
            securityIds[i] = listing.security().securityId();
        }
        this.positions = positions;
        this.publisher = publisher;
        this.listener = listener;
    }

    boolean isFinished() {
        return finished;
    }

    void step(ExitRequest request, long nowNanos) {
        if (finished) {
            return;
        }
        if (!started) {
            start(request, nowNanos);
        }
        boolean done = true;
        for (int i = 0; i < listingIds.length; i++) {
            done &= settle(i, nowNanos);
        }
        if (done || nowNanos >= deadlineNanos) {
            finish(!done);
        }
    }

    /** Closes what the listing still holds if nothing is working there; true once there's nothing left to do. */
    private boolean settle(int index, long nowNanos) {
        Position position = positions.getPosition(listingIds[index]);
        long net = position.netQuantity;
        boolean working = position.leavesBuyQty != 0 || position.leavesSellQty != 0;
        boolean open = flatten && net != 0;
        if (open && !working && nowNanos - lastCloseNanos[index] >= RETRY_INTERVAL_NANOS) {
            lastCloseNanos[index] = nowNanos;
            close(index, net);
        }
        return !working && !open;
    }

    private void start(ExitRequest request, long nowNanos) {
        started = true;
        reason = request.reason();
        flatten = request.flatten();
        deadlineNanos = nowNanos + request.timeoutNanos();
        for (int i = 0; i < listingIds.length; i++) {
            // The first close waits a retry interval, so the OMS has seen the cancels before anything is sold.
            lastCloseNanos[i] = nowNanos;
            encode(i, 0, null);
            publisher.accept(intent);
        }
    }

    /** Sized to the position alone, so it never sells more than is held: that would open a short on some venues. */
    private void close(int index, long net) {
        encode(index, Math.abs(net), net > 0 ? Side.Ask : Side.Bid);
        publisher.accept(intent);
    }

    private void encode(int index, long takeSize, Side takeSide) {
        intent.encoder
                .exchangeId(exchangeIds[index])
                .securityId(securityIds[index])
                .bidPrice(IntentDecoder.bidPriceNullValue())
                .bidSize(IntentDecoder.bidSizeNullValue())
                .askPrice(IntentDecoder.askPriceNullValue())
                .askSize(IntentDecoder.askSizeNullValue())
                .takeLimitPrice(IntentDecoder.takeLimitPriceNullValue());
        intent.encoder.flags().clear();
        if (takeSide == null) {
            intent.encoder
                    .takeSize(IntentDecoder.takeSizeNullValue())
                    .takeSide(Side.NULL_VAL)
                    .takeOrderType(OrderType.NULL_VAL);
        } else {
            intent.encoder.takeSize(takeSize).takeSide(takeSide).takeOrderType(OrderType.MARKET);
        }
    }

    private void finish(boolean timedOut) {
        finished = true;
        List<ExitResult.HeldPosition> held = new ArrayList<>();
        for (int listingId : listingIds) {
            long net = positions.getPosition(listingId).netQuantity;
            if (net != 0) {
                held.add(new ExitResult.HeldPosition(listingId, net));
            }
        }
        if (listener != null) {
            listener.onExitComplete(new ExitResult(reason, flatten, timedOut, List.copyOf(held)));
        }
    }
}
