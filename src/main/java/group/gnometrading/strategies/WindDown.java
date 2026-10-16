package group.gnometrading.strategies;

import group.gnometrading.oms.position.Position;
import group.gnometrading.oms.position.PositionView;
import group.gnometrading.schemas.ExecType;
import group.gnometrading.schemas.Intent;
import group.gnometrading.schemas.IntentDecoder;
import group.gnometrading.schemas.OrderExecutionReport;
import group.gnometrading.schemas.OrderType;
import group.gnometrading.schemas.RejectReason;
import group.gnometrading.schemas.Side;
import group.gnometrading.sm.Listing;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * Takes a strategy out of the market once it has asked to exit: cancels its quotes, then, if asked, closes what it
 * holds with market orders, until nothing is working (and it is flat) or the time runs out. Driven by
 * {@link #step} on every pass of the strategy's loop; allocation-free until it finishes.
 *
 * <p>A listing it can't close is given up on rather than retried until the time runs out: a remainder below the
 * venue's minimum order (a partial fill leaves one) is refused every time, and so, after a few tries, is a close the
 * venue keeps refusing for any other reason.
 */
final class WindDown {

    // Long enough for the OMS to have taken in the last close before it is judged, so a close is never doubled.
    static final long RETRY_INTERVAL_NANOS = 1_000_000_000L;
    // Enough for a rate limit or a briefly empty book to pass; a refusal that lasts longer won't pass in time.
    static final int MAX_CONSECUTIVE_REJECTS = 5;

    private final int[] listingIds;
    private final int[] exchangeIds;
    private final int[] securityIds;
    private final long[] lastCloseNanos;
    private final boolean[] closeSent;
    private final int[] consecutiveRejects;
    // Null until the listing is given up on.
    private final ExitResult.Why[] givenUp;
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
        this.closeSent = new boolean[listings.size()];
        this.consecutiveRejects = new int[listings.size()];
        this.givenUp = new ExitResult.Why[listings.size()];
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

    /**
     * Learns from the closes' outcomes. Only counts once a close has gone out on the listing, so an order the strategy
     * sent before exiting can't make it give up.
     */
    void onExecutionReport(OrderExecutionReport report) {
        if (!started || finished || !flatten) {
            return;
        }
        int index = indexOf(report.decoder.exchangeId(), report.decoder.securityId());
        if (index < 0 || !closeSent[index] || givenUp[index] != null) {
            return;
        }
        ExecType type = report.decoder.execType();
        if (type == ExecType.FILL || type == ExecType.PARTIAL_FILL) {
            consecutiveRejects[index] = 0;
        } else if (type == ExecType.REJECT) {
            onRejected(index, report.decoder.rejectReason());
        }
    }

    private void onRejected(int index, RejectReason rejectReason) {
        // The OMS refuses a size below the listing's minimum order, minimum notional or lot as INVALID_SIZE: what is
        // left can't be closed by any one order, so trying again can't help.
        if (rejectReason == RejectReason.INVALID_SIZE) {
            givenUp[index] = ExitResult.Why.TOO_SMALL;
        } else if (++consecutiveRejects[index] >= MAX_CONSECUTIVE_REJECTS) {
            givenUp[index] = ExitResult.Why.REJECTED;
        }
    }

    /** Closes what the listing still holds if nothing is working there; true once there's nothing left to do. */
    private boolean settle(int index, long nowNanos) {
        Position position = positions.getPosition(listingIds[index]);
        long net = position.netQuantity;
        boolean working = position.leavesBuyQty != 0 || position.leavesSellQty != 0;
        boolean open = flatten && net != 0 && givenUp[index] == null;
        if (open && !working && nowNanos - lastCloseNanos[index] >= RETRY_INTERVAL_NANOS) {
            lastCloseNanos[index] = nowNanos;
            closeSent[index] = true;
            close(index, net);
        }
        return !working && !open;
    }

    private int indexOf(int exchangeId, long securityId) {
        for (int i = 0; i < listingIds.length; i++) {
            if (exchangeIds[i] == exchangeId && securityIds[i] == securityId) {
                return i;
            }
        }
        return -1;
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

    private ExitResult.Why why(int index) {
        if (!flatten) {
            return ExitResult.Why.KEPT;
        }
        return givenUp[index] != null ? givenUp[index] : ExitResult.Why.TIMED_OUT;
    }

    private void finish(boolean timedOut) {
        finished = true;
        List<ExitResult.HeldPosition> held = new ArrayList<>();
        for (int i = 0; i < listingIds.length; i++) {
            long net = positions.getPosition(listingIds[i]).netQuantity;
            if (net != 0) {
                held.add(new ExitResult.HeldPosition(listingIds[i], net, why(i)));
            }
        }
        if (listener != null) {
            listener.onExitComplete(new ExitResult(reason, flatten, timedOut, List.copyOf(held)));
        }
    }
}
