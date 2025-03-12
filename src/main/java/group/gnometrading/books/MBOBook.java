package group.gnometrading.books;

import group.gnometrading.annotations.VisibleForTesting;
import group.gnometrading.collections.LongHashMap;
import group.gnometrading.collections.LongMap;
import group.gnometrading.pools.Pool;
import group.gnometrading.pools.PoolNode;
import group.gnometrading.pools.SingleThreadedObjectPool;
import group.gnometrading.schemas.MBODecoder;
import group.gnometrading.schemas.Side;

public class MBOBook {

    private Limit bidTree, askTree;
    private Limit lowestAsk, highestBid;

    private final LongMap<Order> orderMap;
    private final LongMap<Limit> limitMap;
    private final Pool<Limit> limitPool;
    private final Pool<Order> orderPool;

    public MBOBook() {
        this.orderMap = new LongHashMap<>();
        this.limitMap = new LongHashMap<>();

        this.limitPool = new SingleThreadedObjectPool<>(Limit.class);
        this.orderPool = new SingleThreadedObjectPool<>(Order.class);
    }

    @VisibleForTesting
    public Limit getTreeRoot(final boolean isBid) {
        if (isBid) {
            return bidTree;
        } else {
            return askTree;
        }
    }

    public Limit getTopOfBook(final boolean isBid) {
        if (isBid) {
            return highestBid;
        } else {
            return lowestAsk;
        }
    }

    public boolean apply(final MBODecoder mboDecoder) {
        switch (mboDecoder.action()) {
            case Add -> {
                insert(mboDecoder);
                return true;
            }
            case Cancel -> {
                cancel(mboDecoder, false);
                return true;
            }
            case Modify -> {
                modify(mboDecoder);
                return true;
            }
            // TODO: Support Clear?
        }
        return false;
    }

    private Order createOrder(final MBODecoder mboDecoder) {
        PoolNode<Order> orderPoolNode = orderPool.acquire();
        var order = orderPoolNode.getItem();

        order.orderId = mboDecoder.orderId();
        order.isBid = mboDecoder.side() == Side.Bid;
        order.shares = mboDecoder.size(); // uint32
        order.limitPrice = mboDecoder.price();
        order.entryTime = mboDecoder.timestampRecv();
        order.eventTime = mboDecoder.timestampEvent();

        order.nextOrder = order.prevOrder = null;
        order.parentLimit = null;
        order.self = orderPoolNode;

        orderMap.put(order.orderId, order);
        return order;
    }

    private Limit createLimit(final Order order) {
        PoolNode<Limit> limitPoolNode = limitPool.acquire();
        var limit = limitPoolNode.getItem();

        limit.limitPrice = order.limitPrice;
        limit.orders = 0;
        limit.size = 0;
        limit.parent = limit.left = limit.right = null;
        limit.head = limit.tail = null;
        limit.self = limitPoolNode;

        this.limitMap.put(limit.limitPrice, limit);
        limit.addOrder(order);
        return limit;
    }

    private void insert(final MBODecoder mboDecoder) {
        final Order order = this.createOrder(mboDecoder);
        Limit existingLimit = limitMap.get(mboDecoder.price());
        if (existingLimit != null) {
            existingLimit.addOrder(order);
            return;
        }

        existingLimit = this.createLimit(order);
        if (order.isBid) {
            bidTree = BSTUtils.insert(bidTree, existingLimit);
        } else {
            askTree = BSTUtils.insert(askTree, existingLimit);
        }

        if (order.isBid && (highestBid == null || highestBid.limitPrice < existingLimit.limitPrice)) {
            highestBid = existingLimit;
        } else if (!order.isBid && (lowestAsk == null || lowestAsk.limitPrice > existingLimit.limitPrice)) {
            lowestAsk = existingLimit;
        }
    }

    private void cancel(final MBODecoder mboDecoder, final boolean forceRemove) {
        final Order order = this.orderMap.get(mboDecoder.orderId());
        if (order == null) {
            // TODO: Something better here?
            return;
        }

        if (mboDecoder.size() < order.shares && !forceRemove) {
            order.shares -= mboDecoder.size();
            order.parentLimit.size -= mboDecoder.size();
            return;
        }

        this.orderMap.remove(order.orderId);
        final Limit limit = order.parentLimit;
        if (limit.cancelOrder(order)) {
            this.limitMap.remove(limit.limitPrice);
            if (order.isBid) {
                if (highestBid == limit) {
                    highestBid = limit.parent;
                }
                bidTree = BSTUtils.remove(bidTree, limit);
            } else {
                if (lowestAsk == limit) {
                    lowestAsk = limit.parent;
                }
                askTree = BSTUtils.remove(askTree, limit);
            }
            this.limitPool.release(limit.self);
        }
        this.orderPool.release(order.self);
    }

    private void modify(final MBODecoder mboDecoder) {
        final Order order = this.orderMap.get(mboDecoder.orderId());
        if (order == null) {
            // TODO: Something better here?
            return;
        }

        if (order.limitPrice != mboDecoder.price()) {
            cancel(mboDecoder, true);
            insert(mboDecoder);
            return;
        }

        long sharesDiff = order.shares - mboDecoder.size();
        if (sharesDiff < 0) {
            // Remove priority if order increases in size
            this.limitMap.get(order.limitPrice).sendToBack(order);
        }
        order.shares -= sharesDiff;
        order.parentLimit.size -= sharesDiff;
    }
}
