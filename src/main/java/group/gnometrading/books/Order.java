package group.gnometrading.books;

import group.gnometrading.pools.PoolNode;

public class Order {
    long orderId = 0;
    boolean isBid = false;
    long shares = 0;
    long limitPrice = 0;
    long entryTime = 0;
    long eventTime = 0;

    Order nextOrder = null;
    Order prevOrder = null;
    Limit parentLimit = null;
    PoolNode<Order> self = null;
}
