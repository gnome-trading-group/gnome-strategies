package group.gnometrading.books;

import group.gnometrading.pools.PoolNode;

/**
 * Limit represents a single price level in an order book. It is a container
 * for a doubly-linked list of Orders.
 */
public class Limit {
    long limitPrice = -1;
    long size = 0;
    int orders = 0;
    Limit parent = null;
    Limit left, right = null;
    Order head, tail = null;
    PoolNode<Limit> self = null;

    /**
     * Add a new order to the back of the limit's queue.
     */
    public void addOrder(final Order order) {
        order.parentLimit = this;
        order.nextOrder = head;
        order.prevOrder = null;

        if (head != null) {
            head.prevOrder = order;
        } else {
            tail = order;
        }

        head = order;
        orders++;
        size += order.shares;
    }

    /**
     * Remove the priority on an existing order by placing it in the back of the queue.
     */
    public void sendToBack(final Order order) {
        // Removal
        if (order.prevOrder != null) {
            order.prevOrder.nextOrder = order.nextOrder;
        }
        if (order.nextOrder != null) {
            order.nextOrder.prevOrder = order.prevOrder;
        }
        if (head == order) {
            head = order.nextOrder;
        }
        if (tail == order) {
            tail = order.prevOrder;
        }

        // Insertion
        order.nextOrder = head;
        order.prevOrder = null;

        if (head != null) {
            head.prevOrder = order;
        } else {
            tail = order;
        }
        head = order;
    }

    /**
     * Remove an existing order from the limit level. Return true if no more orders exist.
     */
    public boolean cancelOrder(final Order order) {
        if (order.prevOrder != null) {
            order.prevOrder.nextOrder = order.nextOrder;
        }
        if (order.nextOrder != null) {
            order.nextOrder.prevOrder = order.prevOrder;
        }
        if (head == order) {
            head = order.nextOrder;
        }
        if (tail == order) {
            tail = order.prevOrder;
        }

        orders--;
        size -= order.shares;
        return orders == 0;
    }
}
