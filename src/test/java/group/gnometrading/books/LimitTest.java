package group.gnometrading.books;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class LimitTest {

    private Map<Integer, Order> orderMap;

    @BeforeEach
    void setup() {
        orderMap = new HashMap<>();
    }

    @Test
    void testBasicInteractions() {
        Limit limit = new Limit();
        limit.limitPrice = 100;

        assertEquals(0, limit.size);
        assertEquals(0, limit.orders);

        Order o1 = createOrder(50);
        limit.addOrder(o1);
        assertLimit(limit, "50");

        Order o2 = createOrder(100);
        limit.addOrder(o2);
        assertLimit(limit, "100->50");


        Order o3 = createOrder(200);
        limit.addOrder(o3);
        assertLimit(limit, "200->100->50");

        limit.cancelOrder(o1);
        assertLimit(limit, "200->100");

        limit.addOrder(o1);
        assertLimit(limit, "50->200->100");

        limit.sendToBack(o3);
        assertLimit(limit, "200->50->100");

        limit.cancelOrder(o2);
        assertLimit(limit, "200->50");

        limit.cancelOrder(o1);
        assertLimit(limit, "200");

        limit.cancelOrder(o3);
        assertLimit(limit, "");
    }

    private void assertLimit(Limit limit, String test) {
        String[] parts;
        if (test.isEmpty()) {
            parts = new String[0];
        } else {
            parts = test.split("->");
        }
        int totalShares = 0;
        assertEquals(parts.length, limit.orders);


        // Assert head and tail
        if (parts.length > 0) {
            Order head = this.orderMap.get(Integer.parseInt(parts[0]));
            assertEquals(head, limit.head);
            assertEquals(Integer.parseInt(parts[0]), head.shares);
            assertNull(head.prevOrder);
            totalShares += Integer.parseInt(parts[0]);

            Order tail = this.orderMap.get(Integer.parseInt(parts[parts.length - 1]));
            assertEquals(Integer.parseInt(parts[parts.length - 1]), tail.shares);
            assertEquals(tail, limit.tail);
            assertNull(tail.nextOrder);
            if (head != tail) {
                totalShares += Integer.parseInt(parts[parts.length - 1]);
            }
        } else {
            assertNull(limit.head);
            assertNull(limit.tail);
        }

        for (int i = 1; i < parts.length - 1; i++) {
            Order prev = this.orderMap.get(Integer.parseInt(parts[i - 1]));
            Order at = this.orderMap.get(Integer.parseInt(parts[i]));
            Order next = this.orderMap.get(Integer.parseInt(parts[i + 1]));

            assertEquals(prev, at.prevOrder);
            assertEquals(next, at.nextOrder);
            assertEquals(at, prev.nextOrder);
            assertEquals(at, next.prevOrder);

            assertEquals(Integer.parseInt(parts[i]), at.shares);
            totalShares += Integer.parseInt(parts[i]);
        }

        assertEquals(totalShares, limit.size);
    }

    private Order createOrder(int size) {
        Order order = new Order();
        order.shares = size;
        orderMap.put(size, order);
        return order;
    }

}