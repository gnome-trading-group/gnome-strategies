package group.gnometrading.books;

import group.gnometrading.schemas.*;
import org.agrona.ExpandableDirectByteBuffer;
import org.agrona.MutableDirectBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class MBOBookTest {

    private static final MBODecoder marketUpdateDecoder = new MBODecoder();
    private static final MBOEncoder marketUpdateEncoder = new MBOEncoder();
    private static final MutableDirectBuffer buffer = new ExpandableDirectByteBuffer();
    private static MBOBook book;

    @BeforeEach
    void setup() {
        book = new MBOBook();
        marketUpdateEncoder.wrapAndApplyHeader(buffer, 0, new MessageHeaderEncoder());
        marketUpdateDecoder.wrapAndApplyHeader(buffer, 0, new MessageHeaderDecoder());
    }

    private static Stream<Arguments> testTopOfBookArguments() {
        return Stream.of(
                Arguments.of((Runnable) () -> {}, null, null),
                Arguments.of((Runnable) () -> {
                    add(0x1, 100, 1, 'A');
                }, 100L, null),
                Arguments.of((Runnable) () -> {
                    add(0x1, 100, 1, 'A');
                    add(0x2, 95, 10, 'B');
                }, 100L, 95L),
                Arguments.of((Runnable) () -> {
                    add(0x1, 100, 1, 'A');
                    add(0x2, 95, 10, 'B');
                    cancel(0x1, 100, 1, 'A');
                }, null, 95L),
                Arguments.of((Runnable) () -> {
                    add(0x1, 100, 1, 'A');
                    add(0x2, 95, 10, 'B');
                    add(0x3, 99, 5, 'A');
                }, 99L, 95L),
                Arguments.of((Runnable) () -> {
                    add(0x1, 100, 1, 'A');
                    add(0x2, 95, 10, 'B');
                    add(0x3, 99, 5, 'A');
                    add(0x4, 98, 30, 'B');
                }, 99L, 98L),
                Arguments.of((Runnable) () -> {
                    add(0x1, 100, 1, 'A');
                    add(0x2, 95, 10, 'B');
                    add(0x3, 99, 5, 'A');
                    add(0x4, 98, 30, 'B');
                    cancel(0x3, 99, 5, 'A');
                }, 100L, 98L),
                Arguments.of((Runnable) () -> {
                    add(0x1, 100, 1, 'A');
                    add(0x2, 95, 10, 'B');
                    add(0x3, 99, 5, 'A');
                    add(0x4, 98, 30, 'B');
                    cancel(0x3, 99, 5, 'A');
                    cancel(0x4, 98, 4, 'B'); // Not removing entire order
                }, 100L, 98L),
                Arguments.of((Runnable) () -> {
                    add(0x1, 100, 1, 'A');
                    add(0x2, 95, 10, 'B');
                    add(0x3, 99, 5, 'A');
                    add(0x4, 98, 30, 'B');
                    cancel(0x3, 99, 5, 'A');
                    cancel(0x4, 98, 4, 'B');
                    modify(0x4, 98, 100, 'B');
                }, 100L, 98L),
                Arguments.of((Runnable) () -> {
                    add(0x1, 100, 1, 'A');
                    add(0x2, 95, 10, 'B');
                    add(0x3, 99, 5, 'A');
                    add(0x4, 98, 30, 'B');
                    cancel(0x3, 99, 5, 'A');
                    cancel(0x4, 98, 4, 'B');
                    modify(0x4, 97, 100, 'B');
                }, 100L, 97L)
        );
    }

    @ParameterizedTest
    @MethodSource("testTopOfBookArguments")
    void testTopOfBook(Runnable buildBook, Long askPrice, Long bidPrice) {
        buildBook.run();
        if (askPrice == null) {
            assertNull(book.getTopOfBook(false));
        } else {
            assertEquals(askPrice, book.getTopOfBook(false).limitPrice);
        }

        if (bidPrice == null) {
            assertNull(book.getTopOfBook(true));
        } else {
            assertEquals(bidPrice, book.getTopOfBook(true).limitPrice);
        }
    }

    @Test
    void testIgnoresActions() {
        add(1, 100, 1, 'A');
        add(2, 95, 10, 'B');
        add(3, 99, 5, 'A');
        add(4, 98, 30, 'B');
        cancel(3, 99, 5, 'A');
        String bids = "98=4@30~95=2@10";
        String asks = "100=1@1";

        encodeAction(1, 100, 5, 'A', Action.Fill);
        encodeAction(1, 100, 10, 'A', Action.Trade);
        assertBook(bids, true);
        assertBook(asks, false);
    }

    private static Stream<Arguments> testBookArguments() {
        return Stream.of(
                Arguments.of((Runnable) () -> {}, "", ""),
                Arguments.of((Runnable) () -> {
                    add(1234, 100, 1, 'A');
                }, "", "100=1234@1"),
                Arguments.of((Runnable) () -> {
                    add(1, 100, 1, 'A');
                    add(2, 95, 10, 'B');
                }, "95=2@10", "100=1@1"),
                Arguments.of((Runnable) () -> {
                    add(1, 100, 1, 'A');
                    add(2, 95, 10, 'B');
                    cancel(1, 100, 1, 'A');
                }, "95=2@10", ""),
                Arguments.of((Runnable) () -> {
                    add(1, 100, 1, 'A');
                    add(2, 95, 10, 'B');
                    add(3, 99, 5, 'A');
                }, "95=2@10", "99=3@5~100=1@1"),
                Arguments.of((Runnable) () -> {
                    add(1, 100, 1, 'A');
                    add(2, 95, 10, 'B');
                    add(3, 99, 5, 'A');
                    add(4, 98, 30, 'B');
                }, "98=4@30~95=2@10", "99=3@5~100=1@1"),
                Arguments.of((Runnable) () -> {
                    add(1, 100, 1, 'A');
                    add(2, 95, 10, 'B');
                    add(3, 99, 5, 'A');
                    add(4, 98, 30, 'B');
                    cancel(3, 99, 5, 'A');
                }, "98=4@30~95=2@10", "100=1@1"),
                Arguments.of((Runnable) () -> {
                    add(1, 100, 1, 'A');
                    add(2, 95, 10, 'B');
                    add(3, 99, 5, 'A');
                    add(4, 98, 30, 'B');
                    cancel(3, 99, 5, 'A');
                    cancel(4, 98, 4, 'B'); // Not removing entire order
                }, "98=4@26~95=2@10", "100=1@1"),
                Arguments.of((Runnable) () -> {
                    add(1, 100, 1, 'A');
                    add(2, 95, 10, 'B');
                    add(3, 99, 5, 'A');
                    add(4, 98, 30, 'B');
                    cancel(3, 99, 5, 'A');
                    cancel(4, 98, 4, 'B');
                    modify(4, 98, 100, 'B');
                }, "98=4@100~95=2@10", "100=1@1"),
                Arguments.of((Runnable) () -> {
                    add(1, 100, 1, 'A');
                    add(2, 95, 10, 'B');
                    add(3, 99, 5, 'A');
                    add(4, 98, 30, 'B');
                    cancel(3, 99, 5, 'A');
                    cancel(4, 98, 4, 'B');
                    modify(4, 97, 100, 'B');
                }, "97=4@100~95=2@10", "100=1@1"),
                Arguments.of((Runnable) () -> {
                    add(1, 100, 1, 'A');
                    add(2, 95, 10, 'B');
                    add(3, 99, 5, 'A');
                    add(4, 98, 30, 'B');
                    add(5, 100, 10, 'A');
                    add(6, 95, 100, 'B');
                }, "98=4@30~95=6@100,2@10", "99=3@5~100=5@10,1@1"),
                Arguments.of((Runnable) () -> {
                    add(1, 100, 1, 'A');
                    add(2, 95, 10, 'B');
                    add(3, 99, 5, 'A');
                    add(4, 98, 30, 'B');
                    add(5, 100, 10, 'A');
                    add(6, 95, 100, 'B');
                }, "98=4@30~95=6@100,2@10", "99=3@5~100=5@10,1@1"),
                Arguments.of((Runnable) () -> {
                    add(1, 100, 1, 'A');
                    add(2, 95, 10, 'B');
                    add(3, 99, 5, 'A');
                    add(4, 98, 30, 'B');
                    add(5, 100, 10, 'A');
                    add(6, 95, 100, 'B');
                    add(7, 98, 50, 'B');
                    add(8, 98, 150, 'B');
                    cancel(4, 98, 30, 'B');
                }, "98=8@150,7@50~95=6@100,2@10", "99=3@5~100=5@10,1@1"),
                Arguments.of((Runnable) () -> {
                    add(1, 100, 1, 'A');
                    add(2, 95, 10, 'B');
                    add(3, 99, 5, 'A');
                    add(4, 98, 30, 'B');
                    add(5, 100, 10, 'A');
                    add(6, 95, 100, 'B');
                    add(7, 98, 50, 'B');
                    add(8, 98, 150, 'B');
                    cancel(4, 98, 30, 'B');
                }, "98=8@150,7@50~95=6@100,2@10", "99=3@5~100=5@10,1@1"),
                Arguments.of((Runnable) () -> {
                    add(1, 100, 1, 'A');
                    add(2, 95, 10, 'B');
                    add(3, 99, 5, 'A');
                    add(4, 98, 30, 'B');
                    add(5, 100, 10, 'A');
                    add(6, 95, 100, 'B');
                    add(7, 98, 50, 'B');
                    add(8, 98, 150, 'B');
                    cancel(4, 98, 30, 'B');
                    modify(7, 98, 52, 'B');
                }, "98=7@52,8@150~95=6@100,2@10", "99=3@5~100=5@10,1@1"),
                Arguments.of((Runnable) () -> {
                    add(1, 100, 1, 'A');
                    add(2, 95, 10, 'B');
                    add(3, 99, 5, 'A');
                    add(4, 98, 30, 'B');
                    add(5, 100, 10, 'A');
                    add(6, 95, 100, 'B');
                    add(7, 98, 50, 'B');
                    add(8, 98, 150, 'B');
                    cancel(4, 98, 30, 'B');
                    modify(7, 98, 52, 'B');
                    modify(6, 98, 100, 'B');
                }, "98=6@100,7@52,8@150~95=2@10", "99=3@5~100=5@10,1@1")
        );
    }

    @ParameterizedTest
    @MethodSource("testBookArguments")
    void testBook(Runnable buildBook, String bids, String asks) {
        buildBook.run();
        assertBook(bids, true);
        assertBook(asks, false);
    }

    private static void assertBook(String bookString, boolean bid) {
        if (bookString.isEmpty()) {
            assertNull(book.getTopOfBook(bid));
            assertNull(book.getTreeRoot(bid));
            return;
        }
        String[] parts = bookString.split("~");
        int[] index = new int[] {0};
        assertBookLevel(book.getTreeRoot(bid), bid, parts, index);
        assertEquals(parts.length, index[0]);
    }

    private static void assertBookLevel(Limit at, boolean bid, String[] parts, int[] index) {
        if (at == null) {
            return;
        }
        assertBookLevel(bid ? at.right : at.left, bid, parts, index);
        assertTrue(index[0] < parts.length);
        assertLimit(at, parts[index[0]++]);
        assertBookLevel(bid ? at.left : at.right, bid, parts, index);
    }

    private static void assertLimit(Limit at, String limitString) {
        String[] parts = limitString.split("=");
        long price = Long.parseLong(parts[0]);
        assertEquals(price, at.limitPrice);

        if (parts[1].isEmpty()) {
            assertNull(at.head);
            assertNull(at.tail);
            assertEquals(0, at.orders);
            assertEquals(0, at.size);
            return;
        }

        String[] sizes = parts[1].split(",");
        assertEquals(sizes.length, at.orders);

        int idx = 0;
        Order order = at.head;
        Order last = null;
        long totalSize = 0;
        while (order != null) {
            String[] orderParts = sizes[idx++].split("@");
            long orderId = Long.parseLong(orderParts[0]);
            long size = Long.parseLong(orderParts[1]);
            assertEquals(orderId, order.orderId);
            assertEquals(price, order.limitPrice);
            assertEquals(at, order.parentLimit);
            assertEquals(size, order.shares);
            assertEquals(last, order.prevOrder);
            totalSize += size;

            last = order;
            order = order.nextOrder;
        }
        assertEquals(sizes.length, idx);
        assertEquals(totalSize, at.size);
    }

    private static void encodeAction(long orderId, long price, int size, char side, Action action) {
        marketUpdateEncoder.orderId(orderId)
                .price(price)
                .size(size)
                .side(side == 'A' ? Side.Ask : Side.Bid)
                .action(action);
        book.apply(marketUpdateDecoder);
    }

    private static void add(long orderId, long price, int size, char side) {
        encodeAction(orderId, price, size, side, Action.Add);
    }

    private static void cancel(long orderId, long price, int size, char side) {
        encodeAction(orderId, price, size, side, Action.Cancel);
    }

    private static void modify(long orderId, long price, int size, char side) {
        encodeAction(orderId, price, size, side, Action.Modify);
    }
}