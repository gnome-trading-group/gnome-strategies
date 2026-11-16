package group.gnometrading.strategies.load;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.management.ThreadMXBean;
import group.gnometrading.md.Stream;
import group.gnometrading.md.StreamDirectory;
import group.gnometrading.schemas.Intent;
import group.gnometrading.schemas.IntentDecoder;
import group.gnometrading.schemas.Mbp10Schema;
import group.gnometrading.schemas.OrderExecutionReport;
import group.gnometrading.sequencer.GlobalSequence;
import group.gnometrading.sequencer.SequencedPoller;
import group.gnometrading.sequencer.SequencedRingBuffer;
import group.gnometrading.sm.Exchange;
import group.gnometrading.sm.Listing;
import group.gnometrading.sm.Security;
import group.gnometrading.strategies.StrategyAgent;
import group.gnometrading.strategies.StrategyFactory;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class NoopStrategyTest {

    private static final long SECOND = 1_000_000_000L;
    private static final int KALSHI = 5;

    /** What one intent asked for: listing (by security), bid price and size (null values for a pull), post-only. */
    record Sent(long securityId, long bidPrice, long bidSize, boolean postOnly) {
        boolean isPull() {
            return bidSize == IntentDecoder.bidSizeNullValue();
        }
    }

    private final GlobalSequence sequence = new GlobalSequence();
    private final SequencedRingBuffer<Intent> intentBuffer = new SequencedRingBuffer<>(Intent::new, sequence);
    private final List<Sent> sent = new ArrayList<>();
    private boolean recording = true;
    private final Intent decoded = new Intent();
    private final SequencedPoller intents = intentBuffer.createPoller((seq, templateId, buffer, length) -> {
        if (recording) {
            decoded.wrap(buffer);
            IntentDecoder d = decoded.decoder;
            sent.add(new Sent(
                    d.securityId(), d.bidPrice(), d.bidSize(), d.flags().postOnly()));
        }
    });
    private final Mbp10Schema book = new Mbp10Schema();
    private long clock;

    private NoopStrategy strategy(Long workMicros, Double everySeconds, Integer listings) {
        NoopStrategy strategy = new NoopStrategy(
                7,
                new SequencedRingBuffer<>(Mbp10Schema::new, sequence),
                new SequencedRingBuffer<>(OrderExecutionReport::new, sequence),
                intentBuffer,
                listingId -> null,
                null,
                () -> clock,
                workMicros,
                everySeconds,
                listings,
                null,
                null);
        StreamDirectory directory = new StreamDirectory();
        for (int listingId = 1; listingId <= 3; listingId++) {
            directory.add(
                    new Stream(100 + listingId, 1, "SRC", "LISTING", listingId, null, "BOOK", null, 0, 0, 0, true),
                    listing(listingId));
        }
        strategy.configureStreams(directory);
        return strategy;
    }

    private static Listing listing(int listingId) {
        return new Listing(
                listingId,
                new Exchange(KALSHI, "KALSHI", "Exchange", "us-east-1"),
                new Security(
                        1000 + listingId, "SEC", null, null, null, "USD", "USD", "USD", false, false, 0, 0, true, 0),
                String.valueOf(listingId),
                "SEC");
    }

    /** A book update on the listing at {@code t}, then one pass; returns what the strategy sent. */
    private List<Sent> book(NoopStrategy strategy, int listingId, long t) throws Exception {
        sent.clear();
        step(strategy, listingId, t);
        return new ArrayList<>(sent);
    }

    /** As {@link #book}, keeping nothing: what a measurement of the strategy's own allocation needs. */
    private void step(NoopStrategy strategy, int listingId, long t) throws Exception {
        clock = t;
        book.encoder
                .streamId(100 + listingId)
                .timestampEvent(t)
                .timestampRecv(t)
                .bidPrice0(500_000_000L);
        strategy.submitMarketData(book);
        strategy.doWork();
        intents.poll();
    }

    @Test
    void withoutOrdersItReadsEveryRecordAndSendsNothing() throws Exception {
        NoopStrategy strategy = strategy(null, null, null);

        for (int i = 0; i < 10; i++) {
            assertTrue(book(strategy, 1 + i % 3, i * SECOND).isEmpty());
        }

        assertEquals(10, strategy.activity().get(0), "every record read");
    }

    @Test
    void eachOrderListingInTurnPostsAFarBidThenCancelsIt() throws Exception {
        // Two listings, each acting every 2 s: an action a second, alternating between them.
        NoopStrategy strategy = strategy(null, 2.0, 2);

        List<Sent> first = book(strategy, 1, 0);
        assertEquals(List.of(new Sent(1001, 10_000_000L, 1_000_000L, true)), first, "a 1-cent post-only bid of 1");
        assertTrue(book(strategy, 2, SECOND / 2).isEmpty(), "not due yet");
        List<Sent> second = book(strategy, 2, SECOND);
        assertEquals(1, second.size());
        assertEquals(1002, second.get(0).securityId());
        assertTrue(!second.get(0).isPull());

        assertTrue(book(strategy, 3, 2 * SECOND).get(0).isPull(), "listing 1's bid cancelled");
        assertEquals(1001, sent.get(0).securityId());
        List<Sent> fourth = book(strategy, 3, 3 * SECOND);
        assertEquals(1002, fourth.get(0).securityId(), "listing 3 is past the two order listings");
        assertTrue(fourth.get(0).isPull());
        assertEquals(1001, book(strategy, 1, 4 * SECOND).get(0).securityId(), "and round again");
    }

    @Test
    void afterAStallTheActionsResumeSpacedRatherThanInABurst() throws Exception {
        NoopStrategy strategy = strategy(null, 1.0, 1);
        book(strategy, 1, 0);

        assertEquals(1, book(strategy, 1, 60 * SECOND).size());
        assertTrue(book(strategy, 1, 60 * SECOND + 1).isEmpty());
        assertEquals(1, book(strategy, 1, 61 * SECOND).size());
    }

    @Test
    void itSpendsTheConfiguredTimeOnEachRecord() throws Exception {
        NoopStrategy strategy = strategy(200L, null, null);

        long start = System.nanoTime();
        for (int i = 0; i < 50; i++) {
            book(strategy, 1, i);
        }

        assertTrue(System.nanoTime() - start >= 50 * 200_000L, "50 records at 200 µs each");
    }

    @Test
    void readingAndOrderingAllocateNothing() throws Exception {
        NoopStrategy strategy = strategy(null, 0.001, 3);
        recording = false;
        for (int i = 0; i < 20_000; i++) {
            step(strategy, 1 + i % 3, i * 1_000_000L);
        }

        long before = allocatedOnThisThread();
        for (int i = 20_000; i < 120_000; i++) {
            step(strategy, 1 + i % 3, i * 1_000_000L);
        }
        long allocated = allocatedOnThisThread() - before;

        // A few kilobytes the JIT and first-time paths leave, the same whatever the count.
        assertTrue(allocated < 8_192, "bytes allocated over 100,000 records and their orders: " + allocated);
    }

    @Test
    void theLaunchersArgumentsBuildIt() {
        StrategyAgent agent = StrategyFactory.createWithOwnBuffers(
                NoopStrategy.class.getName(),
                16,
                listingId -> null,
                null,
                Map.of("work_micros", 20, "order_every_seconds", 1, "order_listings", 5));

        assertInstanceOf(NoopStrategy.class, agent);
    }

    private static long allocatedOnThisThread() {
        return ((ThreadMXBean) ManagementFactory.getThreadMXBean())
                .getThreadAllocatedBytes(Thread.currentThread().getId());
    }
}
