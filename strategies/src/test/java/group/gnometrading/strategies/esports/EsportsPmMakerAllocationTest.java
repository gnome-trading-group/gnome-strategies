package group.gnometrading.strategies.esports;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.management.ThreadMXBean;
import group.gnometrading.schemas.OrderStatus;
import java.lang.management.ManagementFactory;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * The maker decides on every book update of six listings, so its decision path must not allocate: re-pricing both
 * bids as the hedge moves, and taking a fill through its hedge. Measured on this thread with the harness keeping
 * nothing, and the engine's clock (which stamps each intent) replaced by one that allocates nothing.
 */
class EsportsPmMakerAllocationTest {

    private static final double[][] BIDS = {{0.44, 100}, {0.43, 200}};
    private static final double[][] ASKS_A = {{0.48, 60}, {0.49, 100}, {0.52, 300}};
    private static final double[][] ASKS_B = {{0.47, 60}, {0.49, 100}, {0.52, 300}};
    private static final double[][] PMI_A_ASKS = {{0.60, 100}};
    private static final double[][] PMI_A_BIDS = {{0.40, 100}};

    private static long allocatedOnThisThread() {
        return ((ThreadMXBean) ManagementFactory.getThreadMXBean())
                .getThreadAllocatedBytes(Thread.currentThread().getId());
    }

    private static int cycle(MakerHarness s, int from, int count) {
        int hedges = 0;
        for (int i = from; i < from + count; i++) {
            long t = i * 1_000_000L;
            s.submitBook(302, i % 2 == 0 ? ASKS_A : ASKS_B, BIDS, t, false);
            s.step();
            s.submitBook(101, PMI_A_ASKS, PMI_A_BIDS, t, false);
            s.step();
            if (i % 50 == 0) {
                s.hold(101, 20);
                s.submitReport(101, OrderStatus.PARTIALLY_FILLED, 20, t);
                s.step();
                s.hold(302, 20);
                s.submitReport(302, OrderStatus.FILLED, 20, t);
                s.step();
                s.hold(101, 0);
                s.hold(302, 0);
                hedges++;
            }
        }
        return hedges;
    }

    @Test
    void quotingAndHedgingAllocateNothing() {
        MakerHarness s = new MakerHarness(Map.of());
        s.feed(MakerHarness.BOOKS, 0);
        assertTrue(MakerHarness.quotes(s.feed(MakerHarness.BOOKS, 0)).isEmpty(), "quoting before the measurement");
        s.recording = false;

        cycle(s, 1, 20_000);
        long before = allocatedOnThisThread();
        int hedges = cycle(s, 20_001, 100_000);
        long allocated = allocatedOnThisThread() - before;

        assertEquals(2_000, hedges);
        // A few kilobytes the JIT and first-time paths leave, the same whatever the count: twice the updates leave
        // about the same.
        assertTrue(allocated < 8_192, "bytes allocated over 200,000 book updates and 2,000 hedged fills: " + allocated);
    }
}
