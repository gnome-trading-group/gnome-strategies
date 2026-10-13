package group.gnometrading.collector;

import com.github.luben.zstd.ZstdOutputStream;
import group.gnometrading.logging.LogMessage;
import group.gnometrading.logging.Logger;
import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.concurrent.CountDownLatch;

/**
 * Splits a stream of events into fixed-length cycles, one file per cycle, and hands finished cycles to the uploader.
 *
 * <p>Runs on the thread receiving data, so the per-event methods take epoch nanoseconds and never allocate. A
 * {@link LocalDateTime} is built only on rotation, once per cycle, for the file's key.
 */
final class CycleManager implements Closeable {

    @FunctionalInterface
    interface KeyFactory {
        String createKey(LocalDateTime timestamp);
    }

    private final Logger logger;
    private final CycleUploader uploader;
    private final String bucketName;
    private final KeyFactory keyFactory;
    private final ZoneId zone;
    private final long cycleNanos;
    private final long gracePeriodNanos;

    private CycleBuffer currentCycle;
    private long currentStartNanos;
    private CycleBuffer previousCycle;
    private long previousStartNanos;

    private volatile boolean closed;
    private volatile boolean shutdownRequested;
    private final CountDownLatch cycleFlippedLatch;

    CycleManager(
            Logger logger,
            CycleUploader uploader,
            String bucketName,
            KeyFactory keyFactory,
            ZoneId zone,
            Duration cycleLength,
            long initialNanos,
            Duration gracePeriod,
            boolean attachShutdownHook) {
        this.logger = logger;
        this.uploader = uploader;
        this.bucketName = bucketName;
        this.keyFactory = keyFactory;
        this.zone = zone;
        this.cycleNanos = cycleLength.toNanos();
        this.gracePeriodNanos = gracePeriod.toNanos();
        this.closed = false;
        this.shutdownRequested = false;
        this.cycleFlippedLatch = new CountDownLatch(1);
        this.currentStartNanos = cycleStart(initialNanos);
        this.currentCycle = newCycle(this.currentStartNanos);
        this.previousCycle = null;

        if (attachShutdownHook) {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    this.waitForCycleAndClose();
                } catch (Exception e) {
                    logger.logf(LogMessage.UNKNOWN_ERROR, "Error trying to cycle files: %s", e.getMessage());
                }
            }));
        }
    }

    private CycleBuffer newCycle(long startNanos) {
        LocalDateTime start = LocalDateTime.ofInstant(Instant.ofEpochSecond(0, startNanos), zone);
        return new CycleBuffer(start, keyFactory.createKey(start));
    }

    static long epochNanos(Instant instant) {
        return instant.getEpochSecond() * 1_000_000_000L + instant.getNano();
    }

    /** The start of the cycle containing {@code eventNanos}. */
    long cycleStart(long eventNanos) {
        return eventNanos - Math.floorMod(eventNanos, cycleNanos);
    }

    /** Whether {@code eventNanos} falls in a cycle after the current one. */
    boolean isPastCurrentCycle(long eventNanos) {
        return eventNanos - currentStartNanos >= cycleNanos;
    }

    boolean rotate(long newCycleStartNanos) {
        if (previousCycle != null) {
            uploader.upload(previousCycle, bucketName);
            previousCycle = null;
        }

        CycleBuffer next = newCycle(newCycleStartNanos);
        logger.logf(LogMessage.DEBUG, "Switching cycle to %s from %s", next.timestamp(), currentCycle.timestamp());

        previousCycle = currentCycle;
        previousStartNanos = currentStartNanos;
        currentCycle = next;
        currentStartNanos = newCycleStartNanos;

        if (shutdownRequested) {
            // Inline so the cycle is in S3 before the shutdown hook waiting on the latch lets the JVM exit.
            uploader.uploadNow(previousCycle, bucketName);
            previousCycle = null;
            cycleFlippedLatch.countDown();
            return true;
        }

        return false;
    }

    void maybeExpireGracePeriod(long eventNanos) {
        if (previousCycle == null) {
            return;
        }
        if (eventNanos - currentStartNanos >= gracePeriodNanos) {
            uploader.upload(previousCycle, bucketName);
            previousCycle = null;
        }
    }

    ZstdOutputStream resolveTarget(long eventCycleStartNanos) {
        if (previousCycle == null) {
            return currentCycle.outputStream();
        }
        if (eventCycleStartNanos == previousStartNanos) {
            return previousCycle.outputStream();
        }
        if (eventCycleStartNanos < previousStartNanos) {
            logger.logf(
                    LogMessage.DEBUG,
                    "Event in cycle starting %d ns predates previous cycle %s, writing to current cycle",
                    eventCycleStartNanos,
                    previousCycle.timestamp());
        }
        return currentCycle.outputStream();
    }

    ZstdOutputStream currentOutputStream() {
        return currentCycle.outputStream();
    }

    long currentStartNanos() {
        return currentStartNanos;
    }

    boolean isClosed() {
        return closed;
    }

    void waitForCycleAndClose() throws IOException, InterruptedException {
        logger.logf(LogMessage.DEBUG, "Shutdown requested, waiting for cycle to flip before closing");
        shutdownRequested = true;
        cycleFlippedLatch.await();
        logger.logf(LogMessage.DEBUG, "Cycle flipped, proceeding with close");
        close();
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            logger.logf(LogMessage.DEBUG, "CycleManager already closed, skipping");
            return;
        }
        closed = true;
        if (previousCycle != null) {
            previousCycle.close();
        }
        currentCycle.close();
        logger.logf(LogMessage.DEBUG, "CycleManager closed successfully");
    }
}
