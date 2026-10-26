package group.gnometrading.collector;

import group.gnometrading.logging.LogMessage;
import group.gnometrading.logging.Logger;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

/**
 * Splits a stream of events into fixed-length cycles, one file per cycle, and hands finished cycles to the uploader.
 *
 * <p>Runs on the thread receiving data, so the per-event methods take epoch nanoseconds and never allocate. A
 * {@link LocalDateTime} is built only on rotation, once per cycle, for the file's key.
 *
 * <p>{@link #flush} hands over the open cycles, the current one partial, when its stream stops: the process shutting
 * down or the stream being quarantined. It must run once the thread receiving data has stopped.
 */
final class CycleManager {

    @FunctionalInterface
    interface KeyFactory {
        String createKey(LocalDateTime timestamp);
    }

    private final Logger logger;
    private final CycleUploader uploader;
    private final DirectBufferPool pool;
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

    CycleManager(
            Logger logger,
            CycleUploader uploader,
            DirectBufferPool pool,
            String bucketName,
            KeyFactory keyFactory,
            ZoneId zone,
            Duration cycleLength,
            long initialNanos,
            Duration gracePeriod) {
        this.logger = logger;
        this.uploader = uploader;
        this.pool = pool;
        this.bucketName = bucketName;
        this.keyFactory = keyFactory;
        this.zone = zone;
        this.cycleNanos = cycleLength.toNanos();
        this.gracePeriodNanos = gracePeriod.toNanos();
        this.currentStartNanos = cycleStart(initialNanos);
        this.currentCycle = newCycle(this.currentStartNanos);
    }

    private CycleBuffer newCycle(long startNanos) {
        LocalDateTime start = LocalDateTime.ofInstant(Instant.ofEpochSecond(0, startNanos), zone);
        return new CycleBuffer(pool, start, keyFactory.createKey(start));
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

    void rotate(long newCycleStartNanos) {
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

    /** The cycle an event belongs in: the previous one while its grace period lasts, else the current one. */
    CycleBuffer resolveTarget(long eventCycleStartNanos) {
        if (previousCycle == null) {
            return currentCycle;
        }
        if (eventCycleStartNanos == previousStartNanos) {
            return previousCycle;
        }
        if (eventCycleStartNanos < previousStartNanos) {
            logger.logf(
                    LogMessage.DEBUG,
                    "Event in cycle starting %d ns predates previous cycle %s, writing to current cycle",
                    eventCycleStartNanos,
                    previousCycle.timestamp());
        }
        return currentCycle;
    }

    CycleBuffer currentCycle() {
        return currentCycle;
    }

    long currentStartNanos() {
        return currentStartNanos;
    }

    boolean isClosed() {
        return closed;
    }

    /**
     * Hands the previous cycle and the partial current one to the uploader and stops taking events. Each partial file
     * keeps its cycle's key; the merger chooses between it and any other copy of that minute.
     */
    void flush() {
        if (closed) {
            return;
        }
        closed = true;
        if (previousCycle != null) {
            uploader.upload(previousCycle, bucketName);
            previousCycle = null;
        }
        logger.logf(LogMessage.DEBUG, "Flushing partial cycle %s", currentCycle.timestamp());
        uploader.upload(currentCycle, bucketName);
    }
}
