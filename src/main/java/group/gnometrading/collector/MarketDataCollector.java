package group.gnometrading.collector;

import com.github.luben.zstd.ZstdOutputStream;
import com.lmax.disruptor.EventHandler;
import group.gnometrading.annotations.VisibleForTesting;
import group.gnometrading.data.MarketDataEntry;
import group.gnometrading.logging.LogMessage;
import group.gnometrading.logging.Logger;
import group.gnometrading.schemas.Schema;
import group.gnometrading.sm.Listing;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.agrona.ExpandableArrayBuffer;
import software.amazon.awssdk.services.s3.S3Client;

public final class MarketDataCollector implements EventHandler<Schema>, Closeable {

    private static class CycleBuffer {
        final MarketDataEntry entry;
        final ByteArrayOutputStream rawBuffer;
        ZstdOutputStream outputStream;

        CycleBuffer(MarketDataEntry entry) {
            this.entry = entry;
            this.rawBuffer = new ByteArrayOutputStream();
            try {
                this.outputStream = new ZstdOutputStream(this.rawBuffer);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private final Logger logger;
    private final Clock clock;
    private final S3Client s3Client;
    private final Listing listing;
    private final String bucketName;
    private final Duration gracePeriod;

    private CycleBuffer currentCycle;
    private CycleBuffer previousCycle;

    private final ExpandableArrayBuffer purgatory;

    public volatile long lastEventNanos;
    private volatile boolean closed;
    private volatile boolean shutdownRequested;
    private final CountDownLatch cycleFlippedLatch;

    public MarketDataCollector(Logger logger, Clock clock, S3Client s3Client, Listing listing, String bucketName) {
        this(logger, clock, s3Client, listing, bucketName, true, Duration.ofSeconds(5));
    }

    @VisibleForTesting
    MarketDataCollector(
            Logger logger,
            Clock clock,
            S3Client s3Client,
            Listing listing,
            String bucketName,
            boolean attachShutdownHook) {
        this(logger, clock, s3Client, listing, bucketName, attachShutdownHook, Duration.ofSeconds(5));
    }

    @VisibleForTesting
    MarketDataCollector(
            Logger logger,
            Clock clock,
            S3Client s3Client,
            Listing listing,
            String bucketName,
            boolean attachShutdownHook,
            Duration gracePeriod) {
        this.logger = logger;
        this.clock = clock;
        this.s3Client = s3Client;
        this.listing = listing;
        this.bucketName = bucketName;
        this.gracePeriod = gracePeriod;
        this.purgatory = new ExpandableArrayBuffer(1 << 12);

        this.closed = false;
        this.shutdownRequested = false;
        this.cycleFlippedLatch = new CountDownLatch(1);
        this.lastEventNanos = 0;

        LocalDateTime cycleStart = LocalDateTime.now(clock).truncatedTo(MarketDataEntry.CYCLE_CHRONO_UNIT);
        this.currentCycle =
                new CycleBuffer(new MarketDataEntry(this.listing, cycleStart, MarketDataEntry.EntryType.RAW));
        this.previousCycle = null;

        if (attachShutdownHook) {
            this.attachShutdownHook();
        }
    }

    public void onEvent(final Schema schema, long sequence, boolean endOfBatch) throws Exception {
        this.lastEventNanos = TimeUnit.MILLISECONDS.toNanos(this.clock.millis());
        if (closed) {
            return;
        }

        long epochSeconds = schema.getEventTimestamp() / 1_000_000_000L;
        long nanoAdjustment = schema.getEventTimestamp() % 1_000_000_000L;

        Instant instant = Instant.ofEpochSecond(epochSeconds, nanoAdjustment);
        LocalDateTime now = LocalDateTime.ofInstant(instant, clock.getZone());
        LocalDateTime eventCycleStart = now.truncatedTo(MarketDataEntry.CYCLE_CHRONO_UNIT);

        if (!now.minus(MarketDataEntry.CYCLE_CHRONO_UNIT.getDuration())
                .isBefore(this.currentCycle.entry.getTimestamp())) {
            // Force-upload any lingering previous cycle before demoting current
            if (this.previousCycle != null) {
                uploadCycle(this.previousCycle);
                this.previousCycle = null;
            }

            logger.logf(
                    LogMessage.DEBUG,
                    "Switching cycle to %s from %s",
                    eventCycleStart,
                    this.currentCycle.entry.getTimestamp());
            this.previousCycle = this.currentCycle;
            this.currentCycle =
                    new CycleBuffer(new MarketDataEntry(this.listing, eventCycleStart, MarketDataEntry.EntryType.RAW));

            if (shutdownRequested) {
                uploadCycle(this.previousCycle);
                this.previousCycle = null;
                cycleFlippedLatch.countDown();
                return;
            }
        }

        maybeExpireGracePeriod(now);

        try {
            schema.buffer.getBytes(0, this.purgatory, 0, schema.totalMessageSize());
            resolveTarget(eventCycleStart, now).write(this.purgatory.byteArray(), 0, schema.totalMessageSize());
        } catch (IOException e) {
            logger.logf(LogMessage.UNKNOWN_ERROR, "Error trying to write to file stream: %s", e.getMessage());
            throw new RuntimeException(e);
        }
    }

    private void maybeExpireGracePeriod(LocalDateTime now) throws IOException {
        if (this.previousCycle == null) {
            return;
        }
        if (!now.isBefore(this.currentCycle.entry.getTimestamp().plus(gracePeriod))) {
            uploadCycle(this.previousCycle);
            this.previousCycle = null;
        }
    }

    private ZstdOutputStream resolveTarget(LocalDateTime eventCycleStart, LocalDateTime now) {
        if (this.previousCycle == null) {
            return this.currentCycle.outputStream;
        }
        if (eventCycleStart.equals(this.previousCycle.entry.getTimestamp())) {
            return this.previousCycle.outputStream;
        }
        if (eventCycleStart.isBefore(this.previousCycle.entry.getTimestamp())) {
            logger.logf(
                    LogMessage.DEBUG,
                    "Event timestamp %s predates previous cycle %s, writing to current cycle",
                    now,
                    this.previousCycle.entry.getTimestamp());
        }
        return this.currentCycle.outputStream;
    }

    private void uploadCycle(CycleBuffer cycle) throws IOException {
        cycle.outputStream.close();

        if (cycle.rawBuffer.size() == 0) {
            logger.logf(LogMessage.DEBUG, "Skipping upload for empty buffer");
            return;
        }

        logger.logf(LogMessage.DEBUG, "Uploading file: %s", cycle.entry.getKey());
        cycle.entry.saveToS3(this.s3Client, this.bucketName, cycle.rawBuffer.toByteArray());
    }

    private void attachShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                this.waitForCycleAndClose();
            } catch (Exception e) {
                this.logger.logf(LogMessage.UNKNOWN_ERROR, "Error trying to cycle files: %s", e.getMessage());
            }
        }));
    }

    @VisibleForTesting
    void waitForCycleAndClose() throws IOException, InterruptedException {
        logger.logf(LogMessage.DEBUG, "Shutdown requested, waiting for cycle to flip before closing");
        shutdownRequested = true;
        cycleFlippedLatch.await();
        logger.logf(LogMessage.DEBUG, "Cycle flipped, proceeding with close");
        this.close();
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            logger.logf(LogMessage.DEBUG, "MarketDataCollector already closed, skipping");
            return;
        }

        closed = true;

        if (this.previousCycle != null) {
            this.previousCycle.rawBuffer.close();
            this.previousCycle.outputStream.close();
        }
        this.currentCycle.rawBuffer.close();
        this.currentCycle.outputStream.close();

        logger.logf(LogMessage.DEBUG, "MarketDataCollector closed successfully");
    }
}
