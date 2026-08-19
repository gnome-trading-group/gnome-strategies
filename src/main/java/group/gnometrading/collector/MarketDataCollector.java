package group.gnometrading.collector;

import com.lmax.disruptor.EventHandler;
import group.gnometrading.annotations.VisibleForTesting;
import group.gnometrading.data.MarketDataEntry;
import group.gnometrading.logging.LogMessage;
import group.gnometrading.logging.Logger;
import group.gnometrading.schemas.Schema;
import group.gnometrading.sm.Listing;
import java.io.Closeable;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.concurrent.TimeUnit;
import org.agrona.ExpandableArrayBuffer;
import software.amazon.awssdk.services.s3.S3Client;

public final class MarketDataCollector implements EventHandler<Schema>, Closeable {

    private final Logger logger;
    private final Clock clock;

    private final CycleManager cycleManager;
    private final ExpandableArrayBuffer purgatory;

    public volatile long lastEventNanos;

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
        this.purgatory = new ExpandableArrayBuffer(1 << 12);
        this.lastEventNanos = 0;

        LocalDateTime cycleStart = LocalDateTime.now(clock).truncatedTo(MarketDataEntry.CYCLE_CHRONO_UNIT);
        this.cycleManager = new CycleManager(
                logger,
                s3Client,
                bucketName,
                ts -> new MarketDataEntry(listing, ts, MarketDataEntry.EntryType.RAW).getKey(),
                cycleStart,
                gracePeriod,
                attachShutdownHook);
    }

    public void onEvent(final Schema schema, long sequence, boolean endOfBatch) throws Exception {
        this.lastEventNanos = TimeUnit.MILLISECONDS.toNanos(this.clock.millis());
        if (cycleManager.isClosed()) {
            return;
        }

        long epochSeconds = schema.getEventTimestamp() / 1_000_000_000L;
        long nanoAdjustment = schema.getEventTimestamp() % 1_000_000_000L;

        Instant instant = Instant.ofEpochSecond(epochSeconds, nanoAdjustment);
        LocalDateTime now = LocalDateTime.ofInstant(instant, clock.getZone());
        LocalDateTime eventCycleStart = now.truncatedTo(MarketDataEntry.CYCLE_CHRONO_UNIT);

        if (!now.minus(MarketDataEntry.CYCLE_CHRONO_UNIT.getDuration()).isBefore(cycleManager.currentTimestamp())) {
            if (cycleManager.rotate(eventCycleStart)) {
                return;
            }
        }

        cycleManager.maybeExpireGracePeriod(now);

        try {
            schema.buffer.getBytes(0, this.purgatory, 0, schema.totalMessageSize());
            cycleManager
                    .resolveTarget(eventCycleStart, now)
                    .write(this.purgatory.byteArray(), 0, schema.totalMessageSize());
        } catch (IOException e) {
            logger.logf(LogMessage.UNKNOWN_ERROR, "Error trying to write to file stream: %s", e.getMessage());
            throw new RuntimeException(e);
        }
    }

    @VisibleForTesting
    void waitForCycleAndClose() throws IOException, InterruptedException {
        cycleManager.waitForCycleAndClose();
    }

    @Override
    public void close() throws IOException {
        cycleManager.close();
    }
}
