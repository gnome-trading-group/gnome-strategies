package group.gnometrading.collector;

import com.github.luben.zstd.ZstdOutputStream;
import com.lmax.disruptor.EventHandler;
import group.gnometrading.annotations.VisibleForTesting;
import group.gnometrading.logging.LogMessage;
import group.gnometrading.logging.Logger;
import group.gnometrading.schemas.Schema;
import group.gnometrading.sm.Listing;
import org.agrona.ExpandableArrayBuffer;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.concurrent.CountDownLatch;

public class MarketDataCollector implements EventHandler<Schema>, Closeable {

    private final Logger logger;
    private final Clock clock;
    private final S3Client s3Client;
    private final Listing listing;
    private final String bucketName;

    private MarketDataEntry entry;

    private final ExpandableArrayBuffer purgatory;
    private final ByteArrayOutputStream rawBuffer;
    private ZstdOutputStream outputStream;

    private volatile boolean closed;
    private volatile boolean shutdownRequested;
    private final CountDownLatch cycleFlippedLatch;

    public MarketDataCollector(
            Logger logger,
            Clock clock,
            S3Client s3Client,
            Listing listing,
            String bucketName
    ) {
        this(logger, clock, s3Client, listing, bucketName, true);
    }

    @VisibleForTesting
    MarketDataCollector(
            Logger logger,
            Clock clock,
            S3Client s3Client,
            Listing listing,
            String bucketName,
            boolean attachShutdownHook
    ) {
        this.logger = logger;
        this.clock = clock;
        this.s3Client = s3Client;
        this.listing = listing;
        this.bucketName = bucketName;
        this.purgatory = new ExpandableArrayBuffer(1 << 12);
        this.rawBuffer = new ByteArrayOutputStream();

        this.closed = false;
        this.shutdownRequested = false;
        this.cycleFlippedLatch = new CountDownLatch(1);

        LocalDateTime cycleStart = LocalDateTime.now(clock).truncatedTo(MarketDataEntry.CYCLE_CHRONO_UNIT);
        this.entry = new MarketDataEntry(this.listing, cycleStart, MarketDataEntry.EntryType.RAW);
        this.openNewStream();
        if (attachShutdownHook) {
            this.attachShutdownHook();
        }
    }

    public void onEvent(final Schema schema, long sequence, boolean endOfBatch) throws Exception {
        if (closed) {
            return;
        }

        long epochSeconds = schema.getEventTimestamp() / 1_000_000_000L;
        long nanoAdjustment = schema.getEventTimestamp() % 1_000_000_000L;

        Instant instant = Instant.ofEpochSecond(epochSeconds, nanoAdjustment);
        LocalDateTime now = LocalDateTime.ofInstant(instant, clock.getZone());

        if (!now.minus(MarketDataEntry.CYCLE_CHRONO_UNIT.getDuration()).isBefore(this.entry.getTimestamp())) {
            LocalDateTime nextCycleStart = now.truncatedTo(MarketDataEntry.CYCLE_CHRONO_UNIT);
            logger.logf(LogMessage.DEBUG, "Switching cycle to %s from %s", nextCycleStart, this.entry.getTimestamp());
            this.uploadFile();
            this.entry = new MarketDataEntry(this.listing, nextCycleStart, MarketDataEntry.EntryType.RAW);
            this.openNewStream();

            if (shutdownRequested) {
                cycleFlippedLatch.countDown();
                return;
            }
        }

        try {
            schema.buffer.getBytes(0, this.purgatory, 0, schema.totalMessageSize());
            outputStream.write(this.purgatory.byteArray(), 0, schema.totalMessageSize());
        } catch (IOException e) {
            logger.logf(LogMessage.UNKNOWN_ERROR, "Error trying to write to file stream: %s", e.getMessage());
            throw new RuntimeException(e);
        }
    }

    private void uploadFile() throws IOException {
        this.outputStream.close();

        if (rawBuffer.size() == 0) {
            logger.logf(LogMessage.DEBUG, "Skipping upload for empty buffer");
            return;
        }

        logger.logf(LogMessage.DEBUG, "Uploading file: %s", this.entry.getKey());
        this.entry.saveToS3(this.s3Client, this.bucketName, this.rawBuffer.toByteArray());
    }

    private void openNewStream() {
        logger.logf(LogMessage.DEBUG, "Opening new S3 file stream: %s", this.entry.getKey());

        try {
            this.rawBuffer.reset();
            this.outputStream = new ZstdOutputStream(this.rawBuffer);
        } catch (IOException e) {
            logger.logf(LogMessage.UNKNOWN_ERROR, "Error trying to create new file stream: %s", e.getMessage());
            throw new RuntimeException(e);
        }
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

        this.rawBuffer.close();
        this.outputStream.close();

        logger.logf(LogMessage.DEBUG, "MarketDataCollector closed successfully");
    }
}
