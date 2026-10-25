package group.gnometrading.collector;

import com.lmax.disruptor.EventHandler;
import group.gnometrading.annotations.VisibleForTesting;
import group.gnometrading.data.StreamEntry;
import group.gnometrading.logging.LogMessage;
import group.gnometrading.logging.Logger;
import group.gnometrading.schemas.RecordType;
import group.gnometrading.schemas.StreamRecord;
import java.io.Closeable;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.agrona.ExpandableArrayBuffer;

public final class MarketDataCollector implements EventHandler<StreamRecord>, Closeable {

    private final Logger logger;
    private final Clock clock;

    private final CycleManager cycleManager;
    private final ExpandableArrayBuffer purgatory;

    public volatile long lastEventNanos;

    public MarketDataCollector(
            Logger logger,
            Clock clock,
            CycleUploader uploader,
            int streamId,
            RecordType recordType,
            String bucketName) {
        this(logger, clock, uploader, streamId, recordType, bucketName, true, Duration.ofSeconds(5));
    }

    @VisibleForTesting
    MarketDataCollector(
            Logger logger,
            Clock clock,
            CycleUploader uploader,
            int streamId,
            RecordType recordType,
            String bucketName,
            boolean attachShutdownHook) {
        this(logger, clock, uploader, streamId, recordType, bucketName, attachShutdownHook, Duration.ofSeconds(5));
    }

    @VisibleForTesting
    MarketDataCollector(
            Logger logger,
            Clock clock,
            CycleUploader uploader,
            int streamId,
            RecordType recordType,
            String bucketName,
            boolean attachShutdownHook,
            Duration gracePeriod) {
        this.logger = logger;
        this.clock = clock;
        this.purgatory = new ExpandableArrayBuffer(1 << 12);
        this.lastEventNanos = 0;

        this.cycleManager = new CycleManager(
                logger,
                uploader,
                bucketName,
                ts -> new StreamEntry(streamId, recordType, ts, StreamEntry.EntryType.RAW).getKey(),
                clock.getZone(),
                StreamEntry.CYCLE_CHRONO_UNIT.getDuration(),
                CycleManager.epochNanos(clock.instant()),
                gracePeriod,
                attachShutdownHook);
    }

    public void onEvent(final StreamRecord schema, long sequence, boolean endOfBatch) throws Exception {
        this.lastEventNanos = TimeUnit.MILLISECONDS.toNanos(this.clock.millis());
        if (cycleManager.isClosed()) {
            return;
        }

        final long eventNanos = schema.getEventTimestamp();
        final long eventCycleStart = cycleManager.cycleStart(eventNanos);

        if (cycleManager.isPastCurrentCycle(eventNanos)) {
            if (cycleManager.rotate(eventCycleStart)) {
                return;
            }
        }

        cycleManager.maybeExpireGracePeriod(eventNanos);

        try {
            schema.buffer.getBytes(0, this.purgatory, 0, schema.totalMessageSize());
            cycleManager.resolveTarget(eventCycleStart).write(this.purgatory.byteArray(), 0, schema.totalMessageSize());
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
