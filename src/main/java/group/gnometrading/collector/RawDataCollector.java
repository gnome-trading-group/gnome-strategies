package group.gnometrading.collector;

import group.gnometrading.annotations.VisibleForTesting;
import group.gnometrading.logging.LogMessage;
import group.gnometrading.logging.Logger;
import group.gnometrading.sm.Listing;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Clock;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.UUID;

public final class RawDataCollector implements Closeable {

    private static final int HEADER_SIZE = 12; // 8B recvTimestamp + 4B payloadLength
    private static final int INITIAL_PAYLOAD_SCRATCH = 1 << 16;

    private final Logger logger;
    private final CycleManager cycleManager;
    private final ByteBuffer headerBuffer = ByteBuffer.allocate(HEADER_SIZE).order(ByteOrder.LITTLE_ENDIAN);
    // The socket buffer is direct, and the zstd stream only takes arrays; grown on the rare oversized message.
    private byte[] payloadScratch = new byte[INITIAL_PAYLOAD_SCRATCH];

    public RawDataCollector(Logger logger, Clock clock, CycleUploader uploader, Listing listing, String bucketName) {
        this(logger, clock, uploader, listing, bucketName, true);
    }

    @VisibleForTesting
    RawDataCollector(
            Logger logger,
            Clock clock,
            CycleUploader uploader,
            Listing listing,
            String bucketName,
            boolean attachShutdownHook) {
        this.logger = logger;

        this.cycleManager = new CycleManager(
                logger,
                uploader,
                bucketName,
                ts -> buildS3Key(listing, ts),
                clock.getZone(),
                Duration.ofMinutes(1),
                CycleManager.epochNanos(clock.instant()),
                Duration.ZERO,
                attachShutdownHook);
    }

    public void capture(long recvTimestamp, ByteBuffer buffer) {
        if (cycleManager.isClosed() || !buffer.hasRemaining()) {
            return;
        }

        if (cycleManager.isPastCurrentCycle(recvTimestamp)) {
            if (cycleManager.rotate(cycleManager.cycleStart(recvTimestamp))) {
                return;
            }
            cycleManager.maybeExpireGracePeriod(recvTimestamp);
        }

        int payloadLength = buffer.remaining();
        headerBuffer.clear();
        headerBuffer.putLong(recvTimestamp);
        headerBuffer.putInt(payloadLength);

        try {
            cycleManager.currentOutputStream().write(headerBuffer.array());
            if (payloadScratch.length < payloadLength) {
                payloadScratch = new byte[Math.max(payloadLength, payloadScratch.length * 2)];
            }
            // Absolute get: the gateway parses the message from this same position next.
            buffer.get(buffer.position(), payloadScratch, 0, payloadLength);
            cycleManager.currentOutputStream().write(payloadScratch, 0, payloadLength);
        } catch (IOException e) {
            logger.logf(LogMessage.UNKNOWN_ERROR, "Error writing raw capture: %s", e.getMessage());
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

    private static String buildS3Key(Listing listing, LocalDateTime timestamp) {
        return String.format(
                "%d/%d/%d/%d/%d/%d/%d/%s.zst",
                listing.security().securityId(),
                listing.exchange().exchangeId(),
                timestamp.getYear(),
                timestamp.getMonthValue(),
                timestamp.getDayOfMonth(),
                timestamp.getHour(),
                timestamp.getMinute(),
                UUID.randomUUID().toString());
    }
}
