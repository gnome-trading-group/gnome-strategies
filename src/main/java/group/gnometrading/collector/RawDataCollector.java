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
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.UUID;
import software.amazon.awssdk.services.s3.S3Client;

public final class RawDataCollector implements Closeable {

    private static final int HEADER_SIZE = 12; // 8B recvTimestamp + 4B payloadLength

    private final Logger logger;
    private final Clock clock;
    private final CycleManager cycleManager;
    private final ByteBuffer headerBuffer = ByteBuffer.allocate(HEADER_SIZE).order(ByteOrder.LITTLE_ENDIAN);

    public RawDataCollector(Logger logger, Clock clock, S3Client s3Client, Listing listing, String bucketName) {
        this(logger, clock, s3Client, listing, bucketName, true);
    }

    @VisibleForTesting
    RawDataCollector(
            Logger logger,
            Clock clock,
            S3Client s3Client,
            Listing listing,
            String bucketName,
            boolean attachShutdownHook) {
        this.logger = logger;
        this.clock = clock;

        LocalDateTime initialMinute = LocalDateTime.now(clock).truncatedTo(ChronoUnit.MINUTES);
        this.cycleManager = new CycleManager(
                logger,
                s3Client,
                bucketName,
                ts -> buildS3Key(listing, ts),
                initialMinute,
                Duration.ZERO,
                attachShutdownHook);
    }

    public void capture(long recvTimestamp, ByteBuffer buffer) {
        if (cycleManager.isClosed() || !buffer.hasRemaining()) {
            return;
        }

        long epochSeconds = recvTimestamp / 1_000_000_000L;
        long nanoAdjustment = recvTimestamp % 1_000_000_000L;
        LocalDateTime now =
                LocalDateTime.ofInstant(Instant.ofEpochSecond(epochSeconds, nanoAdjustment), clock.getZone());
        LocalDateTime minuteStart = now.truncatedTo(ChronoUnit.MINUTES);

        if (!minuteStart.equals(cycleManager.currentTimestamp())) {
            try {
                if (cycleManager.rotate(minuteStart)) {
                    return;
                }
                cycleManager.maybeExpireGracePeriod(minuteStart);
            } catch (IOException e) {
                logger.logf(LogMessage.UNKNOWN_ERROR, "Error rotating raw capture: %s", e.getMessage());
                return;
            }
        }

        int payloadLength = buffer.remaining();
        headerBuffer.clear();
        headerBuffer.putLong(recvTimestamp);
        headerBuffer.putInt(payloadLength);

        try {
            cycleManager.currentOutputStream().write(headerBuffer.array());
            ByteBuffer payload = buffer.duplicate();
            byte[] bytes = new byte[payloadLength];
            payload.get(bytes);
            cycleManager.currentOutputStream().write(bytes);
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
