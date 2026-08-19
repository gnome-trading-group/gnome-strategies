package group.gnometrading.collector;

import com.github.luben.zstd.ZstdOutputStream;
import group.gnometrading.logging.LogMessage;
import group.gnometrading.logging.Logger;
import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.concurrent.CountDownLatch;
import software.amazon.awssdk.services.s3.S3Client;

final class CycleManager implements Closeable {

    @FunctionalInterface
    interface KeyFactory {
        String createKey(LocalDateTime timestamp);
    }

    private final Logger logger;
    private final S3Client s3Client;
    private final String bucketName;
    private final Duration gracePeriod;
    private final KeyFactory keyFactory;

    private CycleBuffer currentCycle;
    private CycleBuffer previousCycle;

    private volatile boolean closed;
    private volatile boolean shutdownRequested;
    private final CountDownLatch cycleFlippedLatch;

    CycleManager(
            Logger logger,
            S3Client s3Client,
            String bucketName,
            KeyFactory keyFactory,
            LocalDateTime initialTimestamp,
            Duration gracePeriod,
            boolean attachShutdownHook) {
        this.logger = logger;
        this.s3Client = s3Client;
        this.bucketName = bucketName;
        this.gracePeriod = gracePeriod;
        this.keyFactory = keyFactory;
        this.closed = false;
        this.shutdownRequested = false;
        this.cycleFlippedLatch = new CountDownLatch(1);
        this.currentCycle = new CycleBuffer(initialTimestamp, keyFactory.createKey(initialTimestamp));
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

    boolean rotate(LocalDateTime newCycleStart) throws IOException {
        if (previousCycle != null) {
            previousCycle.upload(s3Client, bucketName, logger);
            previousCycle = null;
        }

        logger.logf(LogMessage.DEBUG, "Switching cycle to %s from %s", newCycleStart, currentCycle.timestamp());

        previousCycle = currentCycle;
        currentCycle = new CycleBuffer(newCycleStart, keyFactory.createKey(newCycleStart));

        if (shutdownRequested) {
            previousCycle.upload(s3Client, bucketName, logger);
            previousCycle = null;
            cycleFlippedLatch.countDown();
            return true;
        }

        return false;
    }

    void maybeExpireGracePeriod(LocalDateTime now) throws IOException {
        if (previousCycle == null) {
            return;
        }
        if (!now.isBefore(currentCycle.timestamp().plus(gracePeriod))) {
            previousCycle.upload(s3Client, bucketName, logger);
            previousCycle = null;
        }
    }

    ZstdOutputStream resolveTarget(LocalDateTime eventCycleStart, LocalDateTime now) {
        if (previousCycle == null) {
            return currentCycle.outputStream();
        }
        if (eventCycleStart.equals(previousCycle.timestamp())) {
            return previousCycle.outputStream();
        }
        if (eventCycleStart.isBefore(previousCycle.timestamp())) {
            logger.logf(
                    LogMessage.DEBUG,
                    "Event timestamp %s predates previous cycle %s, writing to current cycle",
                    now,
                    previousCycle.timestamp());
        }
        return currentCycle.outputStream();
    }

    ZstdOutputStream currentOutputStream() {
        return currentCycle.outputStream();
    }

    LocalDateTime currentTimestamp() {
        return currentCycle.timestamp();
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
