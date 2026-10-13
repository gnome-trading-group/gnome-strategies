package group.gnometrading.collector;

import group.gnometrading.annotations.VisibleForTesting;
import group.gnometrading.logging.LogMessage;
import group.gnometrading.logging.Logger;
import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * Uploads finished cycle files to S3 off the threads that fill them.
 *
 * <p>Cycles rotate on the thread that receives data: the collector agent for market data, and each
 * listing's socket reader for raw capture. A synchronous S3 put there blocks that thread for the
 * whole request, so every message behind it is read late and gets a late receive timestamp.
 *
 * <p>One thread serves every collector. It blocks on its queue while idle, so it costs no CPU
 * between the once-a-minute rotations, unlike a polling agent on a small CPU quota.
 */
public final class CycleUploader implements Closeable {

    private static final long SHUTDOWN_TIMEOUT_SECONDS = 30;

    private final Logger logger;
    private final S3Client s3Client;
    private final Executor executor;
    private final ExecutorService ownedExecutor;

    public CycleUploader(Logger logger, S3Client s3Client) {
        this(logger, s3Client, Executors.newSingleThreadExecutor(CycleUploader::newUploaderThread), true);
    }

    @VisibleForTesting
    CycleUploader(Logger logger, S3Client s3Client, Executor executor) {
        this(logger, s3Client, executor, false);
    }

    private CycleUploader(Logger logger, S3Client s3Client, Executor executor, boolean ownsExecutor) {
        this.logger = logger;
        this.s3Client = s3Client;
        this.executor = executor;
        this.ownedExecutor = ownsExecutor ? (ExecutorService) executor : null;
        if (ownsExecutor) {
            // Collectors' own shutdown hooks still hand over their last cycles after this runs; those
            // are rejected and uploaded inline, while everything already queued is drained here.
            Runtime.getRuntime().addShutdownHook(new Thread(this::close, "cycle-uploader-shutdown"));
        }
    }

    private static Thread newUploaderThread(Runnable runnable) {
        Thread thread = new Thread(runnable, "cycle-uploader");
        thread.setDaemon(true);
        return thread;
    }

    /**
     * Uploads {@code cycle} in the background. The caller must not write to it afterwards.
     */
    void upload(CycleBuffer cycle, String bucketName) {
        try {
            executor.execute(() -> uploadNow(cycle, bucketName));
        } catch (RejectedExecutionException e) {
            // Shutting down: nothing will drain the queue any more, so upload on the caller's thread.
            uploadNow(cycle, bucketName);
        }
    }

    void uploadNow(CycleBuffer cycle, String bucketName) {
        try {
            cycle.upload(s3Client, bucketName, logger);
        } catch (IOException | RuntimeException e) {
            logger.logf(
                    LogMessage.UNKNOWN_ERROR,
                    "Failed to upload %s to %s: %s",
                    cycle.s3Key(),
                    bucketName,
                    e.getMessage());
        }
    }

    @Override
    public void close() {
        if (ownedExecutor == null) {
            return;
        }
        ownedExecutor.shutdown();
        try {
            if (!ownedExecutor.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                logger.logf(
                        LogMessage.UNKNOWN_ERROR,
                        "Cycle uploads still pending after %d seconds; they are lost",
                        SHUTDOWN_TIMEOUT_SECONDS);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
