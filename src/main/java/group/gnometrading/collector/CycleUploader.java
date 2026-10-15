package group.gnometrading.collector;

import group.gnometrading.annotations.VisibleForTesting;
import group.gnometrading.logging.LogMessage;
import group.gnometrading.logging.Logger;
import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.IntSupplier;
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
 *
 * <p>Every collector rotates at the same minute boundary, so the queue fills all at once. Uploading
 * that backlog back to back burns a full core of request signing and TLS for as long as it lasts,
 * which on a small CPU allocation starves the socket readers and delays their receive timestamps.
 * Uploads are therefore spaced out: a backlog that fits in {@link #DRAIN_WINDOW_NANOS} at {@link
 * #MAX_SPACING_NANOS} apart is spread across it, and a larger one is spaced more tightly.
 */
public final class CycleUploader implements Closeable {

    private static final long SHUTDOWN_TIMEOUT_SECONDS = 30;
    static final long DRAIN_WINDOW_NANOS = TimeUnit.SECONDS.toNanos(20);
    static final long MAX_SPACING_NANOS = TimeUnit.MILLISECONDS.toNanos(200);

    private final Logger logger;
    private final S3Client s3Client;
    private final Executor executor;
    private final ExecutorService ownedExecutor;
    private final IntSupplier queuedUploads;
    private final Sleeper sleeper;
    private volatile boolean closing;

    public CycleUploader(Logger logger, S3Client s3Client) {
        this(logger, s3Client, newUploaderExecutor());
    }

    private CycleUploader(Logger logger, S3Client s3Client, ThreadPoolExecutor executor) {
        this(logger, s3Client, executor, executor, () -> executor.getQueue().size(), LockSupport::parkNanos);
    }

    @VisibleForTesting
    CycleUploader(Logger logger, S3Client s3Client, Executor executor) {
        this(logger, s3Client, executor, null, () -> 0, nanos -> {});
    }

    @VisibleForTesting
    CycleUploader(Logger logger, S3Client s3Client, Executor executor, IntSupplier queuedUploads, Sleeper sleeper) {
        this(logger, s3Client, executor, null, queuedUploads, sleeper);
    }

    private CycleUploader(
            Logger logger,
            S3Client s3Client,
            Executor executor,
            ExecutorService ownedExecutor,
            IntSupplier queuedUploads,
            Sleeper sleeper) {
        this.logger = logger;
        this.s3Client = s3Client;
        this.executor = executor;
        this.ownedExecutor = ownedExecutor;
        this.queuedUploads = queuedUploads;
        this.sleeper = sleeper;
        if (ownedExecutor != null) {
            // Collectors' own shutdown hooks still hand over their last cycles after this runs; those
            // are rejected and uploaded inline, while everything already queued is drained here.
            Runtime.getRuntime().addShutdownHook(new Thread(this::close, "cycle-uploader-shutdown"));
        }
    }

    private static ThreadPoolExecutor newUploaderExecutor() {
        return new ThreadPoolExecutor(
                1, 1, 0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(), CycleUploader::newUploaderThread);
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
            executor.execute(() -> {
                uploadNow(cycle, bucketName);
                pace();
            });
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

    private void pace() {
        int queued = queuedUploads.getAsInt();
        if (queued == 0 || closing) {
            return;
        }
        sleeper.sleep(Math.min(MAX_SPACING_NANOS, DRAIN_WINDOW_NANOS / queued));
    }

    @Override
    public void close() {
        if (ownedExecutor == null) {
            return;
        }
        // The process is stopping, so finishing before the stop timeout matters more than the CPU.
        closing = true;
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

    @FunctionalInterface
    interface Sleeper {
        void sleep(long nanos);
    }
}
