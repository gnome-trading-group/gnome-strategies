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
import java.util.concurrent.atomic.AtomicLong;
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
 *
 * <p>A failed upload is retried after {@link #RETRY_BACKOFF_NANOS}; one that still fails is dropped and counted. With
 * one copy of each stream a lost file is a coverage gap the inventory shows, so it isn't kept on disk. Every file's
 * buffers go back to the pool once it is done with, uploaded or not.
 *
 * <p>The owner closes it last when the process stops, after every collector has flushed.
 */
public final class CycleUploader implements Closeable {

    private static final long SHUTDOWN_TIMEOUT_SECONDS = 45;
    static final long[] RETRY_BACKOFF_NANOS = {
        TimeUnit.SECONDS.toNanos(1), TimeUnit.SECONDS.toNanos(5), TimeUnit.SECONDS.toNanos(30)
    };
    // Stopping: the stop timeout leaves no room for long waits, so retries come quickly.
    static final long CLOSING_BACKOFF_NANOS = TimeUnit.SECONDS.toNanos(1);
    static final long DRAIN_WINDOW_NANOS = TimeUnit.SECONDS.toNanos(20);
    static final long MAX_SPACING_NANOS = TimeUnit.MILLISECONDS.toNanos(200);

    private final Logger logger;
    private final S3Client s3Client;
    private final Executor executor;
    private final ExecutorService ownedExecutor;
    private final IntSupplier queuedUploads;
    private final Sleeper sleeper;
    private volatile boolean closing;
    private final AtomicLong failedUploads = new AtomicLong();
    // When the upload in progress started, retries included; 0 while none is. Lets a health check tell an uploader
    // that is working through a backlog from one stuck on a single file.
    private volatile long uploadStartedNanos;

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
        uploadStartedNanos = System.nanoTime();
        try {
            for (int attempt = 0; ; attempt++) {
                try {
                    cycle.upload(s3Client, bucketName, logger);
                    return;
                } catch (IOException | RuntimeException e) {
                    if (attempt == RETRY_BACKOFF_NANOS.length) {
                        failedUploads.incrementAndGet();
                        logger.logf(
                                LogMessage.UNKNOWN_ERROR,
                                "Dropped %s after %d failed uploads to %s: %s",
                                cycle.s3Key(),
                                attempt + 1,
                                bucketName,
                                e);
                        return;
                    }
                    logger.logf(
                            LogMessage.UNKNOWN_ERROR,
                            "Upload %d of %s to %s failed, retrying: %s",
                            attempt + 1,
                            cycle.s3Key(),
                            bucketName,
                            e);
                    sleeper.sleep(closing ? CLOSING_BACKOFF_NANOS : RETRY_BACKOFF_NANOS[attempt]);
                }
            }
        } finally {
            uploadStartedNanos = 0;
            cycle.release();
        }
    }

    /** Files dropped after every retry failed, since the process started. */
    public long failedUploads() {
        return failedUploads.get();
    }

    /** Whether one upload has been in progress, retries included, for longer than {@code maxNanos}. */
    public boolean isStuck(final long nowNanos, final long maxNanos) {
        final long started = uploadStartedNanos;
        return started != 0 && nowNanos - started > maxNanos;
    }

    /** Files waiting for the uploader thread. */
    public int queuedUploads() {
        return queuedUploads.getAsInt();
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
