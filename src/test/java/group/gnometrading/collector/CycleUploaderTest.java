package group.gnometrading.collector;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import group.gnometrading.logging.NullLogger;
import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

@ExtendWith(MockitoExtension.class)
class CycleUploaderTest {

    private static final String BUCKET = "test-bucket";
    private static final LocalDateTime MINUTE_0 = LocalDateTime.of(2025, 4, 1, 12, 0);
    private static final LocalDateTime MINUTE_1 = LocalDateTime.of(2025, 4, 1, 12, 1);
    private static final LocalDateTime MINUTE_2 = LocalDateTime.of(2025, 4, 1, 12, 2);

    @Mock
    S3Client s3Client;

    private static CycleBuffer cycleWithData(String key) throws IOException {
        CycleBuffer cycle = new CycleBuffer(MINUTE_0, key);
        cycle.outputStream().write("data".getBytes());
        return cycle;
    }

    @Test
    void rotationHandsTheFinishedCycleToTheUploaderInsteadOfUploadingOnTheCaller() throws IOException {
        List<Runnable> queued = new ArrayList<>();
        CycleUploader uploader = new CycleUploader(new NullLogger(), s3Client, queued::add);
        CycleManager manager =
                new CycleManager(new NullLogger(), uploader, BUCKET, ts -> ts + ".zst", MINUTE_0, Duration.ZERO, false);

        manager.currentOutputStream().write("data".getBytes());
        manager.rotate(MINUTE_1);
        manager.rotate(MINUTE_2);

        verify(s3Client, never()).putObject(any(Consumer.class), any(RequestBody.class));
        assertEquals(1, queued.size());

        when(s3Client.putObject(any(Consumer.class), any(RequestBody.class)))
                .thenReturn(PutObjectResponse.builder().build());
        queued.get(0).run();
        verify(s3Client, times(1)).putObject(any(Consumer.class), any(RequestBody.class));
    }

    @Test
    void aSlowUploadDoesNotBlockTheCaller() throws Exception {
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch started = new CountDownLatch(1);
        when(s3Client.putObject(any(Consumer.class), any(RequestBody.class))).thenAnswer(invocation -> {
            started.countDown();
            release.await();
            return PutObjectResponse.builder().build();
        });
        CycleUploader uploader = new CycleUploader(new NullLogger(), s3Client);

        uploader.upload(cycleWithData("slow.zst"), BUCKET);

        assertTrue(started.await(5, TimeUnit.SECONDS), "Upload should run on the uploader thread");
        release.countDown();
        uploader.close();
        verify(s3Client, times(1)).putObject(any(Consumer.class), any(RequestBody.class));
    }

    @Test
    void closeDrainsQueuedUploads() throws Exception {
        when(s3Client.putObject(any(Consumer.class), any(RequestBody.class)))
                .thenReturn(PutObjectResponse.builder().build());
        CycleUploader uploader = new CycleUploader(new NullLogger(), s3Client);

        for (int i = 0; i < 5; i++) {
            uploader.upload(cycleWithData(i + ".zst"), BUCKET);
        }
        uploader.close();

        verify(s3Client, times(5)).putObject(any(Consumer.class), any(RequestBody.class));
    }

    @Test
    void uploadsInlineOnceTheUploaderHasShutDown() throws Exception {
        when(s3Client.putObject(any(Consumer.class), any(RequestBody.class)))
                .thenReturn(PutObjectResponse.builder().build());
        CycleUploader uploader = new CycleUploader(new NullLogger(), s3Client, task -> {
            throw new RejectedExecutionException();
        });

        uploader.upload(cycleWithData("late.zst"), BUCKET);

        verify(s3Client, times(1)).putObject(any(Consumer.class), any(RequestBody.class));
    }

    @Test
    void aFailedUploadIsLoggedNotThrown() throws Exception {
        when(s3Client.putObject(any(Consumer.class), any(RequestBody.class)))
                .thenThrow(new IllegalStateException("S3 down"));
        CycleUploader uploader = new CycleUploader(new NullLogger(), s3Client, Runnable::run);
        CycleBuffer cycle = cycleWithData("failed.zst");

        assertDoesNotThrow(() -> uploader.upload(cycle, BUCKET));
    }
}
