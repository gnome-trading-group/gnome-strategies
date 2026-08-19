package group.gnometrading.collector;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.github.luben.zstd.ZstdInputStream;
import group.gnometrading.logging.NullLogger;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

@ExtendWith(MockitoExtension.class)
class CycleManagerTest {

    private static final String BUCKET = "test-bucket";
    private static final LocalDateTime MINUTE_0 = LocalDateTime.of(2025, 4, 1, 12, 0);
    private static final LocalDateTime MINUTE_1 = LocalDateTime.of(2025, 4, 1, 12, 1);
    private static final LocalDateTime MINUTE_2 = LocalDateTime.of(2025, 4, 1, 12, 2);

    @Mock
    S3Client s3Client;

    @BeforeEach
    void setup() {
        lenient()
                .when(s3Client.putObject(any(Consumer.class), any(RequestBody.class)))
                .thenReturn(PutObjectResponse.builder().build());
    }

    private CycleManager manager(Duration gracePeriod) {
        return new CycleManager(new NullLogger(), s3Client, BUCKET, ts -> ts + ".zst", MINUTE_0, gracePeriod, false);
    }

    @Test
    void testRotateReturnsFalseNormally() throws IOException {
        CycleManager manager = manager(Duration.ZERO);
        assertFalse(manager.rotate(MINUTE_1));
    }

    @Test
    void testDoubleRotationForceUploadsPrevious() throws IOException {
        CycleManager manager = manager(Duration.ofMinutes(5));

        manager.rotate(MINUTE_1);
        verify(s3Client, never()).putObject(any(Consumer.class), any(RequestBody.class));

        manager.rotate(MINUTE_2);
        verify(s3Client, times(1)).putObject(any(Consumer.class), any(RequestBody.class));
    }

    @Test
    void testGracePeriodPreventsImmediateUpload() throws IOException {
        CycleManager manager = manager(Duration.ofSeconds(10));

        manager.rotate(MINUTE_1);
        manager.maybeExpireGracePeriod(MINUTE_1.plusSeconds(5));

        verify(s3Client, never()).putObject(any(Consumer.class), any(RequestBody.class));
    }

    @Test
    void testGracePeriodExpiryTriggersUpload() throws IOException {
        CycleManager manager = manager(Duration.ofSeconds(10));

        manager.rotate(MINUTE_1);
        manager.maybeExpireGracePeriod(MINUTE_1.plusSeconds(10));

        verify(s3Client, times(1)).putObject(any(Consumer.class), any(RequestBody.class));
    }

    @Test
    void testResolveTargetRoutesToPreviousBuffer() throws IOException, Exception {
        List<byte[]> uploaded = new ArrayList<>();
        when(s3Client.putObject(any(Consumer.class), any(RequestBody.class))).thenAnswer(invocation -> {
            RequestBody body = invocation.getArgument(1);
            uploaded.add(body.contentStreamProvider().newStream().readAllBytes());
            return PutObjectResponse.builder().build();
        });

        CycleManager manager = manager(Duration.ofSeconds(30));
        manager.currentOutputStream().write("minute0".getBytes());

        manager.rotate(MINUTE_1);
        manager.resolveTarget(MINUTE_0, MINUTE_0.plusSeconds(55)).write("late".getBytes());

        manager.rotate(MINUTE_2);

        assertEquals(1, uploaded.size());
        try (ZstdInputStream zstd = new ZstdInputStream(new ByteArrayInputStream(uploaded.get(0)))) {
            String content = new String(zstd.readAllBytes());
            assertTrue(content.contains("minute0"));
            assertTrue(content.contains("late"));
        }
    }

    @Test
    void testResolveTargetFallsBackToCurrentWhenNoPrevious() throws IOException {
        List<byte[]> uploaded = new ArrayList<>();
        when(s3Client.putObject(any(Consumer.class), any(RequestBody.class))).thenAnswer(invocation -> {
            RequestBody body = invocation.getArgument(1);
            uploaded.add(body.contentStreamProvider().newStream().readAllBytes());
            return PutObjectResponse.builder().build();
        });

        CycleManager manager = manager(Duration.ZERO);
        manager.resolveTarget(MINUTE_0, MINUTE_0.plusSeconds(30)).write("data".getBytes());

        manager.rotate(MINUTE_1);
        manager.maybeExpireGracePeriod(MINUTE_1);

        assertEquals(1, uploaded.size());
        try (ZstdInputStream zstd = new ZstdInputStream(new ByteArrayInputStream(uploaded.get(0)))) {
            assertTrue(new String(zstd.readAllBytes()).contains("data"));
        }
    }

    @Test
    void testWaitForCycleAndCloseBlocksUntilRotation() throws Exception {
        CycleManager manager = manager(Duration.ZERO);
        manager.currentOutputStream().write("data".getBytes());

        Thread shutdownThread = new Thread(() -> {
            try {
                manager.waitForCycleAndClose();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        shutdownThread.start();
        Thread.sleep(50);

        assertTrue(shutdownThread.isAlive(), "Should block until rotation");

        manager.rotate(MINUTE_1);

        shutdownThread.join(5000);
        assertFalse(shutdownThread.isAlive(), "Should complete after rotation");
        verify(s3Client, times(1)).putObject(any(Consumer.class), any(RequestBody.class));
    }

    @Test
    void testShutdownUploadsBothPreviousAndCurrentCycles() throws Exception {
        ArgumentCaptor<Consumer<PutObjectRequest.Builder>> keyCaptor = ArgumentCaptor.forClass(Consumer.class);
        when(s3Client.putObject(keyCaptor.capture(), any(RequestBody.class)))
                .thenReturn(PutObjectResponse.builder().build());

        CycleManager manager = manager(Duration.ofMinutes(5));
        manager.currentOutputStream().write("min0".getBytes());
        manager.rotate(MINUTE_1);
        manager.currentOutputStream().write("min1".getBytes());

        Thread shutdownThread = new Thread(() -> {
            try {
                manager.waitForCycleAndClose();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        shutdownThread.start();
        Thread.sleep(50);

        manager.rotate(MINUTE_2);

        shutdownThread.join(5000);
        assertFalse(shutdownThread.isAlive());

        verify(s3Client, times(2)).putObject(any(Consumer.class), any(RequestBody.class));

        List<Consumer<PutObjectRequest.Builder>> keys = keyCaptor.getAllValues();
        PutObjectRequest.Builder b1 = PutObjectRequest.builder();
        keys.get(0).accept(b1);
        PutObjectRequest.Builder b2 = PutObjectRequest.builder();
        keys.get(1).accept(b2);

        String key1 = b1.build().key();
        String key2 = b2.build().key();
        assertTrue(key1.contains("12") && key2.contains("12"));
        assertNotEquals(key1, key2);
    }

    @Test
    void testCurrentTimestampReflectsActiveBuffer() throws IOException {
        CycleManager manager = manager(Duration.ZERO);
        assertEquals(MINUTE_0, manager.currentTimestamp());

        manager.rotate(MINUTE_1);
        assertEquals(MINUTE_1, manager.currentTimestamp());
    }

    @Test
    void testIsClosedAfterClose() throws IOException {
        CycleManager manager = manager(Duration.ZERO);
        assertFalse(manager.isClosed());
        manager.close();
        assertTrue(manager.isClosed());
    }
}
