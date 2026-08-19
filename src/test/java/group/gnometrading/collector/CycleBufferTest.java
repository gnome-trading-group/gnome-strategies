package group.gnometrading.collector;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.github.luben.zstd.ZstdInputStream;
import group.gnometrading.logging.NullLogger;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.function.Consumer;
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
class CycleBufferTest {

    private static final String BUCKET = "test-bucket";
    private static final String KEY = "test/key.zst";
    private static final LocalDateTime TIMESTAMP = LocalDateTime.of(2025, 4, 1, 12, 0);

    @Mock
    S3Client s3Client;

    @Test
    void testUploadCallsS3WithCorrectKeyAndBucket() throws IOException {
        when(s3Client.putObject(any(Consumer.class), any(RequestBody.class)))
                .thenReturn(PutObjectResponse.builder().build());

        CycleBuffer buffer = new CycleBuffer(TIMESTAMP, KEY);
        buffer.outputStream().write("data".getBytes());
        buffer.upload(s3Client, BUCKET, new NullLogger());

        ArgumentCaptor<Consumer<PutObjectRequest.Builder>> requestCaptor = ArgumentCaptor.forClass(Consumer.class);
        verify(s3Client).putObject(requestCaptor.capture(), any(RequestBody.class));

        PutObjectRequest.Builder builder = PutObjectRequest.builder();
        requestCaptor.getValue().accept(builder);
        PutObjectRequest request = builder.build();
        assertEquals(KEY, request.key());
        assertEquals(BUCKET, request.bucket());
    }

    @Test
    void testUploadCompressesDataCorrectly() throws IOException {
        when(s3Client.putObject(any(Consumer.class), any(RequestBody.class)))
                .thenReturn(PutObjectResponse.builder().build());

        CycleBuffer buffer = new CycleBuffer(TIMESTAMP, KEY);
        buffer.outputStream().write("hello world".getBytes());
        buffer.upload(s3Client, BUCKET, new NullLogger());

        ArgumentCaptor<RequestBody> bodyCaptor = ArgumentCaptor.forClass(RequestBody.class);
        verify(s3Client).putObject(any(Consumer.class), bodyCaptor.capture());

        byte[] compressed =
                bodyCaptor.getValue().contentStreamProvider().newStream().readAllBytes();
        try (ZstdInputStream zstd = new ZstdInputStream(new ByteArrayInputStream(compressed))) {
            assertEquals("hello world", new String(zstd.readAllBytes()));
        }
    }

    @Test
    void testTimestampReturned() {
        CycleBuffer buffer = new CycleBuffer(TIMESTAMP, KEY);
        assertEquals(TIMESTAMP, buffer.timestamp());
    }

    @Test
    void testCompressedSizePositiveAfterUpload() throws IOException {
        when(s3Client.putObject(any(Consumer.class), any(RequestBody.class)))
                .thenReturn(PutObjectResponse.builder().build());

        CycleBuffer buffer = new CycleBuffer(TIMESTAMP, KEY);
        buffer.outputStream().write("some data".getBytes());
        buffer.upload(s3Client, BUCKET, new NullLogger());

        assertTrue(buffer.compressedSize() > 0);
    }
}
