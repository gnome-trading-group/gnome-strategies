package group.gnometrading.collector;

import com.github.luben.zstd.ZstdInputStream;
import group.gnometrading.logging.NullLogger;
import group.gnometrading.schemas.Schema;
import group.gnometrading.schemas.SchemaType;
import group.gnometrading.sm.Exchange;
import group.gnometrading.sm.Listing;
import group.gnometrading.sm.Security;
import org.agrona.MutableDirectBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.regex.Pattern;

import static java.time.ZoneOffset.UTC;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class MarketDataCollectorTest {

    private static final Listing LISTING = new Listing(532, new Exchange(151, "test-exchange", "test-region", SchemaType.MBO), new Security(499, "test-security", 1), "id", "id");
    private static final String OUTPUT_BUCKET = "test-bucket";
    private static final Pattern KEY_PATTERN = Pattern.compile("^499/151/\\d{4}/\\d{1,2}/\\d{1,2}/\\d{1,2}/\\d{1,2}/mbo/[a-f0-9]{8}\\.zst$");

    @Mock
    S3Client s3Client;

    @Mock
    Clock clock;

    @BeforeEach
    void setup() {
        doReturn(ZoneId.of("UTC")).when(clock).getZone();

        // Mock putObject to return success - capture the lambda and build the request
        lenient().when(s3Client.putObject(ArgumentMatchers.<Consumer<PutObjectRequest.Builder>>any(), any(RequestBody.class)))
                .thenAnswer(invocation -> {
                    Consumer<PutObjectRequest.Builder> requestBuilder = invocation.getArgument(0);
                    // The lambda will be called by the real implementation, but we just return success here
                    return PutObjectResponse.builder().build();
                });
    }

    @Test
    void testNoUploadWithinSameMinute() throws Exception {
        // Start at 12:00:00
        date(2025, 4, 1, 12, 0, 0);
        MarketDataCollector collector = new MarketDataCollector(new NullLogger(), clock, s3Client, LISTING, OUTPUT_BUCKET);

        // Add events with timestamps within the same minute (12:00:xx)
        // All these events have timestamps < minuteStart + 1 minute, so no rotation
        collector.onEvent(bufWithTimestamp("aaaaaaaa", 2025, 4, 1, 12, 0, 5), 0, false);
        collector.onEvent(bufWithTimestamp("bbbbbbbb", 2025, 4, 1, 12, 0, 15), 1, false);
        collector.onEvent(bufWithTimestamp("cccccccc", 2025, 4, 1, 12, 0, 30), 2, false);
        collector.onEvent(bufWithTimestamp("dddddddd", 2025, 4, 1, 12, 0, 59), 3, false);

        // No uploads should have occurred yet (all events in same minute)
        verify(s3Client, never()).putObject(ArgumentMatchers.<Consumer<PutObjectRequest.Builder>>any(), any(RequestBody.class));
    }

    @Test
    void testFileUploadEveryMinute() throws Exception {
        // Start at 12:00:00
        date(2025, 4, 1, 12, 0, 0);

        // Capture the lambdas to verify the keys
        ArgumentCaptor<Consumer<PutObjectRequest.Builder>> requestBuilderCaptor = ArgumentCaptor.forClass(Consumer.class);
        when(s3Client.putObject(requestBuilderCaptor.capture(), any(RequestBody.class)))
                .thenAnswer(invocation -> PutObjectResponse.builder().build());

        MarketDataCollector collector = new MarketDataCollector(new NullLogger(), clock, s3Client, LISTING, OUTPUT_BUCKET);

        // Add event with timestamp 12:00:30
        collector.onEvent(bufWithTimestamp("aaaaaaaa", 2025, 4, 1, 12, 0, 30), 0, false);

        // Add event with timestamp 12:01:01 - should trigger file upload
        // (12:01:01 - 1 min = 12:00:01 which is NOT before 12:00:00)
        collector.onEvent(bufWithTimestamp("bbbbbbbb", 2025, 4, 1, 12, 1, 1), 1, false);

        // Verify first file was uploaded
        verify(s3Client, times(1)).putObject(ArgumentMatchers.<Consumer<PutObjectRequest.Builder>>any(), any(RequestBody.class));

        // Add event with timestamp 12:02:01 - should trigger another file upload
        collector.onEvent(bufWithTimestamp("cccccccc", 2025, 4, 1, 12, 2, 1), 2, false);

        // Verify second file was uploaded
        verify(s3Client, times(2)).putObject(ArgumentMatchers.<Consumer<PutObjectRequest.Builder>>any(), any(RequestBody.class));

        // Verify keys have correct timestamps (minutes only, no seconds)
        List<Consumer<PutObjectRequest.Builder>> builders = requestBuilderCaptor.getAllValues();

        PutObjectRequest.Builder builder1 = PutObjectRequest.builder();
        builders.get(0).accept(builder1);
        PutObjectRequest request1 = builder1.build();

        PutObjectRequest.Builder builder2 = PutObjectRequest.builder();
        builders.get(1).accept(builder2);
        PutObjectRequest request2 = builder2.build();

        assertTrue(request1.key().contains("2025/4/1/12/0"), "First key should contain timestamp 2025/4/1/12/0");
        assertTrue(request2.key().contains("2025/4/1/12/1"), "Second key should contain timestamp 2025/4/1/12/1");
    }

    @Test
    void testExactMinuteBoundaryTriggersRotation() throws Exception {
        // Start at 12:00:00
        date(2025, 4, 1, 12, 0, 0);

        // Capture the lambda to verify the key
        ArgumentCaptor<Consumer<PutObjectRequest.Builder>> requestBuilderCaptor = ArgumentCaptor.forClass(Consumer.class);
        when(s3Client.putObject(requestBuilderCaptor.capture(), any(RequestBody.class)))
                .thenAnswer(invocation -> PutObjectResponse.builder().build());

        MarketDataCollector collector = new MarketDataCollector(new NullLogger(), clock, s3Client, LISTING, OUTPUT_BUCKET);

        // Add event with timestamp 12:00:30
        collector.onEvent(bufWithTimestamp("aaaaaaaa", 2025, 4, 1, 12, 0, 30), 0, false);

        // Add event with timestamp EXACTLY 12:01:00 (minuteStart + 1 minute)
        // This should trigger rotation because 12:01:00 - 1 min = 12:00:00 which is NOT before 12:00:00
        collector.onEvent(bufWithTimestamp("bbbbbbbb", 2025, 4, 1, 12, 1, 0), 1, false);

        // Verify file was uploaded
        verify(s3Client, times(1)).putObject(ArgumentMatchers.<Consumer<PutObjectRequest.Builder>>any(), any(RequestBody.class));

        // Verify the key is for the 12:00 minute
        PutObjectRequest.Builder builder = PutObjectRequest.builder();
        requestBuilderCaptor.getValue().accept(builder);
        PutObjectRequest request = builder.build();
        assertTrue(request.key().contains("2025/4/1/12/0"), "Key should be for 12:00 minute");
    }

    @Test
    void testOneSecondBeforeMinuteBoundaryDoesNotRotate() throws Exception {
        // Start at 12:00:00
        date(2025, 4, 1, 12, 0, 0);
        MarketDataCollector collector = new MarketDataCollector(new NullLogger(), clock, s3Client, LISTING, OUTPUT_BUCKET);

        // Add event with timestamp 12:00:30
        collector.onEvent(bufWithTimestamp("aaaaaaaa", 2025, 4, 1, 12, 0, 30), 0, false);

        // Add event with timestamp 12:00:59 (one second before minute boundary)
        // This should NOT trigger rotation because 12:00:59 - 1 min = 11:59:59 which IS before 12:00:00
        collector.onEvent(bufWithTimestamp("bbbbbbbb", 2025, 4, 1, 12, 0, 59), 1, false);

        // Verify no upload occurred
        verify(s3Client, never()).putObject(ArgumentMatchers.<Consumer<PutObjectRequest.Builder>>any(), any(RequestBody.class));
    }

    @Test
    void testCollectorStartsAtNonZeroSecond() throws Exception {
        // Start at 12:00:55 (55 seconds into the minute)
        date(2025, 4, 1, 12, 0, 55);
        MarketDataCollector collector = new MarketDataCollector(new NullLogger(), clock, s3Client, LISTING, OUTPUT_BUCKET);
        // minuteStart will be truncated to 12:00:00

        // Add event with timestamp 12:00:58
        collector.onEvent(bufWithTimestamp("aaaaaaaa", 2025, 4, 1, 12, 0, 58), 0, false);

        // Add event with timestamp 12:01:55 (exactly 1 minute after collector start wall clock time)
        // This should trigger rotation because 12:01:55 - 1 min = 12:00:55 which is NOT before 12:00:00
        collector.onEvent(bufWithTimestamp("bbbbbbbb", 2025, 4, 1, 12, 1, 55), 1, false);

        // Verify first file was uploaded
        verify(s3Client, times(1)).putObject(ArgumentMatchers.<Consumer<PutObjectRequest.Builder>>any(), any(RequestBody.class));
    }

    @Test
    void testEmptyBufferStillUploads() throws Exception {
        // Start at 12:00:00
        date(2025, 4, 1, 12, 0, 0);
        MarketDataCollector collector = new MarketDataCollector(new NullLogger(), clock, s3Client, LISTING, OUTPUT_BUCKET);

        // Add empty event with timestamp in next minute
        collector.onEvent(bufWithTimestamp("", 2025, 4, 1, 12, 1, 1), 0, false);

        // Upload should still occur (zstd stream has headers even with no data)
        verify(s3Client, times(1)).putObject(ArgumentMatchers.<Consumer<PutObjectRequest.Builder>>any(), any(RequestBody.class));
    }

    @Test
    void testKeyFormatAndStructure() throws Exception {
        // Start at 12:34:56
        date(2025, 4, 1, 12, 34, 56);

        // Capture the lambda and build the request to verify the key
        ArgumentCaptor<Consumer<PutObjectRequest.Builder>> requestBuilderCaptor = ArgumentCaptor.forClass(Consumer.class);
        when(s3Client.putObject(requestBuilderCaptor.capture(), any(RequestBody.class)))
                .thenAnswer(invocation -> PutObjectResponse.builder().build());

        MarketDataCollector collector = new MarketDataCollector(new NullLogger(), clock, s3Client, LISTING, OUTPUT_BUCKET);
        // minuteStart will be truncated to 12:34:00

        // Add data with timestamp 12:34:58
        collector.onEvent(bufWithTimestamp("testdata", 2025, 4, 1, 12, 34, 58), 0, false);

        // Add data with timestamp 12:35:01 to trigger upload
        collector.onEvent(bufWithTimestamp("moredata", 2025, 4, 1, 12, 35, 1), 1, false);

        // Build the request from the captured lambda
        PutObjectRequest.Builder builder = PutObjectRequest.builder();
        requestBuilderCaptor.getValue().accept(builder);
        PutObjectRequest request = builder.build();
        String key = request.key();

        // Verify key format: securityId/exchangeId/year/month/day/hour/minute/schemaType/uuid.zst
        assertTrue(KEY_PATTERN.matcher(key).matches(), "Key should match pattern: " + key);
        assertTrue(key.startsWith("499/151/"), "Key should start with securityId/exchangeId");
        assertTrue(key.contains("2025/4/1/12/34"), "Key should contain timestamp 2025/4/1/12/34");
        assertTrue(key.contains("/mbo/"), "Key should contain schema type");
        assertTrue(key.endsWith(".zst"), "Key should end with .zst");

        // Verify bucket
        assertEquals(OUTPUT_BUCKET, request.bucket());
    }

    @Test
    void testMultipleFileUploads() throws Exception {
        // Start at 12:00:00
        date(2025, 4, 1, 12, 0, 0);

        // Capture the lambdas to verify the keys
        ArgumentCaptor<Consumer<PutObjectRequest.Builder>> requestBuilderCaptor = ArgumentCaptor.forClass(Consumer.class);
        when(s3Client.putObject(requestBuilderCaptor.capture(), any(RequestBody.class)))
                .thenAnswer(invocation -> PutObjectResponse.builder().build());

        MarketDataCollector collector = new MarketDataCollector(new NullLogger(), clock, s3Client, LISTING, OUTPUT_BUCKET);

        // Add data with timestamps in first minute
        collector.onEvent(bufWithTimestamp("data0", 2025, 4, 1, 12, 0, 10), 0, false);
        collector.onEvent(bufWithTimestamp("data1", 2025, 4, 1, 12, 0, 20), 1, false);

        // Rotate to next minute with timestamp 12:01:01
        collector.onEvent(bufWithTimestamp("data2", 2025, 4, 1, 12, 1, 1), 2, false);

        // Add data with timestamp in second minute
        collector.onEvent(bufWithTimestamp("data3", 2025, 4, 1, 12, 1, 30), 3, false);

        // Rotate to third minute with timestamp 12:02:01
        collector.onEvent(bufWithTimestamp("data4", 2025, 4, 1, 12, 2, 1), 4, false);

        // Verify two files were uploaded
        verify(s3Client, times(2)).putObject(requestBuilderCaptor.capture(), any(RequestBody.class));

        List<Consumer<PutObjectRequest.Builder>> builders = requestBuilderCaptor.getAllValues();

        // Build requests from the captured lambdas
        PutObjectRequest.Builder builder1 = PutObjectRequest.builder();
        builders.get(0).accept(builder1);
        PutObjectRequest request1 = builder1.build();

        PutObjectRequest.Builder builder2 = PutObjectRequest.builder();
        builders.get(1).accept(builder2);
        PutObjectRequest request2 = builder2.build();

        // Verify different keys for different minutes
        String key1 = request1.key();
        String key2 = request2.key();
        assertNotEquals(key1, key2, "Keys should be different for different minutes");
        assertTrue(key1.contains("2025/4/1/12/0"), "First key should be for minute 12:00");
        assertTrue(key2.contains("2025/4/1/12/1"), "Second key should be for minute 12:01");
    }

    @Test
    void testOutOfOrderTimestampsDoNotTriggerRotation() throws Exception {
        // Start at 12:00:00
        date(2025, 4, 1, 12, 0, 0);
        MarketDataCollector collector = new MarketDataCollector(new NullLogger(), clock, s3Client, LISTING, OUTPUT_BUCKET);

        // Add event with timestamp 12:01:30 (future)
        collector.onEvent(bufWithTimestamp("future", 2025, 4, 1, 12, 1, 30), 0, false);

        // This should have triggered a rotation
        verify(s3Client, times(1)).putObject(ArgumentMatchers.<Consumer<PutObjectRequest.Builder>>any(), any(RequestBody.class));

        // Now add event with timestamp 12:00:30 (past - out of order)
        // This should NOT trigger another rotation because 12:00:30 - 1 min = 11:59:30 which IS before 12:01:00
        collector.onEvent(bufWithTimestamp("past", 2025, 4, 1, 12, 0, 30), 1, false);

        // Still only 1 upload
        verify(s3Client, times(1)).putObject(ArgumentMatchers.<Consumer<PutObjectRequest.Builder>>any(), any(RequestBody.class));
    }

    @Test
    void testDataCapturedAndCompressed() throws Exception {
        List<byte[]> capturedData = new ArrayList<>();

        when(s3Client.putObject(ArgumentMatchers.<Consumer<PutObjectRequest.Builder>>any(), any(RequestBody.class)))
                .thenAnswer(invocation -> {
                    RequestBody body = invocation.getArgument(1);
                    byte[] data = body.contentStreamProvider().newStream().readAllBytes();
                    capturedData.add(data);
                    return PutObjectResponse.builder().build();
                });

        date(2025, 4, 1, 12, 0, 0);
        MarketDataCollector collector = new MarketDataCollector(new NullLogger(), clock, s3Client, LISTING, OUTPUT_BUCKET);

        // Add data with timestamps
        collector.onEvent(bufWithTimestamp("aaaaaaaa", 2025, 4, 1, 12, 0, 10), 0, false);
        collector.onEvent(bufWithTimestamp("bbbbbbbb", 2025, 4, 1, 12, 0, 20), 1, false);
        collector.onEvent(bufWithTimestamp("cccccccc", 2025, 4, 1, 12, 0, 30), 2, false);

        // Trigger upload by rotating to next minute with timestamp 12:01:01
        collector.onEvent(bufWithTimestamp("dddddddd", 2025, 4, 1, 12, 1, 1), 3, false);

        // Verify we captured data
        assertEquals(1, capturedData.size());
        assertTrue(capturedData.get(0).length > 0, "Should have compressed data");

        // Decompress and verify content
        String decompressed = decompressZstd(capturedData.get(0));
        assertTrue(decompressed.contains("aaaaaaaa"), "Should contain first event data");
        assertTrue(decompressed.contains("bbbbbbbb"), "Should contain second event data");
        assertTrue(decompressed.contains("cccccccc"), "Should contain third event data");
    }

    @Test
    void testWaitForCycleAndCloseWaitsUntilCycleFlips() throws Exception {
        // Start at 12:00:00
        date(2025, 4, 1, 12, 0, 0);
        MarketDataCollector collector = new MarketDataCollector(new NullLogger(), clock, s3Client, LISTING, OUTPUT_BUCKET);

        // Add some data in the first minute
        collector.onEvent(bufWithTimestamp("data1", 2025, 4, 1, 12, 0, 10), 0, false);

        // Start waitForCycleAndClose in a separate thread - it should block until cycle flips
        Thread shutdownThread = new Thread(() -> {
            try {
                collector.waitForCycleAndClose();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        shutdownThread.start();

        Thread.sleep(50);

        assertTrue(shutdownThread.isAlive(), "Shutdown thread should still be waiting for cycle to flip");

        // Now send an event that triggers a cycle flip (timestamp in next minute)
        collector.onEvent(bufWithTimestamp("data2", 2025, 4, 1, 12, 1, 5), 1, false);

        shutdownThread.join(5000);
        assertFalse(shutdownThread.isAlive(), "Shutdown thread should have completed after cycle flip");

        verify(s3Client, times(1)).putObject(ArgumentMatchers.<Consumer<PutObjectRequest.Builder>>any(), any(RequestBody.class));
    }

    private Schema bufWithTimestamp(String input, int year, int month, int day, int hour, int minute, int second) {
        Instant instant = LocalDateTime.of(year, month, day, hour, minute, second).toInstant(UTC);
        long nanos = instant.getEpochSecond() * 1_000_000_000L + instant.getNano();
        return new DummySchema(input, nanos);
    }

    private String decompressZstd(byte[] compressed) {
        try (ZstdInputStream zstdStream = new ZstdInputStream(new ByteArrayInputStream(compressed))) {
            return new String(zstdStream.readAllBytes());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void date(int year, int month, int day, int hour, int minute, int second) {
        when(clock.instant()).thenReturn(LocalDateTime.of(year, month, day, hour, minute, second).toInstant(UTC));
    }

    private static class DummySchema extends Schema {
        private final long eventTimestamp;

        public DummySchema(String input, long eventTimestamp) {
            super(SchemaType.MBP_10);
            this.eventTimestamp = eventTimestamp;
            this.buffer.putBytes(0, input.getBytes()); // Buffer is size 8 from header padding
        }

        @Override
        protected int getEncodedBlockLength() {
            return 0;
        }

        @Override
        public void wrap(MutableDirectBuffer mutableDirectBuffer) {}

        @Override
        public long getSequenceNumber() {
            return 0;
        }

        @Override
        public long getEventTimestamp() {
            return eventTimestamp;
        }
    }

}