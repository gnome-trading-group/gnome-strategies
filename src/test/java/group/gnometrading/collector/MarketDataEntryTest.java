package group.gnometrading.collector;

import com.github.luben.zstd.ZstdInputStream;
import com.github.luben.zstd.ZstdOutputStream;
import group.gnometrading.schemas.MBP10Schema;
import group.gnometrading.schemas.Schema;
import group.gnometrading.schemas.SchemaType;
import group.gnometrading.sm.Listing;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.pagination.sync.SdkIterable;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDateTime;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class MarketDataEntryTest {

    @Mock
    private S3Client s3Client;

    @Mock
    private ListObjectsV2Iterable listObjectsIterable;

    /**
     * Helper to create a fake Listing with specific parameters.
     * Makes it easy to create test data with readable syntax.
     */
    private Listing listing(int securityId, int exchangeId, SchemaType schemaType) {
        return new Listing(securityId, exchangeId, securityId, "test-symbol", "test-name", schemaType);
    }

    /**
     * Helper to create a MarketDataEntry with specific parameters.
     */
    private MarketDataEntry entry(int securityId, int exchangeId, LocalDateTime timestamp,
                                   MarketDataEntry.EntryType type, SchemaType schemaType) {
        return new MarketDataEntry(listing(securityId, exchangeId, schemaType), timestamp, type);
    }

    /**
     * Helper to create a MarketDataEntry with a specific UUID.
     */
    private MarketDataEntry entryWithUuid(int securityId, int exchangeId, LocalDateTime timestamp,
                                          MarketDataEntry.EntryType type, SchemaType schemaType, String uuid) {
        return new MarketDataEntry(listing(securityId, exchangeId, schemaType), timestamp, type, uuid);
    }

    // ============================================================================
    // KEY GENERATION TESTS (Parameterized)
    // ============================================================================

    static Stream<Arguments> keyGenerationTestCases() {
        return Stream.of(
            // Aggregated entries
            Arguments.of(532, 151, 2025, 4, 15, 14, 30, SchemaType.MBP_10, MarketDataEntry.EntryType.AGGREGATED, null,
                "532/151/2025/4/15/14/30/mbp-10.zst"),
            Arguments.of(999, 888, 2024, 12, 25, 23, 59, SchemaType.MBO, MarketDataEntry.EntryType.AGGREGATED, null,
                "999/888/2024/12/25/23/59/mbo.zst"),
            Arguments.of(1, 1, 2025, 1, 1, 12, 0, SchemaType.OHLCV_1H, MarketDataEntry.EntryType.AGGREGATED, null,
                "1/1/2025/1/1/12/0/ohlcv-1h.zst"),
            // Raw entries with UUID
            Arguments.of(532, 151, 2025, 4, 15, 14, 30, SchemaType.MBP_10, MarketDataEntry.EntryType.RAW, "abc12345",
                "532/151/2025/4/15/14/30/mbp-10/abc12345.zst"),
            Arguments.of(999999, 888888, 2025, 4, 15, 14, 30, SchemaType.MBP_10, MarketDataEntry.EntryType.RAW, "xyz98765",
                "999999/888888/2025/4/15/14/30/mbp-10/xyz98765.zst")
        );
    }

    @ParameterizedTest
    @MethodSource("keyGenerationTestCases")
    void testGetKeyGeneration(int securityId, int exchangeId, int year, int month, int day, int hour, int minute,
                               SchemaType schemaType, MarketDataEntry.EntryType entryType, String uuid, String expectedKey) {
        LocalDateTime timestamp = LocalDateTime.of(year, month, day, hour, minute);
        MarketDataEntry entry = uuid != null
            ? entryWithUuid(securityId, exchangeId, timestamp, entryType, schemaType, uuid)
            : entry(securityId, exchangeId, timestamp, entryType, schemaType);

        assertEquals(expectedKey, entry.getKey());
    }

    // ============================================================================
    // KEY PARSING TESTS (Parameterized)
    // ============================================================================

    static Stream<Arguments> keyParsingTestCases() {
        return Stream.of(
            // Aggregated keys
            Arguments.of("532/151/2025/4/15/14/30/mbp-10.zst", 532, 151, 2025, 4, 15, 14, 30,
                MarketDataEntry.EntryType.AGGREGATED, null),
            Arguments.of("999/888/2024/12/25/23/59/mbo.zst", 999, 888, 2024, 12, 25, 23, 59,
                MarketDataEntry.EntryType.AGGREGATED, null),
            // Edge case timestamps
            Arguments.of("532/151/2025/1/1/0/0/mbp-10.zst", 532, 151, 2025, 1, 1, 0, 0,
                MarketDataEntry.EntryType.AGGREGATED, null),
            Arguments.of("532/151/2025/4/15/23/59/mbp-10.zst", 532, 151, 2025, 4, 15, 23, 59,
                MarketDataEntry.EntryType.AGGREGATED, null),
            Arguments.of("532/151/2025/1/5/14/30/mbp-10.zst", 532, 151, 2025, 1, 5, 14, 30,
                MarketDataEntry.EntryType.AGGREGATED, null),
            // Raw keys with UUID
            Arguments.of("532/151/2025/4/15/14/30/mbp-10/abc12345.zst", 532, 151, 2025, 4, 15, 14, 30,
                MarketDataEntry.EntryType.RAW, "abc12345"),
            Arguments.of("532/151/2025/4/15/14/30/mbp-10/12345678.zst", 532, 151, 2025, 4, 15, 14, 30,
                MarketDataEntry.EntryType.RAW, "12345678"),
            // Large IDs
            Arguments.of("999999/888888/2025/4/15/14/30/mbp-10.zst", 999999, 888888, 2025, 4, 15, 14, 30,
                MarketDataEntry.EntryType.AGGREGATED, null)
        );
    }

    @ParameterizedTest
    @MethodSource("keyParsingTestCases")
    void testFromKeyParsing(String key, int securityId, int exchangeId, int year, int month, int day,
                            int hour, int minute, MarketDataEntry.EntryType expectedType, String expectedUuid) {
        MarketDataEntry entry = MarketDataEntry.fromKey(key);

        assertEquals(securityId, entry.getSecurityId());
        assertEquals(exchangeId, entry.getExchangeId());
        assertEquals(year, entry.getTimestamp().getYear());
        assertEquals(month, entry.getTimestamp().getMonthValue());
        assertEquals(day, entry.getTimestamp().getDayOfMonth());
        assertEquals(hour, entry.getTimestamp().getHour());
        assertEquals(minute, entry.getTimestamp().getMinute());
        assertEquals(expectedType, entry.getEntryType());
        if (expectedUuid != null) {
            assertEquals(expectedUuid, entry.getUUID());
        }
    }

    @Test
    void testFromKeyInvalidKeyThrowsException() {
        String invalidKey = "invalid/key/format";
        assertThrows(IllegalArgumentException.class, () -> MarketDataEntry.fromKey(invalidKey));
    }

    // ============================================================================
    // ROUND-TRIP TESTS (Parameterized)
    // ============================================================================

    static Stream<Arguments> roundTripTestCases() {
        return Stream.of(
            Arguments.of(999, 888, 2025, 6, 20, 9, 45, SchemaType.MBO, MarketDataEntry.EntryType.AGGREGATED, null),
            Arguments.of(532, 151, 2025, 4, 15, 14, 30, SchemaType.MBP_10, MarketDataEntry.EntryType.AGGREGATED, null),
            Arguments.of(999, 888, 2025, 6, 20, 9, 45, SchemaType.MBO, MarketDataEntry.EntryType.RAW, "xyz98765"),
            Arguments.of(532, 151, 2025, 4, 15, 14, 30, SchemaType.MBP_10, MarketDataEntry.EntryType.RAW, "abc12345")
        );
    }

    @ParameterizedTest
    @MethodSource("roundTripTestCases")
    void testFromKeyRoundTrip(int securityId, int exchangeId, int year, int month, int day, int hour, int minute,
                               SchemaType schemaType, MarketDataEntry.EntryType entryType, String uuid) {
        LocalDateTime timestamp = LocalDateTime.of(year, month, day, hour, minute);
        MarketDataEntry original = uuid != null
            ? entryWithUuid(securityId, exchangeId, timestamp, entryType, schemaType, uuid)
            : entry(securityId, exchangeId, timestamp, entryType, schemaType);

        String key = original.getKey();
        MarketDataEntry parsed = MarketDataEntry.fromKey(key);

        assertEquals(original.getSecurityId(), parsed.getSecurityId());
        assertEquals(original.getExchangeId(), parsed.getExchangeId());
        assertEquals(original.getSchemaType(), parsed.getSchemaType());
        assertEquals(original.getTimestamp(), parsed.getTimestamp());
        assertEquals(original.getEntryType(), parsed.getEntryType());
        if (uuid != null) {
            assertEquals(original.getUUID(), parsed.getUUID());
        }
    }

    // ============================================================================
    // FROM KEY SCHEMA TYPE PARSING TESTS
    // ============================================================================

    @Test
    void testFromKeyParsesSchemaTypeFromAggregatedKey() {
        // Given: An aggregated key with mbp-10 schema
        String key = "532/151/2025/4/15/14/30/mbp-10.zst";

        // When: Parsing the key
        MarketDataEntry entry = MarketDataEntry.fromKey(key);

        // Then: Schema type is correctly parsed
        assertEquals(SchemaType.MBP_10, entry.getSchemaType());
        assertEquals(532, entry.getSecurityId());
        assertEquals(151, entry.getExchangeId());
    }

    @Test
    void testFromKeyParsesSchemaTypeFromRawKey() {
        // Given: A raw key with mbp-10 schema
        String key = "532/151/2025/4/15/14/30/mbp-10/abc12345.zst";

        // When: Parsing the raw key
        MarketDataEntry entry = MarketDataEntry.fromKey(key);

        // Then: Entry is correctly parsed with UUID and schema type
        assertEquals(MarketDataEntry.EntryType.RAW, entry.getEntryType());
        assertEquals(SchemaType.MBP_10, entry.getSchemaType());
        assertEquals("abc12345", entry.getUUID());
        assertEquals(2025, entry.getTimestamp().getYear());
        assertEquals(4, entry.getTimestamp().getMonthValue());
        assertEquals(15, entry.getTimestamp().getDayOfMonth());
    }

    // ============================================================================
    // SAVE TO S3 TESTS
    // ============================================================================

    @Test
    void testSaveToS3WithByteArray() {
        LocalDateTime timestamp = LocalDateTime.of(2025, 4, 15, 14, 30);
        MarketDataEntry entry = entry(532, 151, timestamp, MarketDataEntry.EntryType.RAW, SchemaType.MBP_10);
        byte[] testData = "test data".getBytes();
        String bucket = "test-bucket";

        ArgumentCaptor<Consumer<software.amazon.awssdk.services.s3.model.PutObjectRequest.Builder>> requestCaptor =
            ArgumentCaptor.forClass(Consumer.class);
        when(s3Client.putObject(requestCaptor.capture(), any(RequestBody.class)))
                .thenAnswer(invocation -> PutObjectResponse.builder().build());
        entry.saveToS3(s3Client, bucket, testData);

        software.amazon.awssdk.services.s3.model.PutObjectRequest.Builder builder =
            software.amazon.awssdk.services.s3.model.PutObjectRequest.builder();
        requestCaptor.getValue().accept(builder);
        software.amazon.awssdk.services.s3.model.PutObjectRequest request = builder.build();

        assertEquals(entry.getKey(), request.key());
        assertEquals(bucket, request.bucket());
    }

    @Test
    void testSaveToS3WithSchemaListCompressesAndUploads() throws IOException {
        // Given: An entry and a list of schemas to save
        LocalDateTime timestamp = LocalDateTime.of(2025, 4, 15, 14, 30);
        MarketDataEntry entry = entry(532, 151, timestamp, MarketDataEntry.EntryType.AGGREGATED, SchemaType.MBP_10);
        String bucket = "test-bucket";

        // Create schemas with specific sequence numbers
        MBP10Schema schema1 = (MBP10Schema) SchemaType.MBP_10.newInstance();
        MBP10Schema schema2 = (MBP10Schema) SchemaType.MBP_10.newInstance();
        schema1.encoder.sequence(100);
        schema2.encoder.sequence(101);
        List<Schema> schemas = List.of(schema1, schema2);

        // Capture the uploaded bytes
        ArgumentCaptor<RequestBody> bodyCaptor = ArgumentCaptor.forClass(RequestBody.class);
        when(s3Client.putObject(any(Consumer.class), bodyCaptor.capture()))
                .thenAnswer(invocation -> PutObjectResponse.builder().build());

        // When: Saving schemas to S3
        entry.saveToS3(s3Client, bucket, schemas);

        // Then: Verify the uploaded data can be decompressed back to the original schemas
        RequestBody capturedBody = bodyCaptor.getValue();
        byte[] uploadedBytes = capturedBody.contentStreamProvider().newStream().readAllBytes();

        // Decompress and verify
        try (ZstdInputStream zstdStream = new ZstdInputStream(new ByteArrayInputStream(uploadedBytes));
             ByteArrayOutputStream buffer = new ByteArrayOutputStream()) {
            zstdStream.transferTo(buffer);
            byte[] decompressedData = buffer.toByteArray();

            // Should have 2 schemas worth of data
            int expectedSize = schema1.totalMessageSize() * 2;
            assertEquals(expectedSize, decompressedData.length);
        }
    }

    @Test
    void testSaveToS3WithSchemaListPreservesSchemaContent() throws IOException {
        // Given: An entry and schemas with specific data
        LocalDateTime timestamp = LocalDateTime.of(2025, 4, 15, 14, 30);
        MarketDataEntry entry = entry(532, 151, timestamp, MarketDataEntry.EntryType.AGGREGATED, SchemaType.MBP_10);
        String bucket = "test-bucket";

        MBP10Schema schema = (MBP10Schema) SchemaType.MBP_10.newInstance();
        schema.encoder.sequence(12345);
        List<Schema> schemas = List.of(schema);

        ArgumentCaptor<RequestBody> bodyCaptor = ArgumentCaptor.forClass(RequestBody.class);
        when(s3Client.putObject(any(Consumer.class), bodyCaptor.capture()))
                .thenAnswer(invocation -> PutObjectResponse.builder().build());

        // When: Saving to S3
        entry.saveToS3(s3Client, bucket, schemas);

        // Then: Decompress and verify the sequence number is preserved
        byte[] uploadedBytes = bodyCaptor.getValue().contentStreamProvider().newStream().readAllBytes();
        try (ZstdInputStream zstdStream = new ZstdInputStream(new ByteArrayInputStream(uploadedBytes));
             ByteArrayOutputStream buffer = new ByteArrayOutputStream()) {
            zstdStream.transferTo(buffer);
            byte[] decompressedData = buffer.toByteArray();

            // Create a new schema and load the data into it
            MBP10Schema loadedSchema = (MBP10Schema) SchemaType.MBP_10.newInstance();
            loadedSchema.buffer.putBytes(0, decompressedData, 0, decompressedData.length);

            assertEquals(12345, loadedSchema.getSequenceNumber());
        }
    }

    // ============================================================================
    // LOAD FROM S3 TESTS
    // ============================================================================

    @Test
    void testLoadFromS3CallsGetObject() throws IOException {
        // Given: An entry and mocked S3 response with compressed data
        LocalDateTime timestamp = LocalDateTime.of(2025, 4, 15, 14, 30);
        MarketDataEntry entry = entry(532, 151, timestamp, MarketDataEntry.EntryType.RAW, SchemaType.MBP_10);
        String bucket = "test-bucket";

        // Create test schema data
        Schema testSchema = SchemaType.MBP_10.newInstance();
        byte[] schemaBytes = new byte[testSchema.totalMessageSize()];
        Arrays.fill(schemaBytes, (byte) 42);

        // Compress the data
        ByteArrayOutputStream compressedOutput = new ByteArrayOutputStream();
        try (ZstdOutputStream zstdStream = new ZstdOutputStream(compressedOutput)) {
            zstdStream.write(schemaBytes);
        }
        byte[] compressedData = compressedOutput.toByteArray();

        when(s3Client.getObject(any(Consumer.class))).thenAnswer(invocation -> new ResponseInputStream<>(
               GetObjectResponse.builder().build(),
               new ByteArrayInputStream(compressedData)
       ));

        // When: Loading from S3
        List<Schema> schemas = entry.loadFromS3(s3Client, bucket);

        // Then: One schema is loaded and S3 was called
        assertEquals(1, schemas.size());
        verify(s3Client).getObject(any(Consumer.class));
    }

    @Test
    void testLoadFromS3UsesCorrectKey() throws IOException {
        // Given: An entry with specific parameters
        LocalDateTime timestamp = LocalDateTime.of(2025, 4, 15, 14, 30);
        MarketDataEntry entry = entryWithUuid(532, 151, timestamp, MarketDataEntry.EntryType.RAW, SchemaType.MBP_10, "abc12345");
        String bucket = "test-bucket";

        // Create minimal compressed data
        Schema testSchema = SchemaType.MBP_10.newInstance();
        byte[] schemaBytes = new byte[testSchema.totalMessageSize()];
        ByteArrayOutputStream compressedOutput = new ByteArrayOutputStream();
        try (ZstdOutputStream zstdStream = new ZstdOutputStream(compressedOutput)) {
            zstdStream.write(schemaBytes);
        }
        byte[] compressedData = compressedOutput.toByteArray();

        when(s3Client.getObject(any(Consumer.class))).thenAnswer(invocation -> new ResponseInputStream<>(
                GetObjectResponse.builder().build(),
                new ByteArrayInputStream(compressedData)
        ));

        entry.loadFromS3(s3Client, bucket);

        ArgumentCaptor<Consumer<software.amazon.awssdk.services.s3.model.GetObjectRequest.Builder>> captor =
            ArgumentCaptor.forClass(Consumer.class);
        verify(s3Client).getObject(captor.capture());

        software.amazon.awssdk.services.s3.model.GetObjectRequest.Builder builder =
            software.amazon.awssdk.services.s3.model.GetObjectRequest.builder();
        captor.getValue().accept(builder);
        software.amazon.awssdk.services.s3.model.GetObjectRequest request = builder.build();

        assertEquals(entry.getKey(), request.key());
        assertEquals(bucket, request.bucket());
    }

    @Test
    void testSaveToS3VerifiesContentIsCorrect() throws IOException {
        // Given: An entry and specific byte data to save
        LocalDateTime timestamp = LocalDateTime.of(2025, 4, 15, 14, 30);
        MarketDataEntry entry = entry(532, 151, timestamp, MarketDataEntry.EntryType.AGGREGATED, SchemaType.MBP_10);
        byte[] testData = "important market data".getBytes();
        String bucket = "test-bucket";

        // Setup mock to capture the RequestBody
        ArgumentCaptor<RequestBody> bodyCaptor = ArgumentCaptor.forClass(RequestBody.class);
        when(s3Client.putObject(any(Consumer.class), bodyCaptor.capture()))
                .thenAnswer(invocation -> PutObjectResponse.builder().build());

        // When: Saving to S3
        entry.saveToS3(s3Client, bucket, testData);

        // Then: The content uploaded matches the input data
        RequestBody capturedBody = bodyCaptor.getValue();
        assertNotNull(capturedBody);
        verify(s3Client).putObject(any(Consumer.class), eq(capturedBody));
    }

    @Test
    void testLoadFromS3WithMultipleSchemas() throws IOException {
        // Given: An entry and mocked S3 response with multiple schema records
        LocalDateTime timestamp = LocalDateTime.of(2025, 4, 15, 14, 30);
        MarketDataEntry entry = entry(532, 151, timestamp, MarketDataEntry.EntryType.RAW, SchemaType.MBP_10);
        String bucket = "test-bucket";

        // Create multiple schema records and compress them together
        Schema schema1 = SchemaType.MBP_10.newInstance();
        Schema schema2 = SchemaType.MBP_10.newInstance();
        Schema schema3 = SchemaType.MBP_10.newInstance();

        byte[] schema1Bytes = new byte[schema1.totalMessageSize()];
        byte[] schema2Bytes = new byte[schema2.totalMessageSize()];
        byte[] schema3Bytes = new byte[schema3.totalMessageSize()];

        // Fill with different patterns to distinguish them
        Arrays.fill(schema1Bytes, (byte) 11);
        Arrays.fill(schema2Bytes, (byte) 22);
        Arrays.fill(schema3Bytes, (byte) 33);

        // Compress all schemas together
        ByteArrayOutputStream compressedOutput = new ByteArrayOutputStream();
        try (ZstdOutputStream zstdStream = new ZstdOutputStream(compressedOutput)) {
            zstdStream.write(schema1Bytes);
            zstdStream.write(schema2Bytes);
            zstdStream.write(schema3Bytes);
        }
        byte[] compressedData = compressedOutput.toByteArray();

        when(s3Client.getObject(any(Consumer.class))).thenAnswer(invocation -> new ResponseInputStream<>(
                GetObjectResponse.builder().build(),
                new ByteArrayInputStream(compressedData)
        ));

        // When: Loading from S3
        List<Schema> schemas = entry.loadFromS3(s3Client, bucket);

        // Then: All three schemas are loaded and S3 was called
        assertEquals(3, schemas.size());
        verify(s3Client).getObject(any(Consumer.class));
    }



    // ============================================================================
    // FETCH KEYS FROM S3 TESTS
    // ============================================================================

    @Test
    void testGetKeysForListingByDayWithSingleKey() {
        // Given: A listing, specific day, and S3 with one key
        Listing testListing = listing(532, 151, SchemaType.MBP_10);
        LocalDateTime day = LocalDateTime.of(2025, 4, 15, 0, 0);
        String bucket = "test-bucket";

        S3Object obj = S3Object.builder().key("532/151/2025/4/15/10/30/mbp-10.zst").build();
        ListObjectsV2Iterable mockIterable = mock(ListObjectsV2Iterable.class);
        SdkIterable<S3Object> contents = () -> Collections.singletonList(obj).iterator();
        when(s3Client.listObjectsV2Paginator(any(Consumer.class))).thenReturn(mockIterable);
        when(mockIterable.contents()).thenReturn(contents);

        // When: Getting keys for a specific day
        List<MarketDataEntry> entries = MarketDataEntry.getKeysForListingByDay(s3Client, bucket, testListing, day);

        // Then: One entry is returned with correct timestamp
        assertEquals(1, entries.size());
        assertEquals(10, entries.get(0).getTimestamp().getHour());
        assertEquals(30, entries.get(0).getTimestamp().getMinute());
    }

    @Test
    void testGetKeysForListingByDayWithMultipleKeys() {
        // Given: A listing, specific day, and S3 with multiple keys
        Listing testListing = listing(532, 151, SchemaType.MBP_10);
        LocalDateTime day = LocalDateTime.of(2025, 4, 15, 0, 0);
        String bucket = "test-bucket";

        // Create keys in non-sorted order to test sorting
        S3Object obj1 = S3Object.builder().key("532/151/2025/4/15/16/45/mbp-10.zst").build();
        S3Object obj2 = S3Object.builder().key("532/151/2025/4/15/10/30/mbp-10.zst").build();
        S3Object obj3 = S3Object.builder().key("532/151/2025/4/15/14/00/mbp-10.zst").build();

        ListObjectsV2Iterable mockIterable = mock(ListObjectsV2Iterable.class);
        SdkIterable<S3Object> contents = () -> Arrays.asList(obj1, obj2, obj3).iterator();
        when(mockIterable.contents()).thenReturn(contents);
        when(s3Client.listObjectsV2Paginator(any(Consumer.class))).thenReturn(mockIterable);

        // When: Getting keys for a specific day
        List<MarketDataEntry> entries = MarketDataEntry.getKeysForListingByDay(s3Client, bucket, testListing, day);

        // Then: All entries are returned and sorted by timestamp
        assertEquals(3, entries.size());
        assertEquals(10, entries.get(0).getTimestamp().getHour());
        assertEquals(14, entries.get(1).getTimestamp().getHour());
        assertEquals(16, entries.get(2).getTimestamp().getHour());
    }

    @Test
    void testGetKeysForListingByDayWithEmptyResult() {
        // Given: A listing, specific day, and S3 with no matching keys
        Listing testListing = listing(532, 151, SchemaType.MBP_10);
        LocalDateTime day = LocalDateTime.of(2025, 4, 15, 0, 0);
        String bucket = "test-bucket";

        ListObjectsV2Iterable mockIterable = mock(ListObjectsV2Iterable.class);
        SdkIterable<S3Object> contents = Collections::emptyIterator;
        when(mockIterable.contents()).thenReturn(contents);
        when(s3Client.listObjectsV2Paginator(any(Consumer.class))).thenReturn(mockIterable);

        // When: Getting keys for a day with no data
        List<MarketDataEntry> entries = MarketDataEntry.getKeysForListingByDay(s3Client, bucket, testListing, day);

        // Then: Empty list is returned
        assertEquals(0, entries.size());
    }

    @Test
    void testGetKeysForListingByDayWithCorrectPrefix() {
        // Given: A listing and specific day
        Listing testListing = listing(532, 151, SchemaType.MBP_10);
        LocalDateTime day = LocalDateTime.of(2025, 4, 15, 0, 0);
        String bucket = "test-bucket";

        S3Object obj = S3Object.builder().key("532/151/2025/4/15/10/30/mbp-10.zst").build();
        ListObjectsV2Iterable mockIterable = mock(ListObjectsV2Iterable.class);
        SdkIterable<S3Object> contents = () -> Collections.singletonList(obj).iterator();
        when(mockIterable.contents()).thenReturn(contents);
        when(s3Client.listObjectsV2Paginator(any(Consumer.class))).thenReturn(mockIterable);

        // When: Getting keys for a specific day
        MarketDataEntry.getKeysForListingByDay(s3Client, bucket, testListing, day);

        // Then: S3 is called with correct prefix (security/exchange/year/month/day/)
        ArgumentCaptor<Consumer<software.amazon.awssdk.services.s3.model.ListObjectsV2Request.Builder>> captor =
            ArgumentCaptor.forClass(Consumer.class);
        verify(s3Client).listObjectsV2Paginator(captor.capture());

        software.amazon.awssdk.services.s3.model.ListObjectsV2Request.Builder builder =
            software.amazon.awssdk.services.s3.model.ListObjectsV2Request.builder();
        captor.getValue().accept(builder);
        software.amazon.awssdk.services.s3.model.ListObjectsV2Request request = builder.build();

        assertEquals("532/151/2025/4/15/", request.prefix());
        assertEquals(bucket, request.bucket());
    }

    @Test
    void testGetKeysForListingByDayWithDifferentDates() {
        // Given: Different dates should produce different prefixes
        Listing testListing = listing(999, 888, SchemaType.MBO);
        LocalDateTime day1 = LocalDateTime.of(2025, 1, 1, 0, 0);
        LocalDateTime day2 = LocalDateTime.of(2025, 12, 31, 0, 0);
        String bucket = "test-bucket";

        ListObjectsV2Iterable mockIterable = mock(ListObjectsV2Iterable.class);
        SdkIterable<S3Object> contents = Collections::emptyIterator;
        when(mockIterable.contents()).thenReturn(contents);
        when(s3Client.listObjectsV2Paginator(any(Consumer.class))).thenReturn(mockIterable);

        // When: Getting keys for different days
        MarketDataEntry.getKeysForListingByDay(s3Client, bucket, testListing, day1);
        MarketDataEntry.getKeysForListingByDay(s3Client, bucket, testListing, day2);

        // Then: Different prefixes are used
        ArgumentCaptor<Consumer<software.amazon.awssdk.services.s3.model.ListObjectsV2Request.Builder>> captor =
            ArgumentCaptor.forClass(Consumer.class);
        verify(s3Client, times(2)).listObjectsV2Paginator(captor.capture());

        List<Consumer<software.amazon.awssdk.services.s3.model.ListObjectsV2Request.Builder>> capturedConsumers = captor.getAllValues();

        software.amazon.awssdk.services.s3.model.ListObjectsV2Request.Builder builder1 =
            software.amazon.awssdk.services.s3.model.ListObjectsV2Request.builder();
        capturedConsumers.get(0).accept(builder1);
        assertEquals("999/888/2025/1/1/", builder1.build().prefix());

        software.amazon.awssdk.services.s3.model.ListObjectsV2Request.Builder builder2 =
            software.amazon.awssdk.services.s3.model.ListObjectsV2Request.builder();
        capturedConsumers.get(1).accept(builder2);
        assertEquals("999/888/2025/12/31/", builder2.build().prefix());
    }

    @Test
    void testGetAllKeysForListingWithMultipleDays() {
        // Given: A listing and S3 with keys from multiple days
        Listing testListing = listing(532, 151, SchemaType.MBP_10);
        String bucket = "test-bucket";

        // Create keys from different days
        S3Object obj1 = S3Object.builder().key("532/151/2025/4/14/10/30/mbp-10.zst").build();
        S3Object obj2 = S3Object.builder().key("532/151/2025/4/15/14/30/mbp-10.zst").build();
        S3Object obj3 = S3Object.builder().key("532/151/2025/4/16/16/45/mbp-10.zst").build();

        ListObjectsV2Iterable mockIterable = mock(ListObjectsV2Iterable.class);
        SdkIterable<S3Object> contents = () -> Arrays.asList(obj1, obj2, obj3).iterator();
        when(mockIterable.contents()).thenReturn(contents);
        when(s3Client.listObjectsV2Paginator(any(Consumer.class))).thenReturn(mockIterable);

        // When: Getting all keys for a listing
        List<MarketDataEntry> entries = MarketDataEntry.getAllKeysForListing(s3Client, bucket, testListing);

        // Then: All entries from all days are returned and sorted
        assertEquals(3, entries.size());
        assertEquals(14, entries.get(0).getTimestamp().getDayOfMonth());
        assertEquals(15, entries.get(1).getTimestamp().getDayOfMonth());
        assertEquals(16, entries.get(2).getTimestamp().getDayOfMonth());
    }

    @Test
    void testGetAllKeysForListingWithCorrectPrefix() {
        // Given: A listing
        Listing testListing = listing(532, 151, SchemaType.MBP_10);
        String bucket = "test-bucket";

        S3Object obj = S3Object.builder().key("532/151/2025/4/15/10/30/mbp-10.zst").build();
        ListObjectsV2Iterable mockIterable = mock(ListObjectsV2Iterable.class);
        SdkIterable<S3Object> contents = () -> Collections.singletonList(obj).iterator();
        when(mockIterable.contents()).thenReturn(contents);
        when(s3Client.listObjectsV2Paginator(any(Consumer.class))).thenReturn(mockIterable);

        // When: Getting all keys for a listing
        MarketDataEntry.getAllKeysForListing(s3Client, bucket, testListing);

        // Then: S3 is called with correct prefix (security/exchange/ only, no date)
        ArgumentCaptor<Consumer<software.amazon.awssdk.services.s3.model.ListObjectsV2Request.Builder>> captor =
            ArgumentCaptor.forClass(Consumer.class);
        verify(s3Client).listObjectsV2Paginator(captor.capture());

        software.amazon.awssdk.services.s3.model.ListObjectsV2Request.Builder builder =
            software.amazon.awssdk.services.s3.model.ListObjectsV2Request.builder();
        captor.getValue().accept(builder);
        software.amazon.awssdk.services.s3.model.ListObjectsV2Request request = builder.build();

        assertEquals("532/151/", request.prefix());
        assertEquals(bucket, request.bucket());
    }

    @Test
    void testGetAllKeysForListingWithEmptyResult() {
        // Given: A listing with no matching keys
        Listing testListing = listing(532, 151, SchemaType.MBP_10);
        String bucket = "test-bucket";

        ListObjectsV2Iterable mockIterable = mock(ListObjectsV2Iterable.class);
        SdkIterable<S3Object> contents = Collections::emptyIterator;
        when(mockIterable.contents()).thenReturn(contents);
        when(s3Client.listObjectsV2Paginator(any(Consumer.class))).thenReturn(mockIterable);

        // When: Getting all keys for a listing with no data
        List<MarketDataEntry> entries = MarketDataEntry.getAllKeysForListing(s3Client, bucket, testListing);

        // Then: Empty list is returned
        assertEquals(0, entries.size());
    }

    @Test
    void testGetKeysForListingByDayWithRawAndAggregatedEntries() {
        // Given: A listing and S3 with both raw and aggregated keys
        Listing testListing = listing(532, 151, SchemaType.MBP_10);
        LocalDateTime day = LocalDateTime.of(2025, 4, 15, 0, 0);
        String bucket = "test-bucket";

        // Mix of raw (with UUID) and aggregated (no UUID) keys
        S3Object rawKey = S3Object.builder().key("532/151/2025/4/15/10/30/mbp-10/abc12345.zst").build();
        S3Object aggKey = S3Object.builder().key("532/151/2025/4/15/14/30/mbp-10.zst").build();

        ListObjectsV2Iterable mockIterable = mock(ListObjectsV2Iterable.class);
        SdkIterable<S3Object> contents = () -> Arrays.asList(rawKey, aggKey).iterator();
        when(mockIterable.contents()).thenReturn(contents);
        when(s3Client.listObjectsV2Paginator(any(Consumer.class))).thenReturn(mockIterable);

        // When: Getting keys for a specific day
        List<MarketDataEntry> entries = MarketDataEntry.getKeysForListingByDay(s3Client, bucket, testListing, day);

        // Then: Both raw and aggregated entries are returned
        assertEquals(2, entries.size());
        assertEquals(MarketDataEntry.EntryType.RAW, entries.get(0).getEntryType());
        assertEquals(MarketDataEntry.EntryType.AGGREGATED, entries.get(1).getEntryType());
    }

    // ============================================================================
    // UUID AND CONSTRUCTOR TESTS
    // ============================================================================

    @Test
    void testConstructorWithRandomUUID() {
        // Given: Creating entries without specifying UUID
        LocalDateTime timestamp = LocalDateTime.of(2025, 4, 15, 14, 30);
        MarketDataEntry entry1 = entry(532, 151, timestamp, MarketDataEntry.EntryType.RAW, SchemaType.MBP_10);
        MarketDataEntry entry2 = entry(532, 151, timestamp, MarketDataEntry.EntryType.RAW, SchemaType.MBP_10);

        // When/Then: Each entry gets a unique 8-character UUID
        assertNotEquals(entry1.getUUID(), entry2.getUUID());
        assertEquals(8, entry1.getUUID().length());
        assertEquals(8, entry2.getUUID().length());
    }

    @Test
    void testToString() {
        LocalDateTime timestamp = LocalDateTime.of(2025, 4, 15, 14, 30);
        MarketDataEntry entry = entryWithUuid(532, 151, timestamp, MarketDataEntry.EntryType.RAW, SchemaType.MBP_10, "test1234");

        String str = entry.toString();

        assertTrue(str.contains("MarketDataEntry"));
        assertTrue(str.contains("test1234"));
    }
}

