package group.gnometrading.collector.merger;

import com.github.luben.zstd.ZstdInputStream;
import com.github.luben.zstd.ZstdOutputStream;
import group.gnometrading.SecurityMaster;
import group.gnometrading.collector.MarketDataEntry;
import group.gnometrading.logging.LogMessage;
import group.gnometrading.logging.Logger;
import group.gnometrading.schemas.MBP10Schema;
import group.gnometrading.schemas.Schema;
import group.gnometrading.schemas.SchemaType;
import group.gnometrading.sm.Exchange;
import group.gnometrading.sm.Listing;
import group.gnometrading.sm.Security;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.mockStatic;

@ExtendWith(MockitoExtension.class)
class MarketDataMergerTest {

    @Mock
    private Logger logger;

    @Mock
    private S3Client s3Client;

    @Mock
    private SecurityMaster securityMaster;

    private MarketDataMerger merger;
    private Clock fixedClock;
    private String inputBucket = "input-bucket";
    private String outputBucket = "output-bucket";
    private String archiveBucket = "archive-bucket";
    private MockedStatic<MarketDataEntry> marketDataEntryMock;

    @BeforeEach
    void setUp() {
        // Fixed clock: 2025-04-15 15:00:00 UTC
        fixedClock = Clock.fixed(
            Instant.parse("2025-04-15T15:00:00Z"),
            ZoneId.of("UTC")
        );
        merger = new MarketDataMerger(
            logger,
            fixedClock,
            s3Client,
            securityMaster,
            inputBucket,
            outputBucket,
            archiveBucket
        );

        // Initialize mockStatic for MarketDataEntry
        marketDataEntryMock = mockStatic(MarketDataEntry.class);
    }

    @AfterEach
    void tearDown() {
        // Close the mockStatic after each test
        if (marketDataEntryMock != null) {
            marketDataEntryMock.close();
        }
    }

    /**
     * Helper to create a fake Listing with specific parameters.
     */
    private Listing listing(int securityId, int exchangeId, SchemaType schemaType) {
        return new Listing(
                securityId,
                new Exchange(exchangeId, "test-exchange", "test-region", schemaType),
                new Security(securityId, "test-security", 1),
                "test-symbol",
                "test-name"
        );
    }

    /**
     * Helper to create a schema with a specific sequence number.
     */
    private Schema schema(long sequenceNumber) {
        MBP10Schema schema = (MBP10Schema) SchemaType.MBP_10.newInstance();
        schema.encoder.sequence(sequenceNumber);
        return schema;
    }

    /**
     * Helper to compress a list of schemas into bytes for S3 mocking.
     * Uses getBytes() to copy from the buffer (same pattern as MarketDataCollector).
     */
    private byte[] compressSchemas(List<Schema> schemas) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        for (Schema schema : schemas) {
            byte[] schemaBytes = new byte[schema.totalMessageSize()];
            schema.buffer.getBytes(0, schemaBytes, 0, schema.totalMessageSize());
            outputStream.write(schemaBytes);
        }
        byte[] uncompressedData = outputStream.toByteArray();

        ByteArrayOutputStream compressedOutput = new ByteArrayOutputStream();
        try (ZstdOutputStream zstdStream = new ZstdOutputStream(compressedOutput)) {
            zstdStream.write(uncompressedData);
        }
        return compressedOutput.toByteArray();
    }

    /**
     * Helper to decompress bytes and extract schemas.
     */
    private List<Schema> decompressSchemas(byte[] compressedData, SchemaType schemaType) throws IOException {
        List<Schema> schemas = new ArrayList<>();
        int expectedSize = schemaType.getInstance().totalMessageSize();

        try (ZstdInputStream zstdStream = new ZstdInputStream(new ByteArrayInputStream(compressedData));
             ByteArrayOutputStream buffer = new ByteArrayOutputStream()) {
            zstdStream.transferTo(buffer);
            byte[] decompressedData = buffer.toByteArray();

            for (int i = 0; i < decompressedData.length; i += expectedSize) {
                byte[] recordData = Arrays.copyOfRange(decompressedData, i, i + expectedSize);
                Schema schema = schemaType.newInstance();
                schema.buffer.putBytes(0, recordData, 0, recordData.length);
                schemas.add(schema);
            }
        }
        return schemas;
    }

    /**
     * Helper to setup S3 mocks for successful copy, delete, and put operations.
     */
    private void setupSuccessfulS3Operations() {
        CopyObjectResponse copyResponse = mock(CopyObjectResponse.class);
        when(copyResponse.sdkHttpResponse()).thenReturn(mock(software.amazon.awssdk.http.SdkHttpResponse.class));
        when(copyResponse.sdkHttpResponse().isSuccessful()).thenReturn(true);
        when(s3Client.copyObject(any(Consumer.class))).thenReturn(copyResponse);

        DeleteObjectsResponse deleteResponse = mock(DeleteObjectsResponse.class);
        when(deleteResponse.hasErrors()).thenReturn(false);
        when(s3Client.deleteObjects(any(Consumer.class))).thenReturn(deleteResponse);

        PutObjectResponse putResponse = mock(PutObjectResponse.class);
        when(s3Client.putObject(any(Consumer.class), any(RequestBody.class))).thenReturn(putResponse);
    }

    /**
     * Helper to capture and return the RequestBody bytes from the first putObject call.
     */
    private byte[] capturePutObjectBytes() {
        ArgumentCaptor<RequestBody> bodyCaptor = ArgumentCaptor.forClass(RequestBody.class);
        verify(s3Client, atLeastOnce()).putObject(any(Consumer.class), bodyCaptor.capture());
        try {
            // Return the first captured value
            return bodyCaptor.getAllValues().get(0).contentStreamProvider().newStream().readAllBytes();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Helper to capture the key from putObject calls.
     */
    private String capturePutObjectKey() {
        ArgumentCaptor<Consumer<PutObjectRequest.Builder>> captor = ArgumentCaptor.forClass(Consumer.class);
        verify(s3Client).putObject(captor.capture(), any(RequestBody.class));
        PutObjectRequest.Builder builder = PutObjectRequest.builder();
        captor.getValue().accept(builder);
        return builder.build().key();
    }

    /**
     * Helper to capture the keys from copyObject calls.
     */
    private List<String> captureCopyObjectKeys() {
        ArgumentCaptor<Consumer<CopyObjectRequest.Builder>> captor = ArgumentCaptor.forClass(Consumer.class);
        verify(s3Client, atLeastOnce()).copyObject(captor.capture());
        List<String> keys = new ArrayList<>();
        for (Consumer<CopyObjectRequest.Builder> consumer : captor.getAllValues()) {
            CopyObjectRequest.Builder builder = CopyObjectRequest.builder();
            consumer.accept(builder);
            keys.add(builder.build().sourceKey());
        }
        return keys;
    }

    /**
     * Helper to capture the keys from deleteObjects calls.
     */
    private List<String> captureDeleteObjectKeys() {
        ArgumentCaptor<Consumer<DeleteObjectsRequest.Builder>> captor = ArgumentCaptor.forClass(Consumer.class);
        verify(s3Client).deleteObjects(captor.capture());
        DeleteObjectsRequest.Builder builder = DeleteObjectsRequest.builder();
        captor.getValue().accept(builder);
        return builder.build().delete().objects().stream()
                .map(ObjectIdentifier::key)
                .toList();
    }

    // ============================================================================
    // EMPTY INPUT TESTS
    // ============================================================================

    @Test
    void testRunMergerWithNoRawFiles() {
        // Given: S3 has no raw files
        ListObjectsV2Iterable mockIterable = mock(ListObjectsV2Iterable.class);
        ListObjectsV2Response emptyResponse = ListObjectsV2Response.builder().contents(Collections.emptyList()).build();
        when(mockIterable.stream()).thenReturn(Stream.of(emptyResponse));
        when(s3Client.listObjectsV2Paginator(any(Consumer.class))).thenReturn(mockIterable);

        // When: Running merger
        Set<Listing> result = merger.runMerger();

        // Then: Empty set is returned and no merging happens
        assertEquals(0, result.size());
        verify(logger).logf(eq(LogMessage.DEBUG), eq("No new S3 files produced to run merging on"));
        verify(s3Client, never()).copyObject(any(Consumer.class));
        verify(s3Client, never()).deleteObjects(any(Consumer.class));
    }

    // ============================================================================
    // MERGE DELAY TESTS
    // ============================================================================

    @Test
    void testRunMergerSkipsFilesWithinMergeDelay() {
        // Given: Raw file from 14:00 (within 60-minute merge delay from 15:00)
        Listing testListing = listing(532, 151, SchemaType.MBP_10);

        // File from 14:00 is within the 60-minute merge delay (current time is 15:00)
        String recentKey = "532/151/2025/4/15/14/0/mbp-10/uuid1234.zst";
        S3Object recentObj = S3Object.builder().key(recentKey).build();

        ListObjectsV2Iterable mockIterable = mock(ListObjectsV2Iterable.class);
        ListObjectsV2Response response = ListObjectsV2Response.builder().contents(Collections.singletonList(recentObj)).build();
        when(mockIterable.stream()).thenReturn(Stream.of(response));
        when(s3Client.listObjectsV2Paginator(any(Consumer.class))).thenReturn(mockIterable);

        // Mock MarketDataEntry.fromKey to return a raw entry from 14:00
        MarketDataEntry rawEntry = new MarketDataEntry(testListing, LocalDateTime.of(2025, 4, 15, 14, 0), MarketDataEntry.EntryType.RAW);
        marketDataEntryMock.when(() -> MarketDataEntry.fromKey(recentKey))
            .thenReturn(rawEntry);

        // When: Running merger
        Set<Listing> result = merger.runMerger();

        // Then: File is skipped (within merge delay) - no processing happens
        assertEquals(0, result.size());
        verify(s3Client, never()).getObject(any(Consumer.class));
        verify(s3Client, never()).copyObject(any(Consumer.class));
        verify(s3Client, never()).deleteObjects(any(Consumer.class));
    }

    // ============================================================================
    // FULL MERGE PROCESSING TESTS
    // ============================================================================

    @Test
    void testRunMergerSingleCollectorVerifyMergedContent() throws IOException {
        // Given: One raw file with 3 schemas (sequences 100, 101, 102)
        Listing testListing = listing(532, 151, SchemaType.MBP_10);
        String rawKey = "532/151/2025/4/15/13/0/mbp-10/uuid1234.zst";
        S3Object rawObj = S3Object.builder().key(rawKey).build();

        ListObjectsV2Iterable mockIterable = mock(ListObjectsV2Iterable.class);
        ListObjectsV2Response response = ListObjectsV2Response.builder().contents(Collections.singletonList(rawObj)).build();
        when(mockIterable.stream()).thenReturn(Stream.of(response));
        when(s3Client.listObjectsV2Paginator(any(Consumer.class))).thenReturn(mockIterable);

        // Mock SecurityMaster to return the listing
        when(securityMaster.getListing(151, 532)).thenReturn(testListing);

        // Mock MarketDataEntry.fromKey
        MarketDataEntry rawEntry = new MarketDataEntry(testListing, LocalDateTime.of(2025, 4, 15, 13, 0), MarketDataEntry.EntryType.RAW, "uuid1234");
        marketDataEntryMock.when(() -> MarketDataEntry.fromKey(rawKey))
            .thenReturn(rawEntry);

        // Create input schemas
        List<Schema> inputSchemas = List.of(schema(100L), schema(101L), schema(102L));
        byte[] compressedData = compressSchemas(inputSchemas);

        // Mock S3 getObject to return compressed data
        when(s3Client.getObject(any(Consumer.class))).thenAnswer(invocation ->
            new ResponseInputStream<>(GetObjectResponse.builder().build(), new ByteArrayInputStream(compressedData))
        );

        // Mock successful S3 operations
        setupSuccessfulS3Operations();

        // When: Running merger
        Set<Listing> result = merger.runMerger();

        // Then: Verify correct listing is returned
        assertEquals(1, result.size());
        assertTrue(result.contains(testListing));

        // Verify the merged content matches what the merge strategy would produce
        MBP10MergeStrategy strategy = new MBP10MergeStrategy();
        Map<String, List<Schema>> strategyInput = new LinkedHashMap<>();
        strategyInput.put("uuid1234", inputSchemas);
        List<Schema> expectedOutput = strategy.mergeRecords(logger, strategyInput);

        // Capture and verify the uploaded content
        byte[] uploadedBytes = capturePutObjectBytes();
        List<Schema> uploadedSchemas = decompressSchemas(uploadedBytes, SchemaType.MBP_10);

        assertEquals(expectedOutput.size(), uploadedSchemas.size());
        for (int i = 0; i < expectedOutput.size(); i++) {
            assertEquals(expectedOutput.get(i).getSequenceNumber(), uploadedSchemas.get(i).getSequenceNumber());
        }
    }

    @Test
    void testRunMergerTwoCollectorsMergesCorrectly() throws IOException {
        // Given: Two raw files from different collectors with overlapping sequences
        Listing testListing = listing(532, 151, SchemaType.MBP_10);
        String key1 = "532/151/2025/4/15/13/0/mbp-10/uuid1111.zst";
        String key2 = "532/151/2025/4/15/13/0/mbp-10/uuid2222.zst";
        S3Object obj1 = S3Object.builder().key(key1).build();
        S3Object obj2 = S3Object.builder().key(key2).build();

        ListObjectsV2Iterable mockIterable = mock(ListObjectsV2Iterable.class);
        ListObjectsV2Response response = ListObjectsV2Response.builder().contents(Arrays.asList(obj1, obj2)).build();
        when(mockIterable.stream()).thenReturn(Stream.of(response));
        when(s3Client.listObjectsV2Paginator(any(Consumer.class))).thenReturn(mockIterable);

        // Mock MarketDataEntry.fromKey for both keys
        MarketDataEntry rawEntry1 = new MarketDataEntry(testListing, LocalDateTime.of(2025, 4, 15, 13, 0), MarketDataEntry.EntryType.RAW, "uuid1111");
        MarketDataEntry rawEntry2 = new MarketDataEntry(testListing, LocalDateTime.of(2025, 4, 15, 13, 0), MarketDataEntry.EntryType.RAW, "uuid2222");
        marketDataEntryMock.when(() -> MarketDataEntry.fromKey(key1)).thenReturn(rawEntry1);
        marketDataEntryMock.when(() -> MarketDataEntry.fromKey(key2)).thenReturn(rawEntry2);

        // Collector 1: sequences 100, 101, 102
        List<Schema> schemas1 = List.of(schema(100L), schema(101L), schema(102L));
        byte[] compressed1 = compressSchemas(schemas1);

        // Collector 2: sequences 100, 101 (missing 102)
        List<Schema> schemas2 = List.of(schema(100L), schema(101L));
        byte[] compressed2 = compressSchemas(schemas2);

        // Mock S3 getObject to return different data for each key
        when(s3Client.getObject(any(Consumer.class))).thenAnswer(invocation -> {
            Consumer<GetObjectRequest.Builder> consumer = invocation.getArgument(0);
            GetObjectRequest.Builder builder = GetObjectRequest.builder();
            consumer.accept(builder);
            String requestedKey = builder.build().key();

            if (requestedKey.equals(key1)) {
                return new ResponseInputStream<>(GetObjectResponse.builder().build(), new ByteArrayInputStream(compressed1));
            } else {
                return new ResponseInputStream<>(GetObjectResponse.builder().build(), new ByteArrayInputStream(compressed2));
            }
        });

        // Mock successful S3 operations
        setupSuccessfulS3Operations();

        // When: Running merger
        Set<Listing> result = merger.runMerger();

        // Then: Verify correct listing is returned
        assertEquals(1, result.size());

        // Verify the merged content matches what the merge strategy would produce
        MBP10MergeStrategy strategy = new MBP10MergeStrategy();
        Map<String, List<Schema>> strategyInput = new LinkedHashMap<>();
        strategyInput.put("uuid1111", schemas1);
        strategyInput.put("uuid2222", schemas2);
        List<Schema> expectedOutput = strategy.mergeRecords(logger, strategyInput);

        // Capture and verify the uploaded content
        byte[] uploadedBytes = capturePutObjectBytes();
        List<Schema> uploadedSchemas = decompressSchemas(uploadedBytes, SchemaType.MBP_10);

        assertEquals(expectedOutput.size(), uploadedSchemas.size());
        for (int i = 0; i < expectedOutput.size(); i++) {
            assertEquals(expectedOutput.get(i).getSequenceNumber(), uploadedSchemas.get(i).getSequenceNumber());
        }
    }

    @Test
    void testRunMergerVerifiesCorrectKeyIsUploaded() throws IOException {
        // Given: One raw file older than merge delay
        Listing testListing = listing(532, 151, SchemaType.MBP_10);
        String rawKey = "532/151/2025/4/15/13/0/mbp-10/uuid1234.zst";
        S3Object rawObj = S3Object.builder().key(rawKey).build();

        ListObjectsV2Iterable mockIterable = mock(ListObjectsV2Iterable.class);
        ListObjectsV2Response response = ListObjectsV2Response.builder().contents(Collections.singletonList(rawObj)).build();
        when(mockIterable.stream()).thenReturn(Stream.of(response));
        when(s3Client.listObjectsV2Paginator(any(Consumer.class))).thenReturn(mockIterable);

        // Mock MarketDataEntry.fromKey
        MarketDataEntry rawEntry = new MarketDataEntry(testListing, LocalDateTime.of(2025, 4, 15, 13, 0), MarketDataEntry.EntryType.RAW, "uuid1234");
        marketDataEntryMock.when(() -> MarketDataEntry.fromKey(rawKey))
            .thenReturn(rawEntry);

        // Create input schemas
        List<Schema> inputSchemas = List.of(schema(100L));
        byte[] compressedData = compressSchemas(inputSchemas);

        when(s3Client.getObject(any(Consumer.class))).thenAnswer(invocation ->
            new ResponseInputStream<>(GetObjectResponse.builder().build(), new ByteArrayInputStream(compressedData))
        );

        setupSuccessfulS3Operations();

        // When: Running merger
        merger.runMerger();

        // Then: Verify the correct aggregated key is uploaded (no UUID)
        String uploadedKey = capturePutObjectKey();
        assertEquals("532/151/2025/4/15/13/0/mbp-10.zst", uploadedKey);
    }

    // ============================================================================
    // ARCHIVE AND DELETE VERIFICATION TESTS
    // ============================================================================

    @Test
    void testRunMergerArchivesCorrectKeys() throws IOException {
        // Given: Two raw files
        Listing testListing = listing(532, 151, SchemaType.MBP_10);
        String key1 = "532/151/2025/4/15/13/0/mbp-10/uuid1111.zst";
        String key2 = "532/151/2025/4/15/13/0/mbp-10/uuid2222.zst";
        S3Object obj1 = S3Object.builder().key(key1).build();
        S3Object obj2 = S3Object.builder().key(key2).build();

        ListObjectsV2Iterable mockIterable = mock(ListObjectsV2Iterable.class);
        ListObjectsV2Response response = ListObjectsV2Response.builder().contents(Arrays.asList(obj1, obj2)).build();
        when(mockIterable.stream()).thenReturn(Stream.of(response));
        when(s3Client.listObjectsV2Paginator(any(Consumer.class))).thenReturn(mockIterable);

        MarketDataEntry rawEntry1 = new MarketDataEntry(testListing, LocalDateTime.of(2025, 4, 15, 13, 0), MarketDataEntry.EntryType.RAW, "uuid1111");
        MarketDataEntry rawEntry2 = new MarketDataEntry(testListing, LocalDateTime.of(2025, 4, 15, 13, 0), MarketDataEntry.EntryType.RAW, "uuid2222");
        marketDataEntryMock.when(() -> MarketDataEntry.fromKey(key1)).thenReturn(rawEntry1);
        marketDataEntryMock.when(() -> MarketDataEntry.fromKey(key2)).thenReturn(rawEntry2);

        List<Schema> schemas = List.of(schema(100L));
        byte[] compressedData = compressSchemas(schemas);
        when(s3Client.getObject(any(Consumer.class))).thenAnswer(invocation ->
            new ResponseInputStream<>(GetObjectResponse.builder().build(), new ByteArrayInputStream(compressedData))
        );

        setupSuccessfulS3Operations();

        // When: Running merger
        merger.runMerger();

        // Then: Verify both keys were copied to archive
        List<String> copiedKeys = captureCopyObjectKeys();
        assertTrue(copiedKeys.contains(key1), "Key1 should be archived");
        assertTrue(copiedKeys.contains(key2), "Key2 should be archived");
    }

    @Test
    void testRunMergerDeletesCorrectKeys() throws IOException {
        // Given: Two raw files
        Listing testListing = listing(532, 151, SchemaType.MBP_10);
        String key1 = "532/151/2025/4/15/13/0/mbp-10/uuid1111.zst";
        String key2 = "532/151/2025/4/15/13/0/mbp-10/uuid2222.zst";
        S3Object obj1 = S3Object.builder().key(key1).build();
        S3Object obj2 = S3Object.builder().key(key2).build();

        ListObjectsV2Iterable mockIterable = mock(ListObjectsV2Iterable.class);
        ListObjectsV2Response response = ListObjectsV2Response.builder().contents(Arrays.asList(obj1, obj2)).build();
        when(mockIterable.stream()).thenReturn(Stream.of(response));
        when(s3Client.listObjectsV2Paginator(any(Consumer.class))).thenReturn(mockIterable);

        MarketDataEntry rawEntry1 = new MarketDataEntry(testListing, LocalDateTime.of(2025, 4, 15, 13, 0), MarketDataEntry.EntryType.RAW, "uuid1111");
        MarketDataEntry rawEntry2 = new MarketDataEntry(testListing, LocalDateTime.of(2025, 4, 15, 13, 0), MarketDataEntry.EntryType.RAW, "uuid2222");
        marketDataEntryMock.when(() -> MarketDataEntry.fromKey(key1)).thenReturn(rawEntry1);
        marketDataEntryMock.when(() -> MarketDataEntry.fromKey(key2)).thenReturn(rawEntry2);

        List<Schema> schemas = List.of(schema(100L));
        byte[] compressedData = compressSchemas(schemas);
        when(s3Client.getObject(any(Consumer.class))).thenAnswer(invocation ->
            new ResponseInputStream<>(GetObjectResponse.builder().build(), new ByteArrayInputStream(compressedData))
        );

        setupSuccessfulS3Operations();

        // When: Running merger
        merger.runMerger();

        // Then: Verify both keys were deleted
        List<String> deletedKeys = captureDeleteObjectKeys();
        assertTrue(deletedKeys.contains(key1), "Key1 should be deleted");
        assertTrue(deletedKeys.contains(key2), "Key2 should be deleted");
    }

    // ============================================================================
    // ERROR HANDLING TESTS
    // ============================================================================

    @Test
    void testRunMergerThrowsOnUnsupportedSchemaType() {
        // Given: Raw file with unsupported schema type (MBO)
        Listing testListing = listing(532, 151, SchemaType.MBO);

        String oldKey = "532/151/2025/4/15/13/0/mbo/uuid1234.zst";
        S3Object oldObj = S3Object.builder().key(oldKey).build();

        ListObjectsV2Iterable mockIterable = mock(ListObjectsV2Iterable.class);
        ListObjectsV2Response response = ListObjectsV2Response.builder().contents(Collections.singletonList(oldObj)).build();
        when(mockIterable.stream()).thenReturn(Stream.of(response));
        when(s3Client.listObjectsV2Paginator(any(Consumer.class))).thenReturn(mockIterable);

        // Use a spy so we can mock loadFromS3 to return empty list (bypassing the schema size bug)
        MarketDataEntry rawEntry = spy(new MarketDataEntry(testListing, LocalDateTime.of(2025, 4, 15, 13, 0), MarketDataEntry.EntryType.RAW, "uuid1234"));
        doReturn(List.of()).when(rawEntry).loadFromS3(any(S3Client.class), anyString());

        marketDataEntryMock.when(() -> MarketDataEntry.fromKey(oldKey))
            .thenReturn(rawEntry);

        // When/Then: Exception is thrown for unsupported schema type
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> merger.runMerger());
        assertTrue(exception.getMessage().contains("Unsupported schema type"));
    }

    @Test
    void testRunMergerThrowsOnFailedCopyToArchive() throws IOException {
        // Given: Copy operation fails
        Listing testListing = listing(532, 151, SchemaType.MBP_10);
        String rawKey = "532/151/2025/4/15/13/0/mbp-10/uuid1234.zst";
        S3Object rawObj = S3Object.builder().key(rawKey).build();

        ListObjectsV2Iterable mockIterable = mock(ListObjectsV2Iterable.class);
        ListObjectsV2Response response = ListObjectsV2Response.builder().contents(Collections.singletonList(rawObj)).build();
        when(mockIterable.stream()).thenReturn(Stream.of(response));
        when(s3Client.listObjectsV2Paginator(any(Consumer.class))).thenReturn(mockIterable);

        MarketDataEntry rawEntry = new MarketDataEntry(testListing, LocalDateTime.of(2025, 4, 15, 13, 0), MarketDataEntry.EntryType.RAW, "uuid1234");
        marketDataEntryMock.when(() -> MarketDataEntry.fromKey(rawKey))
            .thenReturn(rawEntry);

        // Mock S3 getObject
        List<Schema> schemas = List.of(schema(100L));
        byte[] compressedData = compressSchemas(schemas);
        when(s3Client.getObject(any(Consumer.class))).thenAnswer(invocation ->
            new ResponseInputStream<>(GetObjectResponse.builder().build(), new ByteArrayInputStream(compressedData))
        );

        // Mock putObject success but copyObject failure
        PutObjectResponse putResponse = mock(PutObjectResponse.class);
        when(s3Client.putObject(any(Consumer.class), any(RequestBody.class))).thenReturn(putResponse);

        CopyObjectResponse failedCopyResponse = mock(CopyObjectResponse.class);
        when(failedCopyResponse.sdkHttpResponse()).thenReturn(mock(software.amazon.awssdk.http.SdkHttpResponse.class));
        when(failedCopyResponse.sdkHttpResponse().isSuccessful()).thenReturn(false);
        when(s3Client.copyObject(any(Consumer.class))).thenReturn(failedCopyResponse);

        // When/Then: Exception is thrown on failed copy
        assertThrows(RuntimeException.class, () -> merger.runMerger());
    }

    @Test
    void testRunMergerThrowsOnDeleteErrors() throws IOException {
        // Given: Delete operation has errors
        Listing testListing = listing(532, 151, SchemaType.MBP_10);
        String rawKey = "532/151/2025/4/15/13/0/mbp-10/uuid1234.zst";
        S3Object rawObj = S3Object.builder().key(rawKey).build();

        ListObjectsV2Iterable mockIterable = mock(ListObjectsV2Iterable.class);
        ListObjectsV2Response response = ListObjectsV2Response.builder().contents(Collections.singletonList(rawObj)).build();
        when(mockIterable.stream()).thenReturn(Stream.of(response));
        when(s3Client.listObjectsV2Paginator(any(Consumer.class))).thenReturn(mockIterable);

        MarketDataEntry rawEntry = new MarketDataEntry(testListing, LocalDateTime.of(2025, 4, 15, 13, 0), MarketDataEntry.EntryType.RAW, "uuid1234");
        marketDataEntryMock.when(() -> MarketDataEntry.fromKey(rawKey))
            .thenReturn(rawEntry);

        // Mock S3 getObject
        List<Schema> schemas = List.of(schema(100L));
        byte[] compressedData = compressSchemas(schemas);
        when(s3Client.getObject(any(Consumer.class))).thenAnswer(invocation ->
            new ResponseInputStream<>(GetObjectResponse.builder().build(), new ByteArrayInputStream(compressedData))
        );

        // Mock putObject and copyObject success
        PutObjectResponse putResponse = mock(PutObjectResponse.class);
        when(s3Client.putObject(any(Consumer.class), any(RequestBody.class))).thenReturn(putResponse);

        CopyObjectResponse copyResponse = mock(CopyObjectResponse.class);
        when(copyResponse.sdkHttpResponse()).thenReturn(mock(software.amazon.awssdk.http.SdkHttpResponse.class));
        when(copyResponse.sdkHttpResponse().isSuccessful()).thenReturn(true);
        when(s3Client.copyObject(any(Consumer.class))).thenReturn(copyResponse);

        // Mock deleteObjects failure
        DeleteObjectsResponse deleteResponse = mock(DeleteObjectsResponse.class);
        when(deleteResponse.hasErrors()).thenReturn(true);
        when(deleteResponse.errors()).thenReturn(List.of(S3Error.builder().key(rawKey).message("Delete failed").build()));
        when(s3Client.deleteObjects(any(Consumer.class))).thenReturn(deleteResponse);

        // When/Then: Exception is thrown on delete error
        assertThrows(RuntimeException.class, () -> merger.runMerger());
    }
}

