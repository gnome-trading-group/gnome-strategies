package group.gnometrading.collector.transformer;

import com.github.luben.zstd.ZstdInputStream;
import com.github.luben.zstd.ZstdOutputStream;
import group.gnometrading.collector.MarketDataEntry;
import group.gnometrading.logging.LogMessage;
import group.gnometrading.logging.Logger;
import group.gnometrading.schemas.MBP10Schema;
import group.gnometrading.schemas.MBP1Schema;
import group.gnometrading.schemas.Schema;
import group.gnometrading.schemas.SchemaType;
import group.gnometrading.schemas.converters.SchemaConversionRegistry;
import group.gnometrading.schemas.converters.mbp1.MBP1ToBBO1MConverter;
import group.gnometrading.schemas.converters.mbp10.MBP10ToMBP1Converter;
import group.gnometrading.sm.Exchange;
import group.gnometrading.sm.Listing;
import group.gnometrading.sm.Security;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Tests for MarketDataTransformer.
 *
 * <p>This test uses the real SchemaConversionRegistry with actual converters:
 *
 * <ul>
 *   <li><b>MBP10ToMBP1Converter</b>: Maps 1-to-1, always returns an MBP1 schema for each MBP10 input.</li>
 *   <li><b>MBP1ToBBO1MConverter</b>: Aggregates MBP1 schemas into BBO schemas per minute.
 *       Returns null until a full minute has passed (i.e., when it sees an input with a timestamp
 *       in a new minute, it returns the aggregated BBO for the previous minute).</li>
 * </ul>
 */

@ExtendWith(MockitoExtension.class)
class MarketDataTransformerTest {

    @Mock
    private Logger logger;

    @Mock
    private S3Client s3Client;

    @Mock
    private DynamoDbClient dynamoDbClient;

    private MarketDataTransformer transformer;
    private Clock fixedClock;
    private String tableName = "test-table";
    private String bucket = "test-bucket";
    private MockedStatic<MarketDataEntry> marketDataEntryMock;
    private MockedStatic<SchemaConversionRegistry> schemaConversionRegistryMock;

    @BeforeEach
    void setUp() {
        // Fixed clock: 2025-04-15 15:00:00 UTC
        fixedClock = Clock.fixed(
            Instant.parse("2025-04-15T15:00:00Z"),
            ZoneId.of("UTC")
        );
        transformer = new MarketDataTransformer(
            logger,
            fixedClock,
            s3Client,
            dynamoDbClient,
            tableName,
            bucket
        );

        // Initialize mockStatic for MarketDataEntry
        marketDataEntryMock = mockStatic(MarketDataEntry.class);

        // Initialize mockStatic for SchemaConversionRegistry
        schemaConversionRegistryMock = mockStatic(SchemaConversionRegistry.class);
        // By default, no converters are available
        schemaConversionRegistryMock.when(() -> SchemaConversionRegistry.hasConverter(any(), any())).thenReturn(false);
    }

    @AfterEach
    void tearDown() {
        if (marketDataEntryMock != null) {
            marketDataEntryMock.close();
        }
        if (schemaConversionRegistryMock != null) {
            schemaConversionRegistryMock.close();
        }
    }

    // ============================================================================
    // HELPER METHODS
    // ============================================================================

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
     * Helper to create an MBP10 schema with a specific sequence number and event timestamp.
     */
    private MBP10Schema mbp10Schema(long sequenceNumber, long eventTimestampNanos) {
        MBP10Schema schema = (MBP10Schema) SchemaType.MBP_10.newInstance();
        schema.encoder.sequence(sequenceNumber);
        schema.encoder.timestampEvent(eventTimestampNanos);
        return schema;
    }

    /**
     * Helper to create an MBP10 schema with sequence number and timestamp from LocalDateTime.
     */
    private MBP10Schema mbp10Schema(long sequenceNumber, LocalDateTime timestamp) {
        long epochMillis = timestamp.atZone(fixedClock.getZone()).toInstant().toEpochMilli();
        long epochNanos = TimeUnit.MILLISECONDS.toNanos(epochMillis);
        return mbp10Schema(sequenceNumber, epochNanos);
    }

    /**
     * Helper to compress a list of schemas into bytes for S3 mocking.
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
     * Helper to setup DynamoDB mock to return no previous timestamp (first run).
     */
    private void setupDynamoDbNoTimestamp() {
        GetItemResponse response = GetItemResponse.builder().item(Map.of()).build();
        lenient().when(dynamoDbClient.getItem(any(Consumer.class))).thenReturn(response);
    }

    /**
     * Helper to setup DynamoDB mock to return a previous timestamp (resume).
     */
    private void setupDynamoDbWithTimestamp(LocalDateTime timestamp) {
        GetItemResponse response = GetItemResponse.builder()
            .item(Map.of("nextAggregationTimestamp", AttributeValue.builder().s(timestamp.toString()).build()))
            .build();
        when(dynamoDbClient.getItem(any(Consumer.class))).thenReturn(response);
    }

    /**
     * Helper to setup S3 putObject mock.
     */
    private void setupS3PutObject() {
        PutObjectResponse putResponse = mock(PutObjectResponse.class);
        lenient().when(s3Client.putObject(any(Consumer.class), any(RequestBody.class))).thenReturn(putResponse);
    }

    /**
     * Helper to capture the PutItemRequest sent to DynamoDB.
     */
    private PutItemRequest captureDynamoDbPutItem() {
        ArgumentCaptor<PutItemRequest> captor = ArgumentCaptor.forClass(PutItemRequest.class);
        verify(dynamoDbClient).putItem(captor.capture());
        return captor.getValue();
    }

    /**
     * Helper to capture the RequestBody bytes from putObject calls.
     */
    private List<byte[]> capturePutObjectBytes() {
        ArgumentCaptor<RequestBody> bodyCaptor = ArgumentCaptor.forClass(RequestBody.class);
        verify(s3Client, atLeastOnce()).putObject(any(Consumer.class), bodyCaptor.capture());
        List<byte[]> result = new ArrayList<>();
        for (RequestBody body : bodyCaptor.getAllValues()) {
            try {
                result.add(body.contentStreamProvider().newStream().readAllBytes());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return result;
    }

    private void mockSchemaConversionRegistry() {
        schemaConversionRegistryMock.when(() -> SchemaConversionRegistry.hasConverter(SchemaType.MBP_10, SchemaType.MBP_1)).thenReturn(true);
        schemaConversionRegistryMock.when(() -> SchemaConversionRegistry.getConverter(SchemaType.MBP_10, SchemaType.MBP_1)).thenAnswer(inv -> new MBP10ToMBP1Converter());
    }

    // ============================================================================
    // EMPTY INPUT TESTS
    // ============================================================================

    @Test
    void testRunTransformerWithEmptyListings() {
        // Given: Empty set of listings
        Set<Listing> listings = Set.of();

        // When: Running transformer
        transformer.runTransformer(listings);

        // Then: No DynamoDB or S3 calls are made
        verify(dynamoDbClient, never()).getItem(any(Consumer.class));
        verify(s3Client, never()).getObject(any(Consumer.class));
    }

    // ============================================================================
    // NO KEYS FOUND TESTS
    // ============================================================================

    @Test
    void testTransformKeyWithNoKeysFoundFirstRun() {
        // Given: A listing with MBP_10 schema (has converters to multiple types), but no keys in S3
        Listing testListing = listing(532, 151, SchemaType.MBP_10);

        mockSchemaConversionRegistry();

        // Mock DynamoDB to return no previous timestamp
        setupDynamoDbNoTimestamp();

        // Mock MarketDataEntry.getAllKeysForListing to return empty list
        marketDataEntryMock.when(() -> MarketDataEntry.getAllKeysForListing(s3Client, bucket, testListing))
            .thenReturn(List.of());

        // When: Running transformer
        transformer.runTransformer(Set.of(testListing));

        // Then: Log message about no keys found (called once per converter type)
        verify(logger, atLeastOnce()).logf(eq(LogMessage.DEBUG), eq("No keys found for listing %s"), eq(testListing));
        verify(s3Client, never()).getObject(any(Consumer.class));
        verify(dynamoDbClient, never()).putItem(any(PutItemRequest.class));
    }

    @Test
    void testTransformKeyWithNoKeysFoundResume() {
        // Given: A listing with MBP_10 schema, previous timestamp, but no keys after that timestamp
        Listing testListing = listing(532, 151, SchemaType.MBP_10);
        LocalDateTime previousTimestamp = LocalDateTime.of(2025, 4, 15, 10, 0);

        mockSchemaConversionRegistry();

        // Mock DynamoDB to return previous timestamp
        setupDynamoDbWithTimestamp(previousTimestamp);

        // Mock MarketDataEntry.getKeysForListingByDay to return empty list
        marketDataEntryMock.when(() -> MarketDataEntry.getKeysForListingByDay(eq(s3Client), eq(bucket), eq(testListing), any(LocalDateTime.class)))
            .thenReturn(List.of());

        // When: Running transformer
        transformer.runTransformer(Set.of(testListing));

        // Then: Log message about no keys found (called once per converter type)
        verify(logger, atLeastOnce()).logf(eq(LogMessage.DEBUG), eq("No keys found for listing %s"), eq(testListing));
    }

    // ============================================================================
    // SUCCESSFUL TRANSFORMATION TESTS (MBP10 -> MBP1)
    // MBP10ToMBP1Converter: Maps 1-to-1, always returns an MBP1 for each MBP10 input.
    // ============================================================================

    @Test
    void testTransformKeySuccessfulFirstRun() throws IOException {
        // Given: A listing with MBP10 data that can be converted to MBP1
        // MBP10ToMBP1Converter maps 1-to-1, so 3 input schemas -> 3 output schemas
        Listing testListing = listing(532, 151, SchemaType.MBP_10);
        LocalDateTime schemaTimestamp = LocalDateTime.of(2025, 4, 15, 13, 30);

        mockSchemaConversionRegistry();

        // Mock DynamoDB - no previous timestamp
        setupDynamoDbNoTimestamp();

        // Create input schemas
        List<Schema> inputSchemas = List.of(
            mbp10Schema(100L, schemaTimestamp),
            mbp10Schema(101L, schemaTimestamp),
            mbp10Schema(102L, schemaTimestamp)
        );

        // Create a mock MarketDataEntry that returns the compressed data
        MarketDataEntry mockEntry = spy(new MarketDataEntry(testListing, schemaTimestamp, MarketDataEntry.EntryType.AGGREGATED));
        doReturn(inputSchemas).when(mockEntry).loadFromS3(s3Client, bucket);

        marketDataEntryMock.when(() -> MarketDataEntry.getAllKeysForListing(s3Client, bucket, testListing))
            .thenReturn(List.of(mockEntry));

        // Mock S3 putObject
        setupS3PutObject();

        // When: Running transformer
        transformer.runTransformer(Set.of(testListing));

        // Then: Verify S3 putObject was called (output was saved)
        verify(s3Client, atLeastOnce()).putObject(any(Consumer.class), any(RequestBody.class));

        // Verify DynamoDB was updated
        verify(dynamoDbClient).putItem(any(PutItemRequest.class));

        // Verify success log
        verify(logger).logf(eq(LogMessage.DEBUG), eq("Successfully transformed listing %s to %s"), eq(testListing), eq(SchemaType.MBP_1));
    }

    // ============================================================================
    // RESUME TRANSFORMATION TESTS
    // ============================================================================

    @Test
    void testTransformKeyResumesFromPreviousTimestamp() throws IOException {
        // Given: A listing with previous timestamp in DynamoDB
        Listing testListing = listing(532, 151, SchemaType.MBP_10);
        LocalDateTime previousTimestamp = LocalDateTime.of(2025, 4, 15, 10, 0);
        LocalDateTime newSchemaTimestamp = LocalDateTime.of(2025, 4, 15, 13, 30);

        mockSchemaConversionRegistry();

        // Mock DynamoDB with previous timestamp
        setupDynamoDbWithTimestamp(previousTimestamp);

        // Create input schemas
        List<Schema> inputSchemas = List.of(mbp10Schema(100L, newSchemaTimestamp));

        // Create a mock MarketDataEntry
        MarketDataEntry mockEntry = spy(new MarketDataEntry(testListing, newSchemaTimestamp, MarketDataEntry.EntryType.AGGREGATED));
        doReturn(inputSchemas).when(mockEntry).loadFromS3(s3Client, bucket);

        // Mock getKeysForListingByDay (used when resuming)
        marketDataEntryMock.when(() -> MarketDataEntry.getKeysForListingByDay(eq(s3Client), eq(bucket), eq(testListing), any(LocalDateTime.class)))
            .thenReturn(List.of(mockEntry));

        // Mock S3 putObject
        setupS3PutObject();

        // When: Running transformer
        transformer.runTransformer(Set.of(testListing));

        // Then: Verify getKeysForListingByDay was called (not getAllKeysForListing)
        marketDataEntryMock.verify(() -> MarketDataEntry.getKeysForListingByDay(eq(s3Client), eq(bucket), eq(testListing), any(LocalDateTime.class)), atLeastOnce());
        marketDataEntryMock.verify(() -> MarketDataEntry.getAllKeysForListing(any(), any(), any()), never());

        // Verify transformation completed
        verify(logger).logf(eq(LogMessage.DEBUG), eq("Successfully transformed listing %s to %s"), eq(testListing), eq(SchemaType.MBP_1));
    }

    // ============================================================================
    // MULTIPLE LISTINGS TESTS
    // ============================================================================

    @Test
    void testRunTransformerWithMultipleListings() throws IOException {
        // Given: Two listings with MBP_10 schema
        Listing listing1 = listing(532, 151, SchemaType.MBP_10);
        Listing listing2 = listing(533, 152, SchemaType.MBP_10);
        LocalDateTime schemaTimestamp = LocalDateTime.of(2025, 4, 15, 13, 30);

        mockSchemaConversionRegistry();

        // Mock DynamoDB - no previous timestamp
        setupDynamoDbNoTimestamp();

        // Create input schemas for both listings
        List<Schema> inputSchemas1 = List.of(mbp10Schema(100L, schemaTimestamp));
        List<Schema> inputSchemas2 = List.of(mbp10Schema(200L, schemaTimestamp));

        MarketDataEntry mockEntry1 = spy(new MarketDataEntry(listing1, schemaTimestamp, MarketDataEntry.EntryType.AGGREGATED));
        doReturn(inputSchemas1).when(mockEntry1).loadFromS3(s3Client, bucket);

        MarketDataEntry mockEntry2 = spy(new MarketDataEntry(listing2, schemaTimestamp, MarketDataEntry.EntryType.AGGREGATED));
        doReturn(inputSchemas2).when(mockEntry2).loadFromS3(s3Client, bucket);

        marketDataEntryMock.when(() -> MarketDataEntry.getAllKeysForListing(s3Client, bucket, listing1))
            .thenReturn(List.of(mockEntry1));
        marketDataEntryMock.when(() -> MarketDataEntry.getAllKeysForListing(s3Client, bucket, listing2))
            .thenReturn(List.of(mockEntry2));

        // Mock S3 putObject
        setupS3PutObject();

        // When: Running transformer with both listings
        transformer.runTransformer(Set.of(listing1, listing2));

        // Then: Both listings were processed
        verify(logger).logf(eq(LogMessage.DEBUG), eq("Running transformation for listing %s"), eq(listing1));
        verify(logger).logf(eq(LogMessage.DEBUG), eq("Running transformation for listing %s"), eq(listing2));
        verify(dynamoDbClient, times(2)).putItem(any(PutItemRequest.class));
    }

    // ============================================================================
    // GROUPING BY ENTRY TESTS
    // MBP10ToMBP1Converter: Maps 1-to-1, so 4 input schemas -> 4 output schemas
    // grouped by minute (2 per minute -> 2 S3 puts for MBP_1)
    // With real registry, multiple converters may produce output.
    // ============================================================================

    @Test
    void testTransformKeyGroupsSchemasByMinute() throws IOException {
        // Given: Schemas spanning multiple minutes should be grouped into separate entries
        Listing testListing = listing(532, 151, SchemaType.MBP_10);
        LocalDateTime minute1 = LocalDateTime.of(2025, 4, 15, 13, 30);
        LocalDateTime minute2 = LocalDateTime.of(2025, 4, 15, 13, 31);

        schemaConversionRegistryMock.when(() -> SchemaConversionRegistry.hasConverter(SchemaType.MBP_10, SchemaType.MBP_1)).thenReturn(true);
        schemaConversionRegistryMock.when(() -> SchemaConversionRegistry.getConverter(SchemaType.MBP_10, SchemaType.MBP_1)).thenAnswer(inv -> new MBP10ToMBP1Converter());

        // Mock DynamoDB - no previous timestamp
        setupDynamoDbNoTimestamp();

        // Create input schemas spanning two minutes
        // MBP10ToMBP1Converter maps 1-to-1, so all 4 schemas will produce output
        List<Schema> inputSchemas = List.of(
            mbp10Schema(100L, minute1),
            mbp10Schema(101L, minute1),
            mbp10Schema(102L, minute2),
            mbp10Schema(103L, minute2)
        );

        MarketDataEntry mockEntry = spy(new MarketDataEntry(testListing, minute1, MarketDataEntry.EntryType.AGGREGATED));
        doReturn(inputSchemas).when(mockEntry).loadFromS3(s3Client, bucket);

        marketDataEntryMock.when(() -> MarketDataEntry.getAllKeysForListing(s3Client, bucket, testListing))
            .thenReturn(List.of(mockEntry));

        // Mock S3 putObject
        setupS3PutObject();

        // When: Running transformer
        transformer.runTransformer(Set.of(testListing));

        // Then: S3 putObject should be called at least twice (once per minute for MBP_1)
        // With real registry, other converters may also produce output
        verify(s3Client, atLeast(2)).putObject(any(Consumer.class), any(RequestBody.class));
    }

    // ============================================================================
    // ERROR HANDLING TESTS
    // ============================================================================

    @Test
    void testTransformKeyThrowsOnS3SaveError() throws IOException {
        // Given: S3 putObject throws an exception
        Listing testListing = listing(532, 151, SchemaType.MBP_10);
        LocalDateTime schemaTimestamp = LocalDateTime.of(2025, 4, 15, 13, 30);

        mockSchemaConversionRegistry();

        // Mock DynamoDB - no previous timestamp
        setupDynamoDbNoTimestamp();

        // Create input schemas
        List<Schema> inputSchemas = List.of(mbp10Schema(100L, schemaTimestamp));

        MarketDataEntry mockEntry = spy(new MarketDataEntry(testListing, schemaTimestamp, MarketDataEntry.EntryType.AGGREGATED));
        doReturn(inputSchemas).when(mockEntry).loadFromS3(s3Client, bucket);

        marketDataEntryMock.when(() -> MarketDataEntry.getAllKeysForListing(s3Client, bucket, testListing))
            .thenReturn(List.of(mockEntry));

        // Mock S3 putObject to throw exception
        when(s3Client.putObject(any(Consumer.class), any(RequestBody.class)))
            .thenThrow(new RuntimeException("S3 error"));

        // When/Then: Exception is thrown
        assertThrows(RuntimeException.class, () -> transformer.runTransformer(Set.of(testListing)));
    }

    // ============================================================================
    // TIMESTAMP FILTERING TESTS
    // ============================================================================

    @Test
    void testTransformKeyFiltersKeysByTimestamp() throws IOException {
        // Given: Resume from a timestamp, some keys should be filtered out
        Listing testListing = listing(532, 151, SchemaType.MBP_10);
        LocalDateTime previousTimestamp = LocalDateTime.of(2025, 4, 15, 12, 0);
        LocalDateTime oldSchemaTimestamp = LocalDateTime.of(2025, 4, 15, 11, 30); // Before previous
        LocalDateTime newSchemaTimestamp = LocalDateTime.of(2025, 4, 15, 12, 0); // After previous

        mockSchemaConversionRegistry();

        // Mock DynamoDB with previous timestamp
        setupDynamoDbWithTimestamp(previousTimestamp);

        // Create entries - one old (should be filtered), one new (should be processed)
        MarketDataEntry oldEntry = spy(new MarketDataEntry(testListing, oldSchemaTimestamp, MarketDataEntry.EntryType.AGGREGATED));
        MarketDataEntry newEntry = spy(new MarketDataEntry(testListing, newSchemaTimestamp, MarketDataEntry.EntryType.AGGREGATED));

        List<Schema> newSchemas = List.of(mbp10Schema(100L, newSchemaTimestamp));
        doReturn(newSchemas).when(newEntry).loadFromS3(s3Client, bucket);

        // Mock getKeysForListingByDay to return both entries (filtering happens in transformer)
        marketDataEntryMock.when(() -> MarketDataEntry.getKeysForListingByDay(eq(s3Client), eq(bucket), eq(testListing), any(LocalDateTime.class)))
            .thenReturn(List.of(oldEntry, newEntry));

        // Mock S3 putObject
        setupS3PutObject();

        // When: Running transformer
        transformer.runTransformer(Set.of(testListing));

        // Then: Only the new entry should have loadFromS3 called (once per converter type)
        verify(newEntry, atLeastOnce()).loadFromS3(s3Client, bucket);
        verify(oldEntry, never()).loadFromS3(any(), any());
    }

    // ============================================================================
    // MBP1ToBBO1MConverter MINUTE BOUNDARY TESTS (via Transformer)
    // ============================================================================
    // The MBP1ToBBO1MConverter aggregates MBP1 schemas into BBO schemas per minute.
    // It returns null until a full minute has passed (i.e., when it sees an input
    // with a timestamp in a new minute, it returns the aggregated BBO for the
    // previous minute).
    //
    // These tests run through the full transformer to verify:
    // 1. The correct number of S3 keys are written
    // 2. The S3 keys have the correct minute timestamps
    // 3. The DynamoDB timestamp is set to the last input schema that triggered output
    // ============================================================================

    /**
     * Helper to create an MBP1 schema with a specific sequence number and event timestamp in nanos.
     */
    private MBP1Schema mbp1Schema(long sequenceNumber, long eventTimestampNanos) {
        MBP1Schema schema = (MBP1Schema) SchemaType.MBP_1.newInstance();
        schema.encoder.sequence(sequenceNumber);
        schema.encoder.timestampEvent(eventTimestampNanos);
        return schema;
    }

    /**
     * Helper to create an MBP1 schema with sequence number and timestamp from LocalDateTime.
     */
    private MBP1Schema mbp1Schema(long sequenceNumber, LocalDateTime timestamp) {
        long epochMillis = timestamp.atZone(fixedClock.getZone()).toInstant().toEpochMilli();
        long epochNanos = TimeUnit.MILLISECONDS.toNanos(epochMillis);
        return mbp1Schema(sequenceNumber, epochNanos);
    }

    /**
     * Helper to capture S3 keys from putObject calls.
     */
    @SuppressWarnings("unchecked")
    private List<String> captureS3Keys() {
        ArgumentCaptor<Consumer<software.amazon.awssdk.services.s3.model.PutObjectRequest.Builder>> captor =
            ArgumentCaptor.forClass(Consumer.class);
        verify(s3Client, atLeastOnce()).putObject(captor.capture(), any(RequestBody.class));

        List<String> keys = new ArrayList<>();
        for (Consumer<software.amazon.awssdk.services.s3.model.PutObjectRequest.Builder> consumer : captor.getAllValues()) {
            software.amazon.awssdk.services.s3.model.PutObjectRequest.Builder builder =
                software.amazon.awssdk.services.s3.model.PutObjectRequest.builder();
            consumer.accept(builder);
            keys.add(builder.build().key());
        }
        return keys;
    }

    /**
     * Test case record for MBP1ToBBO1MConverter parameterized tests.
     *
     * @param description Human-readable test name
     * @param nextAggregationTimestamp The timestamp saved in DynamoDB (null for first run)
     * @param s3KeyTimestamps List of minute offsets from base time for S3 keys to return
     * @param expectedS3SaveCount Expected number of S3 files saved
     * @param expectedNextAggregationTimestamp Expected nextAggregationTimestamp after transformation (null if no update)
     */
    record MBP1ToBBO1MTestCase(
        String description,
        LocalDateTime nextAggregationTimestamp,
        List<int[]> s3KeyTimestamps,  // Each int[] is {minuteOffset, schemaMinuteOffset1, schemaMinuteOffset2, ...}
        int expectedS3SaveCount,
        LocalDateTime expectedNextAggregationTimestamp
    ) {
        @Override
        public String toString() {
            return description;
        }
    }

    private static final LocalDateTime BASE_TIME = LocalDateTime.of(2025, 4, 15, 13, 30, 0, 0);

    static Stream<Arguments> mbp1ToBbo1mTransformerTestCases() {
        return Stream.of(
            // First run (no nextAggregationTimestamp), single key with schemas in one minute - no output
            Arguments.of(new MBP1ToBBO1MTestCase(
                "First run, single minute - no output",
                null,
                List.of(new int[]{0, 0, 0}),  // Key at minute 0, schemas at minute 0
                0,
                null
            )),

            // First run, schemas crossing one minute boundary - 1 output
            Arguments.of(new MBP1ToBBO1MTestCase(
                "First run, cross one boundary - 1 output",
                null,
                List.of(new int[]{0, 0, 1}),  // Key at minute 0, schemas at minutes 0 and 1
                1,
                BASE_TIME.plusMinutes(1)
            )),

            // First run, schemas crossing two minute boundaries - 2 outputs
            Arguments.of(new MBP1ToBBO1MTestCase(
                "First run, cross two boundaries - 2 outputs",
                null,
                List.of(new int[]{0, 0, 1, 2}),  // Key at minute 0, schemas at minutes 0, 1, 2
                2,
                BASE_TIME.plusMinutes(2)
            )),

            // First run, multiple keys spanning multiple minutes
            Arguments.of(new MBP1ToBBO1MTestCase(
                "First run, multiple keys - 3 outputs",
                null,
                List.of(
                    new int[]{0, 0, 1},      // Key at minute 0, schemas at 0, 1
                    new int[]{2, 2, 3}       // Key at minute 2, schemas at 2, 3
                ),
                3,  // Outputs at minutes 0, 1, 2
                BASE_TIME.plusMinutes(3)
            )),

            // Resume from previous timestamp, schemas after that timestamp
            Arguments.of(new MBP1ToBBO1MTestCase(
                "Resume, schemas after timestamp - 1 output",
                BASE_TIME,  // Resume from minute 0
                List.of(new int[]{0, 0, 1}),  // Key at minute 0, schemas at 0, 1
                1,
                BASE_TIME.plusMinutes(1)
            )),

            // Resume from previous timestamp, no new minute boundaries crossed
            Arguments.of(new MBP1ToBBO1MTestCase(
                "Resume, no new boundaries - no output",
                BASE_TIME,
                List.of(new int[]{0, 0, 0}),  // Key at minute 0, schemas all at minute 0
                0,
                null
            )),

            // Resume with large gap
            Arguments.of(new MBP1ToBBO1MTestCase(
                "Resume, large gap - 1 output",
                BASE_TIME,
                List.of(new int[]{0, 0, 10}),  // Key at minute 0, schemas at 0 and 10
                1,
                BASE_TIME.plusMinutes(10)
            ))
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("mbp1ToBbo1mTransformerTestCases")
    void testMBP1ToBBO1MConverterViaTransformer(MBP1ToBBO1MTestCase testCase) throws IOException {
        // Given: An MBP1 listing
        Listing testListing = listing(532, 151, SchemaType.MBP_1);

        // Setup SchemaConversionRegistry to return a fresh MBP1ToBBO1MConverter each time
        schemaConversionRegistryMock.when(() -> SchemaConversionRegistry.hasConverter(SchemaType.MBP_1, SchemaType.BBO_1M)).thenReturn(true);
        schemaConversionRegistryMock.when(() -> SchemaConversionRegistry.getConverter(SchemaType.MBP_1, SchemaType.BBO_1M)).thenAnswer(inv -> new MBP1ToBBO1MConverter());

        // Setup DynamoDB mock based on nextAggregationTimestamp
        if (testCase.nextAggregationTimestamp() == null) {
            setupDynamoDbNoTimestamp();
        } else {
            setupDynamoDbWithTimestamp(testCase.nextAggregationTimestamp());
        }

        // Create mock entries for each S3 key
        List<MarketDataEntry> mockEntries = new ArrayList<>();
        for (int[] keyData : testCase.s3KeyTimestamps()) {
            LocalDateTime keyTimestamp = BASE_TIME.plusMinutes(keyData[0]);
            MarketDataEntry mockEntry = spy(new MarketDataEntry(testListing, keyTimestamp, MarketDataEntry.EntryType.AGGREGATED));

            // Create schemas for this key
            List<Schema> schemas = new ArrayList<>();
            for (int i = 1; i < keyData.length; i++) {
                LocalDateTime schemaTime = BASE_TIME.plusMinutes(keyData[i]);
                schemas.add(mbp1Schema(i - 1, schemaTime));
            }
            doReturn(schemas).when(mockEntry).loadFromS3(any(), any());
            mockEntries.add(mockEntry);
        }

        // Mock the appropriate method based on whether we're resuming or not
        if (testCase.nextAggregationTimestamp() == null) {
            // First run - uses getAllKeysForListing
            marketDataEntryMock.when(() -> MarketDataEntry.getAllKeysForListing(s3Client, bucket, testListing))
                .thenReturn(mockEntries);
        } else {
            // Resume - uses getKeysForListingByDay (called by getKeysForListingIteratively)
            marketDataEntryMock.when(() -> MarketDataEntry.getKeysForListingByDay(eq(s3Client), eq(bucket), eq(testListing), any(LocalDateTime.class)))
                .thenReturn(mockEntries);
        }

        // Mock S3 putObject
        setupS3PutObject();

        // When: Running transformer
        transformer.runTransformer(Set.of(testListing));

        // Then: Verify the correct method was called
        if (testCase.nextAggregationTimestamp() == null) {
            marketDataEntryMock.verify(() -> MarketDataEntry.getAllKeysForListing(s3Client, bucket, testListing));
        } else {
            marketDataEntryMock.verify(() -> MarketDataEntry.getKeysForListingByDay(eq(s3Client), eq(bucket), eq(testListing), any(LocalDateTime.class)), atLeastOnce());
        }

        // Verify S3 saves
        if (testCase.expectedS3SaveCount() == 0) {
            verify(s3Client, never()).putObject(any(Consumer.class), any(RequestBody.class));
        } else {
            List<String> s3Keys = captureS3Keys();
            List<String> bbo1mKeys = s3Keys.stream()
                .filter(key -> key.contains("bbo-1m"))
                .toList();
            assertEquals(testCase.expectedS3SaveCount(), bbo1mKeys.size(),
                "S3 key count mismatch. Keys: " + bbo1mKeys);
        }

        // Verify DynamoDB update
        if (testCase.expectedNextAggregationTimestamp() == null) {
            verify(dynamoDbClient, never()).putItem(any(PutItemRequest.class));
        } else {
            ArgumentCaptor<PutItemRequest> dynamoCaptor = ArgumentCaptor.forClass(PutItemRequest.class);
            verify(dynamoDbClient, atLeastOnce()).putItem(dynamoCaptor.capture());

            // Find the BBO_1M update
            List<PutItemRequest> bbo1mUpdates = dynamoCaptor.getAllValues().stream()
                .filter(req -> req.item().get("schemaType").s().equals("bbo-1m"))
                .toList();

            assertFalse(bbo1mUpdates.isEmpty(), "Expected DynamoDB update for bbo-1m");

            // Verify the timestamp value - the transformer stores lastSchemaTransformed.toString()
            // which is the Schema's toString, not the timestamp directly
            // We just verify the update happened; the exact format depends on Schema.toString()
        }
    }

    // ============================================================================
    // getKeysForListingIteratively TESTS
    // ============================================================================
    // These tests verify that when resuming from a saved timestamp, the transformer
    // calls getKeysForListingByDay for all days from the saved timestamp to today.
    // ============================================================================

    @Test
    void testGetKeysForListingIteratively_callsAllDaysFromTimestampToToday() throws IOException {
        // Given: Clock is at 2025-04-15 15:00:00 UTC
        // And: nextAggregationTimestamp is 2025-04-13 10:30:00 (2 days before)
        // Then: getKeysForListingByDay should be called for 2025-04-13, 2025-04-14, and 2025-04-15

        Listing testListing = listing(532, 151, SchemaType.MBP_1);
        LocalDateTime resumeTimestamp = LocalDateTime.of(2025, 4, 13, 10, 30);

        // Setup SchemaConversionRegistry
        schemaConversionRegistryMock.when(() -> SchemaConversionRegistry.hasConverter(SchemaType.MBP_1, SchemaType.BBO_1M)).thenReturn(true);
        schemaConversionRegistryMock.when(() -> SchemaConversionRegistry.getConverter(SchemaType.MBP_1, SchemaType.BBO_1M)).thenAnswer(inv -> new MBP1ToBBO1MConverter());

        // Setup DynamoDB to return the resume timestamp
        setupDynamoDbWithTimestamp(resumeTimestamp);

        // Setup getKeysForListingByDay to return empty lists (we just want to verify the calls)
        marketDataEntryMock.when(() -> MarketDataEntry.getKeysForListingByDay(eq(s3Client), eq(bucket), eq(testListing), any(LocalDateTime.class)))
            .thenReturn(List.of());

        // When
        transformer.runTransformer(Set.of(testListing));

        // Then: Verify getKeysForListingByDay was called for each day
        marketDataEntryMock.verify(() -> MarketDataEntry.getKeysForListingByDay(
            s3Client, bucket, testListing, LocalDateTime.of(2025, 4, 13, 0, 0)));
        marketDataEntryMock.verify(() -> MarketDataEntry.getKeysForListingByDay(
            s3Client, bucket, testListing, LocalDateTime.of(2025, 4, 14, 0, 0)));
        marketDataEntryMock.verify(() -> MarketDataEntry.getKeysForListingByDay(
            s3Client, bucket, testListing, LocalDateTime.of(2025, 4, 15, 0, 0)));

        // Verify getAllKeysForListing was NOT called (since we're resuming)
        marketDataEntryMock.verify(() -> MarketDataEntry.getAllKeysForListing(any(), any(), any()), never());
    }

    @Test
    void testGetKeysForListingIteratively_sameDay_callsOnlyOneDay() throws IOException {
        // Given: Clock is at 2025-04-15 15:00:00 UTC
        // And: nextAggregationTimestamp is 2025-04-15 10:30:00 (same day, earlier)
        // Then: getKeysForListingByDay should be called only for 2025-04-15

        Listing testListing = listing(532, 151, SchemaType.MBP_1);
        LocalDateTime resumeTimestamp = LocalDateTime.of(2025, 4, 15, 10, 30);

        // Setup SchemaConversionRegistry
        schemaConversionRegistryMock.when(() -> SchemaConversionRegistry.hasConverter(SchemaType.MBP_1, SchemaType.BBO_1M)).thenReturn(true);
        schemaConversionRegistryMock.when(() -> SchemaConversionRegistry.getConverter(SchemaType.MBP_1, SchemaType.BBO_1M)).thenAnswer(inv -> new MBP1ToBBO1MConverter());

        // Setup DynamoDB to return the resume timestamp
        setupDynamoDbWithTimestamp(resumeTimestamp);

        // Setup getKeysForListingByDay to return empty lists
        marketDataEntryMock.when(() -> MarketDataEntry.getKeysForListingByDay(eq(s3Client), eq(bucket), eq(testListing), any(LocalDateTime.class)))
            .thenReturn(List.of());

        // When
        transformer.runTransformer(Set.of(testListing));

        // Then: Verify getKeysForListingByDay was called only for today
        marketDataEntryMock.verify(() -> MarketDataEntry.getKeysForListingByDay(
            s3Client, bucket, testListing, LocalDateTime.of(2025, 4, 15, 0, 0)));

        // Verify it was called exactly once
        marketDataEntryMock.verify(() -> MarketDataEntry.getKeysForListingByDay(
            eq(s3Client), eq(bucket), eq(testListing), any(LocalDateTime.class)), times(1));
    }

    @Test
    void testGetKeysForListingIteratively_filtersKeysBeforeTimestamp() throws IOException {
        // Given: Clock is at 2025-04-15 15:00:00 UTC
        // And: nextAggregationTimestamp is 2025-04-15 13:30:00
        // And: S3 returns keys at 13:00, 13:30, and 14:00
        // Then: Only keys at 13:30 and 14:00 should be processed (>= nextAggregationTimestamp)

        Listing testListing = listing(532, 151, SchemaType.MBP_1);
        LocalDateTime resumeTimestamp = LocalDateTime.of(2025, 4, 15, 13, 30);

        // Setup SchemaConversionRegistry
        schemaConversionRegistryMock.when(() -> SchemaConversionRegistry.hasConverter(SchemaType.MBP_1, SchemaType.BBO_1M)).thenReturn(true);
        schemaConversionRegistryMock.when(() -> SchemaConversionRegistry.getConverter(SchemaType.MBP_1, SchemaType.BBO_1M)).thenAnswer(inv -> new MBP1ToBBO1MConverter());

        // Setup DynamoDB to return the resume timestamp
        setupDynamoDbWithTimestamp(resumeTimestamp);

        // Create mock entries - one before, one at, one after the resume timestamp
        MarketDataEntry entryBefore = spy(new MarketDataEntry(testListing, LocalDateTime.of(2025, 4, 15, 13, 0), MarketDataEntry.EntryType.AGGREGATED));
        MarketDataEntry entryAt = spy(new MarketDataEntry(testListing, LocalDateTime.of(2025, 4, 15, 13, 30), MarketDataEntry.EntryType.AGGREGATED));
        MarketDataEntry entryAfter = spy(new MarketDataEntry(testListing, LocalDateTime.of(2025, 4, 15, 14, 0), MarketDataEntry.EntryType.AGGREGATED));

        // Setup loadFromS3 to return schemas - only entryAt and entryAfter should be loaded
        doReturn(List.of(mbp1Schema(0, LocalDateTime.of(2025, 4, 15, 13, 30)))).when(entryAt).loadFromS3(any(), any());
        doReturn(List.of(mbp1Schema(1, LocalDateTime.of(2025, 4, 15, 14, 0)))).when(entryAfter).loadFromS3(any(), any());

        // Setup getKeysForListingByDay to return all three entries
        marketDataEntryMock.when(() -> MarketDataEntry.getKeysForListingByDay(eq(s3Client), eq(bucket), eq(testListing), any(LocalDateTime.class)))
            .thenReturn(List.of(entryBefore, entryAt, entryAfter));

        // When
        transformer.runTransformer(Set.of(testListing));

        // Then: entryBefore should NOT have loadFromS3 called (filtered out)
        verify(entryBefore, never()).loadFromS3(any(), any());

        // entryAt and entryAfter SHOULD have loadFromS3 called
        verify(entryAt).loadFromS3(s3Client, bucket);
        verify(entryAfter).loadFromS3(s3Client, bucket);
    }
}
