package group.gnometrading.collector;

import com.github.luben.zstd.ZstdInputStream;
import com.github.luben.zstd.ZstdOutputStream;
import group.gnometrading.logging.NullLogger;
import group.gnometrading.schemas.MBP10Schema;
import group.gnometrading.schemas.MBP1Schema;
import group.gnometrading.schemas.Schema;
import group.gnometrading.schemas.SchemaType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.cloudwatch.model.PutMetricDataRequest;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Whoever you are reading this class, if you have to edit this, I apologize. Go use AI or something.
 */
@ExtendWith(MockitoExtension.class)
@Disabled
class MarketDataAggregatorTest {

    private static final String INPUT = "input";
    private static final String OUTPUT = "output";

    @Mock
    S3Client s3Client;

    @Mock
    CloudWatchClient cloudWatchClient;

    MarketDataAggregator aggregator;

    @BeforeEach
    void setup() {
        reset(s3Client, cloudWatchClient);
        this.aggregator = new MarketDataAggregator(new NullLogger(), s3Client, cloudWatchClient, INPUT, OUTPUT);
    }

    // Generate test data statically to avoid schema state issues
    private static final S3Data TEST2_COLLECTOR0 = collector(3, 1, "202504011200", SchemaType.MBP_10, 0,
            record(100, 0x1));
    private static final S3Data TEST3_COLLECTOR0 = collector(3, 1, "202504011100", SchemaType.MBP_10, 0,
            record(100, 0x1));
    private static final S3Data TEST3_COLLECTOR1 = collector(3, 1, "202504011100", SchemaType.MBP_10, 1,
            record(100, 0x1));
    private static final S3Data TEST4_COLLECTOR0 = collector(3, 5, "202504011200", SchemaType.MBP_1, 0,
            record(50, 0x1), record(51, 0x2));
    private static final S3Data TEST4_COLLECTOR1 = collector(3, 5, "202504011200", SchemaType.MBP_1, 1,
            record(50, 0x1));
    private static final S3Data TEST5_COLLECTOR0 = collector(6, 0, "202404011200", SchemaType.MBO, 0,
            record(101, 0x2), record(101, 0x2), record(102, 0x4));
    private static final S3Data TEST5_COLLECTOR1 = collector(6, 0, "202404011200", SchemaType.MBO, 1,
            record(101, 0x2));
    private static final S3Data TEST5_EXPECTED = collector(6, 0, "202404011200", SchemaType.MBO, 0,
            record(101, 0x2), record(102, 0x4));
    private static final S3Data TEST6_COLLECTOR0_EX0 = collector(6, 0, "202404011200", SchemaType.MBO, 0,
            record(101, 0x5), record(102, 0x2));
    private static final S3Data TEST6_COLLECTOR1_EX1 = collector(6, 1, "202404011200", SchemaType.MBO, 1,
            record(101, 0x5));
    private static final S3Data TEST7_COLLECTOR0_T1200 = collector(6, 0, "202404011200", SchemaType.MBO, 0,
            record(101, 0x5), record(102, 0x2));
    private static final S3Data TEST7_COLLECTOR0_T1300 = collector(6, 0, "202404011300", SchemaType.MBO, 3,
            record(101, 0x5), record(102, 0x2));
    private static final S3Data TEST7_COLLECTOR1_T1200 = collector(6, 1, "202404011200", SchemaType.MBO, 1,
            record(101, 0x5));

    // Test 8: Three collectors, one missing multiple records
    private static final S3Data TEST8_COLLECTOR0 = collector(10, 2, "202505011400", SchemaType.MBP_1, 0,
            record(200, 0xA), record(201, 0xB), record(202, 0xC), record(203, 0xD));
    private static final S3Data TEST8_COLLECTOR1 = collector(10, 2, "202505011400", SchemaType.MBP_1, 1,
            record(200, 0xA), record(201, 0xB), record(202, 0xC), record(203, 0xD));
    private static final S3Data TEST8_COLLECTOR2 = collector(10, 2, "202505011400", SchemaType.MBP_1, 2,
            record(200, 0xA)); // Missing 3 records

    // Test 9: Multiple duplicates at different sequence numbers
    private static final S3Data TEST9_COLLECTOR0 = collector(7, 3, "202506011500", SchemaType.MBO, 0,
            record(300, 0x10), record(300, 0x10), record(301, 0x11), record(301, 0x11), record(302, 0x12));
    private static final S3Data TEST9_COLLECTOR1 = collector(7, 3, "202506011500", SchemaType.MBO, 1,
            record(300, 0x10), record(301, 0x11), record(302, 0x12));
    private static final S3Data TEST9_EXPECTED = collector(7, 3, "202506011500", SchemaType.MBO, 0,
            record(300, 0x10), record(301, 0x11), record(302, 0x12));

    // Test 10: Missing records at different sequence numbers (gaps)
    private static final S3Data TEST10_COLLECTOR0 = collector(8, 4, "202507011600", SchemaType.MBP_10, 0,
            record(400, 0x20), record(402, 0x22), record(404, 0x24));
    private static final S3Data TEST10_COLLECTOR1 = collector(8, 4, "202507011600", SchemaType.MBP_10, 1,
            record(401, 0x21), record(403, 0x23), record(405, 0x25));
    private static final S3Data TEST10_EXPECTED = collector(8, 4, "202507011600", SchemaType.MBP_10, 0,
            record(400, 0x20), record(401, 0x21), record(402, 0x22), record(403, 0x23), record(404, 0x24), record(405, 0x25));

    // Test 11: Same sequence number, different content (missing records)
    private static final S3Data TEST11_COLLECTOR0 = collector(9, 5, "202508011700", SchemaType.MBO, 0,
            record(500, 0x30), record(500, 0x31)); // Two different records at seq=500
    private static final S3Data TEST11_COLLECTOR1 = collector(9, 5, "202508011700", SchemaType.MBO, 1,
            record(500, 0x30)); // Only has one of them
    private static final S3Data TEST11_EXPECTED = collector(9, 5, "202508011700", SchemaType.MBO, 0,
            record(500, 0x30), record(500, 0x31));

    // Test 12: All collectors have different subsets (complex missing pattern)
    private static final S3Data TEST12_COLLECTOR0 = collector(11, 6, "202509011800", SchemaType.MBP_1, 0,
            record(600, 0x40), record(601, 0x41));
    private static final S3Data TEST12_COLLECTOR1 = collector(11, 6, "202509011800", SchemaType.MBP_1, 1,
            record(601, 0x41), record(602, 0x42));
    private static final S3Data TEST12_COLLECTOR2 = collector(11, 6, "202509011800", SchemaType.MBP_1, 2,
            record(600, 0x40), record(602, 0x42));
    private static final S3Data TEST12_EXPECTED = collector(11, 6, "202509011800", SchemaType.MBP_1, 0,
            record(600, 0x40), record(601, 0x41), record(602, 0x42));

    // Test 13: High duplicate count (one record appears many times)
    private static final S3Data TEST13_COLLECTOR0 = collector(12, 7, "202510011900", SchemaType.MBO, 0,
            record(700, 0x50), record(700, 0x50), record(700, 0x50), record(700, 0x50));
    private static final S3Data TEST13_COLLECTOR1 = collector(12, 7, "202510011900", SchemaType.MBO, 1,
            record(700, 0x50));
    private static final S3Data TEST13_EXPECTED = collector(12, 7, "202510011900", SchemaType.MBO, 0,
            record(700, 0x50));

    // Test 14: Mixed scenario - duplicates, missing, and correct records
    private static final S3Data TEST14_COLLECTOR0 = collector(13, 8, "202511012000", SchemaType.MBP_10, 0,
            record(800, 0x60), record(800, 0x60), record(801, 0x61), record(803, 0x63));
    private static final S3Data TEST14_COLLECTOR1 = collector(13, 8, "202511012000", SchemaType.MBP_10, 1,
            record(800, 0x60), record(802, 0x62), record(803, 0x63));
    private static final S3Data TEST14_EXPECTED = collector(13, 8, "202511012000", SchemaType.MBP_10, 0,
            record(800, 0x60), record(801, 0x61), record(802, 0x62), record(803, 0x63));

    // Test 15: Large sequence number gap
    private static final S3Data TEST15_COLLECTOR0 = collector(14, 9, "202512012100", SchemaType.MBO, 0,
            record(1000, 0x70), record(10000, 0x71));
    private static final S3Data TEST15_COLLECTOR1 = collector(14, 9, "202512012100", SchemaType.MBO, 1,
            record(1000, 0x70), record(10000, 0x71));

    // Test 16: Single collector with duplicates (no redundancy from other collectors)
    private static final S3Data TEST16_COLLECTOR0 = collector(15, 10, "202601012200", SchemaType.MBP_1, 0,
            record(1100, 0x80), record(1100, 0x80), record(1101, 0x81));
    private static final S3Data TEST16_EXPECTED = collector(15, 10, "202601012200", SchemaType.MBP_1, 0,
            record(1100, 0x80), record(1101, 0x81));

    private static Stream<Arguments> testMarketDataAggregatorArgs() {
        return Stream.of(
                // Test 1: Empty input
                Arguments.of(
                        new ArrayList<S3Data>(),
                        new ArrayList<S3Data>(),
                        List.of()
                ),

                // Test 2: Single collector, single record
                // Collector 0: seq=100, seed=0x1
                // Expected: 1 unique record, 0 missing, 0 duplicates
                Arguments.of(
                        files(TEST2_COLLECTOR0),
                        files(TEST2_COLLECTOR0),
                        List.of(stats(1, 1, 0, 0))
                ),

                // Test 3: Two collectors, both captured same record (perfect redundancy)
                // Collector 0: seq=100, seed=0x1
                // Collector 1: seq=100, seed=0x1 (duplicate)
                // Expected: 1 unique record, 0 missing, 0 duplicates (count=2 is expected for 2 collectors)
                Arguments.of(
                        files(TEST3_COLLECTOR0, TEST3_COLLECTOR1),
                        files(TEST3_COLLECTOR0),
                        List.of(stats(2, 1, 0, 0))
                ),

                // Test 4: Two collectors, one has 2 records, other has 1 (missing record)
                // Collector 0: seq=50, seed=0x1; seq=51, seed=0x2
                // Collector 1: seq=50, seed=0x1
                // Expected: 2 unique records, 1 missing (seq=51 only in collector 0), 0 duplicates
                Arguments.of(
                        files(TEST4_COLLECTOR0, TEST4_COLLECTOR1),
                        files(TEST4_COLLECTOR0),
                        List.of(stats(3, 2, 1, 0))
                ),

                // Test 5: Duplicate detection - same sequence appears 3 times in one collector
                // Collector 0: seq=101, seed=0x2; seq=101, seed=0x2 (dup); seq=102, seed=0x4
                // Collector 1: seq=101, seed=0x2
                // Expected: 2 unique records, 1 missing (seq=102), 1 duplicate (seq=101 appears 3 times total, expected 2)
                Arguments.of(
                        files(TEST5_COLLECTOR0, TEST5_COLLECTOR1),
                        files(TEST5_EXPECTED),
                        List.of(stats(4, 2, 1, 1))
                ),

                // Test 6: Multiple exchanges (different MarketDataKeys)
                // Exchange 0: Collector 0 has seq=101,102; Exchange 1: Collector 1 has seq=101
                Arguments.of(
                        files(TEST6_COLLECTOR0_EX0, TEST6_COLLECTOR1_EX1),
                        files(TEST6_COLLECTOR0_EX0, TEST6_COLLECTOR1_EX1),
                        List.of(
                                stats(2, 2, 0, 0),  // Exchange 0
                                stats(1, 1, 0, 0)   // Exchange 1
                        )
                ),

                // Test 7: Multiple collectors, multiple timestamps
                // All same exchange/security, but different timestamps create different keys
                Arguments.of(
                        files(TEST7_COLLECTOR0_T1200, TEST7_COLLECTOR0_T1300, TEST7_COLLECTOR1_T1200),
                        files(TEST7_COLLECTOR0_T1200, TEST7_COLLECTOR0_T1300, TEST7_COLLECTOR1_T1200),
                        List.of(
                                stats(2, 2, 0, 0),  // 202404011200, exchange 0
                                stats(2, 2, 0, 0),  // 202404011300, exchange 0
                                stats(1, 1, 0, 0)   // 202404011200, exchange 1
                        )
                ),

                // Test 8: Three collectors, one missing multiple records
                // Collector 0: seq=200,201,202,203
                // Collector 1: seq=200,201,202,203
                // Collector 2: seq=200 only (missing 3 records)
                // Expected: 4 unique, 3 missing, 0 duplicates
                Arguments.of(
                        files(TEST8_COLLECTOR0, TEST8_COLLECTOR1, TEST8_COLLECTOR2),
                        files(TEST8_COLLECTOR0),
                        List.of(stats(9, 4, 3, 0))
                ),

                // Test 9: Multiple duplicates at different sequence numbers
                // Collector 0: seq=300 (2x), seq=301 (2x), seq=302
                // Collector 1: seq=300, seq=301, seq=302
                // Expected: 3 unique, 0 missing, 2 duplicates (seq=300 and seq=301 each appear 3 times, expected 2)
                Arguments.of(
                        files(TEST9_COLLECTOR0, TEST9_COLLECTOR1),
                        files(TEST9_EXPECTED),
                        List.of(stats(8, 3, 0, 2))
                ),

                // Test 10: Missing records at different sequence numbers (interleaved gaps)
                // Collector 0: seq=400,402,404 (even sequences)
                // Collector 1: seq=401,403,405 (odd sequences)
                // Expected: 6 unique, 6 missing (each collector missing 3), 0 duplicates
                Arguments.of(
                        files(TEST10_COLLECTOR0, TEST10_COLLECTOR1),
                        files(TEST10_EXPECTED),
                        List.of(stats(6, 6, 6, 0))
                ),

                // Test 11: Same sequence number, different content (missing records)
                // Collector 0: seq=500 with seed=0x30, seq=500 with seed=0x31
                // Collector 1: seq=500 with seed=0x30 only
                // Expected: 2 unique, 1 missing (seq=500 with seed=0x31), 0 duplicates
                Arguments.of(
                        files(TEST11_COLLECTOR0, TEST11_COLLECTOR1),
                        files(TEST11_EXPECTED),
                        List.of(stats(3, 2, 1, 0))
                ),

                // Test 12: All collectors have different subsets (complex missing pattern)
                // Collector 0: seq=600,601
                // Collector 1: seq=601,602
                // Collector 2: seq=600,602
                // Expected: 3 unique, 3 missing (each collector missing 1), 0 duplicates
                Arguments.of(
                        files(TEST12_COLLECTOR0, TEST12_COLLECTOR1, TEST12_COLLECTOR2),
                        files(TEST12_EXPECTED),
                        List.of(stats(6, 3, 3, 0))
                ),

                // Test 13: High duplicate count (one record appears many times)
                // Collector 0: seq=700 (4 times)
                // Collector 1: seq=700 (1 time)
                // Expected: 1 unique, 0 missing, 1 duplicate (1 unique record has count=5 > 2 collectors)
                Arguments.of(
                        files(TEST13_COLLECTOR0, TEST13_COLLECTOR1),
                        files(TEST13_EXPECTED),
                        List.of(stats(5, 1, 0, 1))
                ),

                // Test 14: Mixed scenario - duplicates, missing, and correct records
                // Collector 0: seq=800 (2x), seq=801, seq=803
                // Collector 1: seq=800, seq=802, seq=803
                // Expected: 4 unique, 2 missing (seq=801 and seq=802), 1 duplicate (seq=800 appears 3 times, expected 2)
                Arguments.of(
                        files(TEST14_COLLECTOR0, TEST14_COLLECTOR1),
                        files(TEST14_EXPECTED),
                        List.of(stats(7, 4, 2, 1))
                ),

                // Test 15: Large sequence number gap
                // Collector 0: seq=1000, seq=10000 (gap of 9000)
                // Collector 1: seq=1000, seq=10000
                // Expected: 2 unique, 0 missing, 0 duplicates
                Arguments.of(
                        files(TEST15_COLLECTOR0, TEST15_COLLECTOR1),
                        files(TEST15_COLLECTOR0),
                        List.of(stats(4, 2, 0, 0))
                ),

                // Test 16: Single collector with duplicates (no redundancy from other collectors)
                // Collector 0: seq=1100 (2x), seq=1101
                // Expected: 2 unique, 0 missing (only 1 collector, so count < 1 is impossible), 1 duplicate (seq=1100 has count=2 > 1)
                Arguments.of(
                        files(TEST16_COLLECTOR0),
                        files(TEST16_EXPECTED),
                        List.of(stats(3, 2, 0, 1))
                )
        );
    }

    @ParameterizedTest
    @MethodSource("testMarketDataAggregatorArgs")
    void testMarketDataAggregator(
            List<S3Data> inputS3Files,
            List<S3Data> outputS3Files,
            List<MetricStats> metricStats
    ) throws IOException {
        when(s3Client.listObjectsV2(any(Consumer.class))).thenReturn(
                ListObjectsV2Response.builder()
                        .contents(
                                inputS3Files.stream().map(key -> S3Object.builder().key(key.key).build()).toList()
                        )
                        .build()
        );
        if (!inputS3Files.isEmpty()) {
            when(s3Client.deleteObjects(any(Consumer.class))).thenReturn(DeleteObjectsResponse.builder().build());
        }
        if (!outputS3Files.isEmpty()) {
            when(s3Client.putObject((Consumer<PutObjectRequest.Builder>) any(Consumer.class), (RequestBody) any())).thenReturn(PutObjectResponse.builder().build());
        }

        for (S3Data s3Data : inputS3Files) {
            InputStream mockStream = new ByteArrayInputStream(s3Data.compressed().array());
            ResponseInputStream<GetObjectResponse> mockResponse = new ResponseInputStream<>(
                    GetObjectResponse.builder().build(),
                    mockStream
            );
            doReturn(mockResponse).when(s3Client).getObject(argThat((ArgumentMatcher<Consumer<GetObjectRequest.Builder>>) argument -> {
                var input = GetObjectRequest.builder();
                argument.accept(input);
                var request = input.build();

                return request.key().equals(s3Data.key);
            }));
        }
        if (!outputS3Files.isEmpty()) {
            when(cloudWatchClient.putMetricData(any(Consumer.class))).thenReturn(null); // TODO: Check this for correct nummies
        }

        aggregator.runAggregator();
        assertTrue(true);

        ArgumentCaptor<Consumer<ListObjectsV2Request.Builder>> listCaptor = ArgumentCaptor.forClass(Consumer.class);
        verify(s3Client, times(1)).listObjectsV2(listCaptor.capture());
        ListObjectsV2Request.Builder listBuilder = ListObjectsV2Request.builder();
        listCaptor.getValue().accept(listBuilder);
        ListObjectsV2Request capturedListRequest = listBuilder.build();
        assertEquals(INPUT, capturedListRequest.bucket());

        ArgumentCaptor<Consumer<DeleteObjectsRequest.Builder>> deleteCaptor = ArgumentCaptor.forClass(Consumer.class);
        verify(s3Client, times(inputS3Files.isEmpty() ? 0 : 1)).deleteObjects(deleteCaptor.capture());
        if (!inputS3Files.isEmpty()) {
            DeleteObjectsRequest.Builder deleteBuilder = DeleteObjectsRequest.builder();
            deleteCaptor.getValue().accept(deleteBuilder);
            DeleteObjectsRequest capturedDeletedRequest = deleteBuilder.build();
            assertEquals(inputS3Files.size(), capturedDeletedRequest.delete().objects().size());
            for (var obj : capturedDeletedRequest.delete().objects()) {
                assertTrue(inputS3Files.stream().anyMatch(item -> item.key.equals(obj.key())));
            }
        }

        ArgumentCaptor<Consumer<PutObjectRequest.Builder>> putObjectReqCaptor = ArgumentCaptor.forClass(Consumer.class);
        ArgumentCaptor<RequestBody> requestBodyCaptor = ArgumentCaptor.forClass(RequestBody.class);
        verify(s3Client, times(outputS3Files.size())).putObject(putObjectReqCaptor.capture(), requestBodyCaptor.capture());

        ArgumentCaptor<Consumer<PutMetricDataRequest.Builder>> putMetricCaptor = ArgumentCaptor.forClass(Consumer.class);
        verify(cloudWatchClient, times(outputS3Files.size())).putMetricData(putMetricCaptor.capture());

        if (!outputS3Files.isEmpty()) {
            var putObjectCaptures = putObjectReqCaptor.getAllValues();
            var requestBodyCaptures = requestBodyCaptor.getAllValues();
            var putMetricCaptures = putMetricCaptor.getAllValues();
            for (int i = 0; i < putObjectCaptures.size(); i++) {
                var outputS3 = outputS3Files.get(i);
                var bytes = requestBodyCaptures.get(i).contentStreamProvider().newStream().readAllBytes();
                PutObjectRequest.Builder builder = PutObjectRequest.builder();
                putObjectCaptures.get(i).accept(builder);
                PutObjectRequest putObjectRequest = builder.build();

                assertArrayEquals(outputS3.object.array(), decompress(bytes));
                assertEquals(outputS3.outputKey(), putObjectRequest.key());
                assertEquals(OUTPUT, putObjectRequest.bucket());

                PutMetricDataRequest.Builder putMetricBuilder = PutMetricDataRequest.builder();
                putMetricCaptures.get(i).accept(putMetricBuilder);
                PutMetricDataRequest putMetricDataRequest = putMetricBuilder.build();
                String[] keyParts = outputS3Files.get(i).outputKey().split("/");
                // Key format: securityId/exchangeId/timestamp/schemaType
                // Dimensions order: SecurityId, ExchangeId, Timestamp, Schema
                for (var metric : putMetricDataRequest.metricData()) {
                    assertEquals(keyParts[0], metric.dimensions().get(0).value()); // securityId
                    assertEquals(keyParts[1], metric.dimensions().get(1).value()); // exchangeId
                    assertEquals(keyParts[2], metric.dimensions().get(2).value()); // timestamp
                    String schemaTypeWithoutExt = keyParts[3].replace(".zst", "");
                    assertEquals(schemaTypeWithoutExt, metric.dimensions().get(3).value()); // schemaType
                }

                MetricStats stats = metricStats.get(i);
                assertEquals(stats.total, putMetricDataRequest.metricData().get(0).value());
                assertEquals(stats.unique, putMetricDataRequest.metricData().get(1).value());
                assertEquals(stats.missing, putMetricDataRequest.metricData().get(2).value());
                assertEquals(stats.duplicate, putMetricDataRequest.metricData().get(3).value());
                assertEquals("MarketData", putMetricDataRequest.namespace());
            }
        }
    }

    @Test
    void testEmptyInputBucket() {
        // Test with no files in input bucket
        when(s3Client.listObjectsV2(any(Consumer.class))).thenReturn(
                ListObjectsV2Response.builder()
                        .contents(List.of())
                        .build()
        );

        aggregator.runAggregator();

        // Should not attempt any operations
        verify(s3Client, times(1)).listObjectsV2(any(Consumer.class));
        verify(s3Client, never()).getObject(any(Consumer.class));
        verify(s3Client, never()).putObject(any(Consumer.class), any(RequestBody.class));
        verify(s3Client, never()).deleteObjects(any(Consumer.class));
        verify(cloudWatchClient, never()).putMetricData(any(Consumer.class));
    }

    @Test
    void testSingleFileNoAggregationNeeded() throws IOException {
        // Single file should still be processed (decompressed and recompressed)
        S3Data inputFile = collector(151, 499, "202504011200", SchemaType.MBO, 0,
                record(100, 0x1),
                record(101, 0x2),
                record(102, 0x3));

        when(s3Client.listObjectsV2(any(Consumer.class))).thenReturn(
                ListObjectsV2Response.builder()
                        .contents(S3Object.builder().key(inputFile.key).build())
                        .build()
        );

        InputStream mockStream = new ByteArrayInputStream(inputFile.compressed().array());
        ResponseInputStream<GetObjectResponse> mockResponse = new ResponseInputStream<>(
                GetObjectResponse.builder().build(),
                mockStream
        );
        doReturn(mockResponse).when(s3Client).getObject(any(Consumer.class));

        when(s3Client.putObject(any(Consumer.class), any(RequestBody.class)))
                .thenReturn(PutObjectResponse.builder().build());
        when(s3Client.deleteObjects(any(Consumer.class)))
                .thenReturn(DeleteObjectsResponse.builder().build());
        when(cloudWatchClient.putMetricData(any(Consumer.class))).thenReturn(null);

        aggregator.runAggregator();

        // Should upload output file
        ArgumentCaptor<Consumer<PutObjectRequest.Builder>> putCaptor = ArgumentCaptor.forClass(Consumer.class);
        ArgumentCaptor<RequestBody> bodyCaptor = ArgumentCaptor.forClass(RequestBody.class);
        verify(s3Client, times(1)).putObject(putCaptor.capture(), bodyCaptor.capture());

        // Verify output matches input (no missing or duplicate records)
        byte[] outputBytes = bodyCaptor.getValue().contentStreamProvider().newStream().readAllBytes();
        assertArrayEquals(inputFile.object.array(), decompress(outputBytes));

        // Verify metrics: 3 total, 3 unique, 0 missing, 0 duplicate
        ArgumentCaptor<Consumer<PutMetricDataRequest.Builder>> metricCaptor = ArgumentCaptor.forClass(Consumer.class);
        verify(cloudWatchClient, times(1)).putMetricData(metricCaptor.capture());

        PutMetricDataRequest.Builder metricBuilder = PutMetricDataRequest.builder();
        metricCaptor.getValue().accept(metricBuilder);
        PutMetricDataRequest metricRequest = metricBuilder.build();

        assertEquals(3.0, metricRequest.metricData().get(0).value()); // Total
        assertEquals(3.0, metricRequest.metricData().get(1).value()); // Unique
        assertEquals(0.0, metricRequest.metricData().get(2).value()); // Missing
        assertEquals(0.0, metricRequest.metricData().get(3).value()); // Duplicate
    }

    // Generate test data statically to avoid schema state issues
    private static final S3Data PERFECT_REDUNDANCY_FILE1 = collector(151, 499, "202504011200", SchemaType.MBP_10, 0,
            record(100, 0x1), record(101, 0x2));
    private static final S3Data PERFECT_REDUNDANCY_FILE2 = collector(151, 499, "202504011200", SchemaType.MBP_10, 1,
            record(100, 0x1), record(101, 0x2));
    private static final S3Data PERFECT_REDUNDANCY_FILE3 = collector(151, 499, "202504011200", SchemaType.MBP_10, 2,
            record(100, 0x1), record(101, 0x2));

    @Test
    void testPerfectRedundancyAllRecordsIdentical() throws IOException {
        // 3 collectors, all captured the exact same records
        List<S3Data> inputFiles = List.of(PERFECT_REDUNDANCY_FILE1, PERFECT_REDUNDANCY_FILE2, PERFECT_REDUNDANCY_FILE3);

        when(s3Client.listObjectsV2(any(Consumer.class))).thenReturn(
                ListObjectsV2Response.builder()
                        .contents(inputFiles.stream().map(f -> S3Object.builder().key(f.key).build()).toList())
                        .build()
        );

        for (S3Data file : inputFiles) {
            InputStream mockStream = new ByteArrayInputStream(file.compressed().array());
            ResponseInputStream<GetObjectResponse> mockResponse = new ResponseInputStream<>(
                    GetObjectResponse.builder().build(),
                    mockStream
            );
            doReturn(mockResponse).when(s3Client).getObject(argThat((ArgumentMatcher<Consumer<GetObjectRequest.Builder>>) argument -> {
                var input = GetObjectRequest.builder();
                argument.accept(input);
                return input.build().key().equals(file.key);
            }));
        }

        when(s3Client.putObject(any(Consumer.class), any(RequestBody.class)))
                .thenReturn(PutObjectResponse.builder().build());
        when(s3Client.deleteObjects(any(Consumer.class)))
                .thenReturn(DeleteObjectsResponse.builder().build());
        when(cloudWatchClient.putMetricData(any(Consumer.class))).thenReturn(null);

        aggregator.runAggregator();

        // Verify metrics: 6 total (2 records × 3 files), 2 unique, 0 missing, 0 duplicate
        ArgumentCaptor<Consumer<PutMetricDataRequest.Builder>> metricCaptor = ArgumentCaptor.forClass(Consumer.class);
        verify(cloudWatchClient, times(1)).putMetricData(metricCaptor.capture());

        PutMetricDataRequest.Builder metricBuilder = PutMetricDataRequest.builder();
        metricCaptor.getValue().accept(metricBuilder);
        PutMetricDataRequest metricRequest = metricBuilder.build();

        assertEquals(6.0, metricRequest.metricData().get(0).value()); // Total: 2 records × 3 files
        assertEquals(2.0, metricRequest.metricData().get(1).value()); // Unique: 2 distinct records
        assertEquals(0.0, metricRequest.metricData().get(2).value()); // Missing: all records in all files
        assertEquals(0.0, metricRequest.metricData().get(3).value()); // Duplicate: each appears exactly 3 times
    }

    @Test
    void testThrowsOnLeftoverBytes() {
        // Create a file with data that doesn't divide evenly by message size
        SchemaType schemaType = SchemaType.MBO;
        int msgSize = schemaType.getInstance().totalMessageSize();

        // Create data with leftover bytes (msgSize + 5 extra bytes)
        byte[] invalidData = new byte[msgSize + 5];
        new Random(0x42).nextBytes(invalidData);
        byte[] compressed = compress(invalidData);

        String key = "499/151/202504011200/mbo/test-id.zst";

        when(s3Client.listObjectsV2(any(Consumer.class))).thenReturn(
                ListObjectsV2Response.builder()
                        .contents(S3Object.builder().key(key).build())
                        .build()
        );

        InputStream mockStream = new ByteArrayInputStream(compressed);
        ResponseInputStream<GetObjectResponse> mockResponse = new ResponseInputStream<>(
                GetObjectResponse.builder().build(),
                mockStream
        );
        doReturn(mockResponse).when(s3Client).getObject(any(Consumer.class));

        // Should throw AssertionError due to leftover bytes
        assertThrows(AssertionError.class, () -> aggregator.runAggregator());
    }

    @Test
    void testThrowsOnIllegalKey() {
        // Test various illegal key formats
        String[] illegalKeys = {
                "invalid-key-format.zst",
                "499/151/mbo/test-id.zst",  // Missing timestamp
                "499/151/20250401/mbo/test-id.zst",  // Timestamp too short (8 digits instead of 12)
                "499/151/2025040112001/mbo/test-id.zst",  // Timestamp too long (13 digits)
                "abc/151/202504011200/mbo/test-id.zst",  // Non-numeric security ID
                "499/xyz/202504011200/mbo/test-id.zst",  // Non-numeric exchange ID
                "499/151/202504011200/test-id.zst",  // Missing schema type
                "499/151/202504011200/mbo/",  // Missing file name
        };

        for (String illegalKey : illegalKeys) {
            when(s3Client.listObjectsV2(any(Consumer.class))).thenReturn(
                    ListObjectsV2Response.builder()
                            .contents(S3Object.builder().key(illegalKey).build())
                            .build()
            );

            assertThrows(IllegalArgumentException.class, () -> aggregator.runAggregator(),
                    "Should throw IllegalArgumentException for key: " + illegalKey);
        }
    }

    @Test
    void testNoOverlapBetweenFilesAllMissing() throws IOException {
        // 3 collectors, each captured completely different records (no overlap)
        S3Data file1 = collector(151, 499, "202504011200", SchemaType.MBO, 0,
                record(100, 0x1), record(101, 0x2));
        S3Data file2 = collector(151, 499, "202504011200", SchemaType.MBO, 1,
                record(102, 0x3), record(103, 0x4));
        S3Data file3 = collector(151, 499, "202504011200", SchemaType.MBO, 2,
                record(104, 0x5), record(105, 0x6));

        List<S3Data> inputFiles = List.of(file1, file2, file3);

        when(s3Client.listObjectsV2(any(Consumer.class))).thenReturn(
                ListObjectsV2Response.builder()
                        .contents(inputFiles.stream().map(f -> S3Object.builder().key(f.key).build()).toList())
                        .build()
        );

        for (S3Data file : inputFiles) {
            InputStream mockStream = new ByteArrayInputStream(file.compressed().array());
            ResponseInputStream<GetObjectResponse> mockResponse = new ResponseInputStream<>(
                    GetObjectResponse.builder().build(),
                    mockStream
            );
            doReturn(mockResponse).when(s3Client).getObject(argThat((ArgumentMatcher<Consumer<GetObjectRequest.Builder>>) argument -> {
                var input = GetObjectRequest.builder();
                argument.accept(input);
                return input.build().key().equals(file.key);
            }));
        }

        when(s3Client.putObject(any(Consumer.class), any(RequestBody.class)))
                .thenReturn(PutObjectResponse.builder().build());
        when(s3Client.deleteObjects(any(Consumer.class)))
                .thenReturn(DeleteObjectsResponse.builder().build());
        when(cloudWatchClient.putMetricData(any(Consumer.class))).thenReturn(null);

        aggregator.runAggregator();

        // Verify metrics: 6 total, 6 unique, 6 missing (each record appears in only 1 of 3 files)
        ArgumentCaptor<Consumer<PutMetricDataRequest.Builder>> metricCaptor = ArgumentCaptor.forClass(Consumer.class);
        verify(cloudWatchClient, times(1)).putMetricData(metricCaptor.capture());

        PutMetricDataRequest.Builder metricBuilder = PutMetricDataRequest.builder();
        metricCaptor.getValue().accept(metricBuilder);
        PutMetricDataRequest metricRequest = metricBuilder.build();

        assertEquals(6.0, metricRequest.metricData().get(0).value()); // Total: 6 records
        assertEquals(6.0, metricRequest.metricData().get(1).value()); // Unique: 6 distinct records
        assertEquals(6.0, metricRequest.metricData().get(2).value()); // Missing: all 6 records missing from 2 files each
        assertEquals(0.0, metricRequest.metricData().get(3).value()); // Duplicate: no duplicates
    }

    @Test
    void testOutOfOrderSequenceNumbers() throws IOException {
        // Records arrive out of order, but should be sorted by sequence number in output
        // File 1: seq 300, 100, 200 (out of order)
        // File 2: seq 100, 300, 200 (out of order, but same records as file1)
        // Output should be: 100, 200, 300 (sorted)

        // Use same seeds for same sequence numbers so they deduplicate
        S3Data file1 = itemWithSequences(151, 499, "202504011200", SchemaType.MBP_1, 0,
                new int[]{0x3, 0x1, 0x2}, new int[]{300, 100, 200});
        S3Data file2 = itemWithSequences(151, 499, "202504011200", SchemaType.MBP_1, 1,
                new int[]{0x1, 0x3, 0x2}, new int[]{100, 300, 200});

        List<S3Data> inputFiles = List.of(file1, file2);

        when(s3Client.listObjectsV2(any(Consumer.class))).thenReturn(
                ListObjectsV2Response.builder()
                        .contents(inputFiles.stream().map(f -> S3Object.builder().key(f.key).build()).toList())
                        .build()
        );

        for (S3Data file : inputFiles) {
            InputStream mockStream = new ByteArrayInputStream(file.compressed().array());
            ResponseInputStream<GetObjectResponse> mockResponse = new ResponseInputStream<>(
                    GetObjectResponse.builder().build(),
                    mockStream
            );
            doReturn(mockResponse).when(s3Client).getObject(argThat((ArgumentMatcher<Consumer<GetObjectRequest.Builder>>) argument -> {
                var input = GetObjectRequest.builder();
                argument.accept(input);
                return input.build().key().equals(file.key);
            }));
        }

        when(s3Client.putObject(any(Consumer.class), any(RequestBody.class)))
                .thenReturn(PutObjectResponse.builder().build());
        when(s3Client.deleteObjects(any(Consumer.class)))
                .thenReturn(DeleteObjectsResponse.builder().build());
        when(cloudWatchClient.putMetricData(any(Consumer.class))).thenReturn(null);

        aggregator.runAggregator();

        // Verify output is sorted by sequence number
        ArgumentCaptor<RequestBody> bodyCaptor = ArgumentCaptor.forClass(RequestBody.class);
        verify(s3Client, times(1)).putObject(any(Consumer.class), bodyCaptor.capture());

        byte[] outputBytes = bodyCaptor.getValue().contentStreamProvider().newStream().readAllBytes();
        byte[] decompressed = decompress(outputBytes);

        // Parse the output and verify sequence numbers are in order
        Schema schema = SchemaType.MBP_1.getInstance();
        int msgSize = schema.totalMessageSize();
        assertEquals(0, decompressed.length % msgSize, "Output should be evenly divisible by message size");

        long previousSeq = -1;
        for (int i = 0; i < decompressed.length; i += msgSize) {
            byte[] record = Arrays.copyOfRange(decompressed, i, i + msgSize);
            schema.buffer.putBytes(0, record);
            schema.wrap(schema.buffer);
            long currentSeq = schema.getSequenceNumber();

            assertTrue(currentSeq >= previousSeq,
                    "Sequence numbers should be in non-decreasing order. Previous: " + previousSeq + ", Current: " + currentSeq);
            previousSeq = currentSeq;
        }

        // Should have sequence numbers 100, 200, 300 (3 unique records after deduplication)
        assertEquals(3, decompressed.length / msgSize, "Should have 3 records after deduplication");
        assertEquals(100, getSequenceAtIndex(decompressed, schema, 0));
        assertEquals(200, getSequenceAtIndex(decompressed, schema, 1));
        assertEquals(300, getSequenceAtIndex(decompressed, schema, 2));
    }

    @Test
    void testSameSequenceNumberPreservesInsertionOrder() throws IOException {
        // Multiple records with the same sequence number should preserve insertion order
        // This tests the LinkedHashMap behavior

        // Create records with same sequence number but different content
        S3Data file1 = itemWithSequences(151, 499, "202504011200", SchemaType.MBP_10, 0,
                new int[]{0x1, 0x2, 0x3}, new int[]{100, 100, 100});

        when(s3Client.listObjectsV2(any(Consumer.class))).thenReturn(
                ListObjectsV2Response.builder()
                        .contents(S3Object.builder().key(file1.key).build())
                        .build()
        );

        InputStream mockStream = new ByteArrayInputStream(file1.compressed().array());
        ResponseInputStream<GetObjectResponse> mockResponse = new ResponseInputStream<>(
                GetObjectResponse.builder().build(),
                mockStream
        );
        doReturn(mockResponse).when(s3Client).getObject(any(Consumer.class));

        when(s3Client.putObject(any(Consumer.class), any(RequestBody.class)))
                .thenReturn(PutObjectResponse.builder().build());
        when(s3Client.deleteObjects(any(Consumer.class)))
                .thenReturn(DeleteObjectsResponse.builder().build());
        when(cloudWatchClient.putMetricData(any(Consumer.class))).thenReturn(null);

        aggregator.runAggregator();

        // Verify output preserves insertion order
        ArgumentCaptor<RequestBody> bodyCaptor = ArgumentCaptor.forClass(RequestBody.class);
        verify(s3Client, times(1)).putObject(any(Consumer.class), bodyCaptor.capture());

        byte[] outputBytes = bodyCaptor.getValue().contentStreamProvider().newStream().readAllBytes();
        assertArrayEquals(file1.object.array(), decompress(outputBytes),
                "Output should preserve insertion order for records with same sequence number");
    }

    private static long getSequenceAtIndex(byte[] data, Schema schema, int index) {
        int msgSize = schema.totalMessageSize();
        byte[] record = Arrays.copyOfRange(data, index * msgSize, (index + 1) * msgSize);
        schema.buffer.putBytes(0, record);
        schema.wrap(schema.buffer);
        return schema.getSequenceNumber();
    }

    /**
     * Represents a single record with a sequence number and random seed for content generation.
     */
    private record RecordSpec(long sequence, int seed) {}

    /**
     * Helper to create a RecordSpec.
     */
    private static RecordSpec record(long sequence, int seed) {
        return new RecordSpec(sequence, seed);
    }

    /**
     * Creates test data for a single collector (one S3 file).
     * Each collector is identified by its collectorId and contains multiple records.
     */
    private static S3Data collector(int securityId, int exchangeId, String timestamp,
                                   SchemaType schemaType, int collectorId, RecordSpec... records) {
        Schema schema = schemaType.getInstance();
        int msgSize = schema.totalMessageSize();
        byte[] output = new byte[msgSize * records.length];

        for (int i = 0; i < records.length; i++) {
            byte[] item = new byte[msgSize];
            new Random(records[i].seed).nextBytes(item);
            schema.buffer.putBytes(0, item);
            setSequence(records[i].sequence, schema);
            schema.wrap(schema.buffer);
            schema.buffer.getBytes(0, output, i * msgSize, msgSize);
        }

        return new S3Data("%d/%d/%s/%s/id-%d.zst".formatted(securityId, exchangeId, timestamp, schemaType.getIdentifier(), collectorId), ByteBuffer.wrap(output));
    }

    private static S3Data itemWithSequences(int securityId, int exchangeId, String timestamp,
                                            SchemaType schemaType, int id, int[] seeds, int[] sequences) {
        if (seeds.length != sequences.length) {
            throw new IllegalArgumentException("seeds and sequences must have same length");
        }

        Schema schema = schemaType.getInstance();
        int msgSize = schema.totalMessageSize();
        byte[] output = new byte[msgSize * seeds.length];
        for (int i = 0; i < seeds.length; i++) {
            byte[] item = new byte[msgSize];
            new Random(seeds[i]).nextBytes(item);
            schema.buffer.putBytes(0, item);
            setSequence(sequences[i], schema);
            schema.wrap(schema.buffer);
            schema.buffer.getBytes(0, output, i * msgSize, msgSize);
        }
        return new S3Data("%d/%d/%s/%s/id-%d.zst".formatted(securityId, exchangeId, timestamp, schemaType.getIdentifier(), id), ByteBuffer.wrap(output));
    }

    private static S3Data item(int securityId, int exchangeId, String timestamp, SchemaType schemaType, int id, int time, int... seeds) {
        Schema schema = schemaType.getInstance();
        int msgSize = schema.totalMessageSize();
        byte[] output = new byte[msgSize * seeds.length];
        for (int i = 0; i < seeds.length; i++) {
            byte[] item = new byte[msgSize];
            new Random(seeds[i]).nextBytes(item);
            schema.buffer.putBytes(0, item);
            setSequence(time + i, schema); // Each record gets unique sequence number
            schema.wrap(schema.buffer); // Re-apply the header
            schema.buffer.getBytes(0, output, i * msgSize, msgSize);
        }
        return new S3Data("%d/%d/%s/%s/id-%d.zst".formatted(securityId, exchangeId, timestamp, schemaType.getIdentifier(), id), ByteBuffer.wrap(output));
    }

    private static void setSequence(long time, Schema schema) {
        if (schema instanceof MBP10Schema mbp10) {
            mbp10.encoder.sequence(time);
        } else if (schema instanceof MBP1Schema mbp1) {
            mbp1.encoder.sequence(time);
        } else if (schema instanceof group.gnometrading.schemas.MBOSchema mbo) {
            mbo.encoder.sequence(time);
        } else {
            throw new IllegalArgumentException("Unsupported schema type: " + schema.getClass().getName());
        }
    }

    public static byte[] compress(byte[] input) {
        ByteArrayOutputStream compressedOutput = new ByteArrayOutputStream();
        try (ZstdOutputStream zstdStream = new ZstdOutputStream(compressedOutput)) {
            zstdStream.write(input);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return compressedOutput.toByteArray();
    }

    public static byte[] decompress(byte[] input) {
        try (ZstdInputStream zstdStream = new ZstdInputStream(new ByteArrayInputStream(input))) {
            return zstdStream.readAllBytes();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static List<S3Data> files(S3Data... input) {
        Map<String, List<S3Data>> output = new LinkedHashMap<>();
        for (S3Data s3Data : input) {
            output.computeIfAbsent(s3Data.key, k -> new ArrayList<>()).add(s3Data);
        }

        List<S3Data> combined = new ArrayList<>();
        for (List<S3Data> group : output.values()) {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            for (S3Data record : group) {
                try {
                    outputStream.write(record.object.array());
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            byte[] total = outputStream.toByteArray();
            combined.add(new S3Data(group.get(0).key, ByteBuffer.wrap(total)));
        }
        return combined;
    }

    private static MetricStats stats(int total, int unique, int missing, int duplicate) {
        return new MetricStats(total, unique, missing, duplicate);
    }

    private record MetricStats(int total, int unique, int missing, int duplicate) {}

    private record S3Data(String key, ByteBuffer object) {
        public ByteBuffer compressed() {
            return ByteBuffer.wrap(compress(object.array()));
        }
        public String outputKey() {
            String[] parts = key.split("/");
            return "%s/%s/%s/%s.zst".formatted(parts[0], parts[1], parts[2], parts[3]);
        }
    }
}