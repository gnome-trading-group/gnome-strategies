package group.gnometrading.collector;

import com.github.luben.zstd.ZstdInputStream;
import com.github.luben.zstd.ZstdOutputStream;
import group.gnometrading.logging.NullLogger;
import group.gnometrading.schemas.MBP10Schema;
import group.gnometrading.schemas.MBP1Schema;
import group.gnometrading.schemas.Schema;
import group.gnometrading.schemas.SchemaType;
import org.junit.jupiter.api.BeforeEach;
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
class MarketDataAggregatorTest {

    private static final String INPUT = "input";
    private static final String OUTPUT = "output";

    @Mock
    S3Client s3Client;

    @Mock
    CloudWatchClient cloudWatchClient;

    MarketDataAggregator aggregator;

    static int idx = 0;

    @BeforeEach
    void setup() {
        this.aggregator = new MarketDataAggregator(new NullLogger(), s3Client, cloudWatchClient, INPUT, OUTPUT);
    }

    private static Stream<Arguments> testMarketDataAggregatorArgs() {
        return Stream.of(
                Arguments.of(
                        new ArrayList<S3Data>(),
                        new ArrayList<S3Data>(),
                        List.of(stats(0, 0, 0, 0))
                ),
                // Trivial case: one single input leads to one output
                Arguments.of(
                        files(
                                item(1, 3, "2025040111", SchemaType.MBP_10, 0, 100, 0x1)
                        ),
                        files(
                                item(1, 3, "2025040111", SchemaType.MBP_10, 0, 100, 0x1)
                        ),
                        List.of(stats(1, 1, 0, 0))
                ),
                // Trivial case: two duplicate files lead to only one output
                Arguments.of(
                        files(
                                item(1, 3, "2025040111", SchemaType.MBP_10, 0, 100, 0x1),
                                item(1, 3, "2025040111", SchemaType.MBP_10, 1, 100, 0x1)
                        ),
                        files(
                                item(1, 3, "2025040111", SchemaType.MBP_10, 0, 100, 0x1)
                        ),
                        List.of(stats(2, 1, 0, 0))
                ),
                // One file has two entries, and one file has a duplicate
                // idx starts from 5
                Arguments.of(
                        files(
                                item(5, 3, "2025040112", SchemaType.MBP_1, 0, 50, 0x1, 0x2),
                                item(5, 3, "2025040112", SchemaType.MBP_1, 1, 50, 0x1)
                        ),
                        files(
                                item(5, 3, "2025040112", SchemaType.MBP_1, 0, 50, 0x1, 0x2)
                        ),
                        List.of(stats(3, 2, 1, 0))
                ),
                // Test simple duplicate entry case
                Arguments.of(
                        files(
                                item(0, 6, "2024040112", SchemaType.MBO, 0, 101, 0x1, 0x1, 0x2),
                                item(0, 6, "2024040112", SchemaType.MBO, 1, 101, 0x1)
                        ),
                        files(
                                item(0, 6, "2024040112", SchemaType.MBO, 0, 101, 0x1, 0x2)
                        ),
                        List.of(stats(4, 2, 1, 1))
                ),
                // Simple case with multiple exchanges
                Arguments.of(
                        files(
                                item(0, 6, "2024040112", SchemaType.MBO, 0, 101, 0x5, 0x2),
                                item(1, 6, "2024040112", SchemaType.MBO, 1, 101, 0x5)
                        ),
                        files(
                                item(0, 6, "2024040112", SchemaType.MBO, 0, 101, 0x5, 0x2),
                                item(1, 6, "2024040112", SchemaType.MBO, 1, 101, 0x5)
                        ),
                        List.of(
                                stats(2, 2, 0, 0),
                                stats(1, 1, 0, 0)
                        )
                ),
                Arguments.of(
                        files(
                                item(0, 6, "2024040112", SchemaType.MBO, 0, 101, 0x5, 0x2),
                                item(0, 6, "2024040112", SchemaType.MBO, 0, 103, 0x5, 0x2),
                                item(0, 6, "2024040112", SchemaType.MBO, 0, 151, 0x5, 0x2, 0x3),
                                item(1, 6, "2024040112", SchemaType.MBO, 1, 101, 0x5)
                        ),
                        files(
                                item(0, 6, "2024040112", SchemaType.MBO, 0, 101, 0x5, 0x2),
                                item(0, 6, "2024040112", SchemaType.MBO, 0, 103, 0x5, 0x2),
                                item(0, 6, "2024040112", SchemaType.MBO, 0, 151, 0x5, 0x2, 0x3),
                                item(1, 6, "2024040112", SchemaType.MBO, 1, 101, 0x5)
                        ),
                        List.of(
                                stats(7, 7, 0, 0),
                                stats(1, 1, 0, 0)
                        )
                ),
                Arguments.of(
                        files(
                                item(0, 6, "2024040112", SchemaType.MBO, 0, 101, 0x5, 0x2),
                                item(0, 6, "2024040112", SchemaType.MBO, 0, 103, 0x5, 0x2),
                                item(0, 6, "2024040112", SchemaType.MBO, 0, 151, 0x5, 0x2, 0x3),
                                item(0, 6, "2024040112", SchemaType.MBO, 3, 101, 0x5, 0x2),
                                item(1, 6, "2024040112", SchemaType.MBO, 1, 101, 0x5)
                        ),
                        files(
                                item(0, 6, "2024040112", SchemaType.MBO, 0, 101, 0x5, 0x2),
                                item(0, 6, "2024040112", SchemaType.MBO, 0, 103, 0x5, 0x2),
                                item(0, 6, "2024040112", SchemaType.MBO, 0, 151, 0x5, 0x2, 0x3),
                                item(1, 6, "2024040112", SchemaType.MBO, 1, 101, 0x5)
                        ),
                        List.of(
                                stats(9, 7, 5, 0),
                                stats(1, 1, 0, 0)
                        )
                ),
                Arguments.of(
                        files(
                                item(0, 6, "2024040112", SchemaType.MBO, 0, 101, 0x5, 0x2),
                                item(0, 6, "2024040112", SchemaType.MBO, 0, 103, 0x5, 0x2),
                                item(0, 6, "2024040112", SchemaType.MBO, 0, 151, 0x5, 0x2, 0x3),
                                item(0, 6, "2024040113", SchemaType.MBO, 3, 101, 0x5, 0x2),
                                item(1, 6, "2024040112", SchemaType.MBO, 1, 101, 0x5)
                        ),
                        files(
                                item(0, 6, "2024040112", SchemaType.MBO, 0, 101, 0x5, 0x2),
                                item(0, 6, "2024040112", SchemaType.MBO, 0, 103, 0x5, 0x2),
                                item(0, 6, "2024040112", SchemaType.MBO, 0, 151, 0x5, 0x2, 0x3),
                                item(0, 6, "2024040113", SchemaType.MBO, 3, 101, 0x5, 0x2),
                                item(1, 6, "2024040112", SchemaType.MBO, 1, 101, 0x5)
                        ),
                        List.of(
                                stats(7, 7, 0, 0),
                                stats(2, 2, 0, 0),
                                stats(1, 1, 0, 0)
                        )
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
                String[] keyParts = outputS3Files.get(i).key.split("/");
                for (var metric : putMetricDataRequest.metricData()) {
                    for (int z = 0; z < 4; z++) {
                        assertEquals(keyParts[z], metric.dimensions().get(z).value());
                    }
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
    void testThrowsOnLeftoverBytes() {
        // TODO
    }

    @Test
    void testThrowsOnIllegalKey() {
        // TODO
    }

    private static S3Data item(int exchangeId, int securityId, String timestamp, SchemaType schemaType, int id, int time, int... seeds) {
        int msgSize = schemaType.getInstance().totalMessageSize();
        byte[] output = new byte[msgSize * seeds.length];
        for (int i = 0; i < seeds.length; i++) {
            byte[] item = new byte[msgSize];
            new Random(seeds[i]).nextBytes(item);
            schemaType.getInstance().buffer.putBytes(0, item);
            setSequence(time, schemaType.getInstance());
            schemaType.getInstance().wrap(schemaType.getInstance().buffer); // Re-apply the header
            schemaType.getInstance().buffer.getBytes(0, output, i * msgSize, msgSize);
        }
        return new S3Data("%d/%d/%s/%s/id-%d.zst".formatted(exchangeId, securityId, timestamp, schemaType.getIdentifier(), id), ByteBuffer.wrap(output));
    }

    private static void setSequence(long time, Schema schema) {
        if (schema instanceof MBP10Schema) {
            ((MBP10Schema) schema).encoder.sequence(time);
        } else if (schema instanceof MBP1Schema) {
            ((MBP1Schema) schema).encoder.sequence(time);
        } else {
            throw new IllegalArgumentException("Add more if statements you lazy man.");
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