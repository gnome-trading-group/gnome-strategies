package group.gnometrading.collector;

import com.github.luben.zstd.ZstdInputStream;
import com.github.luben.zstd.ZstdOutputStream;
import group.gnometrading.schemas.Schema;
import group.gnometrading.schemas.SchemaType;
import org.agrona.concurrent.UnsafeBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.cloudwatch.model.Dimension;
import software.amazon.awssdk.services.cloudwatch.model.MetricDatum;
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.S3Error;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * This class is invoked via AWS Lambda. It takes in the raw S3 files dumped by
 * MarketDataCollector and aggregates them into a consolidated stream.
 */
public class MarketDataAggregator {

    private static final Logger logger = LoggerFactory.getLogger(MarketDataAggregator.class);
    private static final Pattern FILE_PATTERN = Pattern.compile(
            "^(\\d+)/(\\d+)/(\\d{10})/([^/]+)/([^/]+)\\.zst$"
    );

    private final S3Client s3Client;
    private final CloudWatchClient cloudWatchClient;
    private final String inputBucket;
    private final String outputBucket;

    public MarketDataAggregator(
            S3Client s3Client,
            CloudWatchClient cloudWatchClient,
            String inputBucket,
            String outputBucket
    ) {
        this.s3Client = s3Client;
        this.cloudWatchClient = cloudWatchClient;
        this.inputBucket = inputBucket;
        this.outputBucket = outputBucket;
    }

    public void runAggregator() {
        Map<MarketDataKey, Set<String>> input = collectRawFiles();
        if (input.isEmpty()) {
            logger.info("No new S3 files produced to run aggregation on");
            return;
        }

        for (var item : input.entrySet()) {
            logger.info("Running key {} with {} entries", item.getKey().toString(), item.getValue().size());
            aggregateKeys(item.getKey(), item.getValue());
        }

        Set<String> allKeys = input.values().stream()
                .flatMap(Set::stream)
                .collect(Collectors.toSet());
        cleanUpKeys(allKeys);
    }

    private void cleanUpKeys(Set<String> keys) {
        logger.info("Deleting {} keys", keys.size());
        List<ObjectIdentifier> objectsToDelete = keys.stream()
                .map(key -> ObjectIdentifier.builder().key(key).build())
                .toList();
        var response = s3Client.deleteObjects(
                request -> request
                        .bucket(inputBucket)
                        .delete(delete -> delete.objects(objectsToDelete))
        );
        if (response.hasErrors()) {
            for (S3Error error : response.errors()) {
                logger.error("Error deleting key {}: {}", error.key(), error.message());
            }
            throw new RuntimeException("Error while deleting keys. Please check the logs");
        } else {
            logger.info("Successfully deleted keys");
        }
    }

    private void aggregateKeys(MarketDataKey marketDataKey, Set<String> keys) {
        Schema schema = SchemaType.findById(marketDataKey.schemaType).getInstance();
        TreeMap<Long, Map<ByteBuffer, Integer>> recordCounts = new TreeMap<>();
        int totalRecords = 0;

        for (String key : keys) {
            List<ByteBuffer> records = extractEntries(key, schema.totalMessageSize());
            totalRecords += records.size();
            for (ByteBuffer record : records) {
                schema.wrap(new UnsafeBuffer(record));
                // Enforce insertion order on the same timestamp in which the exchange sent it to us (ie, use a LinkedHashMap)
                var map = recordCounts.computeIfAbsent(schema.getSequenceNumber(), ignore -> new LinkedHashMap<>());
                map.put(record, map.getOrDefault(record, 0) + 1);
            }
        }

        long missingRecords = recordCounts.values().stream().flatMap(inner -> inner.values().stream()).filter(count -> count < keys.size()).count();
        long duplicateRecords = recordCounts.values().stream().flatMap(inner -> inner.values().stream()).filter(count -> count > keys.size()).count();
        long uniqueRecords = recordCounts.values().stream().mapToInt(Map::size).sum();

        var outputEntries = recordCounts.values().stream().flatMap(inner -> inner.keySet().stream()).toList();
        try {
            writeAggregatedKey(marketDataKey, outputEntries);
        } catch (IOException e) {
            logger.error("Error trying to write aggregated key: ", e);
            throw new RuntimeException(e);
        }
        logger.info("Total: {} | Missing: {} | Duplicates: {}", totalRecords, missingRecords, duplicateRecords);
        pushMetricsToCloudWatch(marketDataKey, totalRecords, uniqueRecords, missingRecords, duplicateRecords);
    }

    private void writeAggregatedKey(MarketDataKey key, List<ByteBuffer> records) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        for (ByteBuffer record : records) {
            outputStream.write(record.array());
        }
        byte[] uncompressedData = outputStream.toByteArray();

        ByteArrayOutputStream compressedOutput = new ByteArrayOutputStream();
        try (ZstdOutputStream zstdStream = new ZstdOutputStream(compressedOutput)) {
            zstdStream.write(uncompressedData);
        }

        s3Client.putObject(
                request -> request.key(key.toString()).bucket(this.outputBucket),
                RequestBody.fromBytes(compressedOutput.toByteArray())
        );
    }

    private void pushMetricsToCloudWatch(MarketDataKey key, int total, long unique, long missing, long duplicate) {
        var dimensions = new Dimension[] {
                Dimension.builder().name("Exchange").value(String.valueOf(key.exchangeId)).build(),
                Dimension.builder().name("Security").value(String.valueOf(key.securityId)).build(),
                Dimension.builder().name("Timestamp").value(key.timestamp).build(),
                Dimension.builder().name("Schema").value(key.schemaType).build()
        };
        List<MetricDatum> metrics = Arrays.asList(
                MetricDatum.builder()
                        .metricName("TotalRecords")
                        .unit(StandardUnit.COUNT)
                        .value((double) total)
                        .dimensions(dimensions)
                        .build(),
                MetricDatum.builder()
                        .metricName("UniqueRecords")
                        .unit(StandardUnit.COUNT)
                        .value((double) unique)
                        .dimensions(dimensions)
                        .build(),
                MetricDatum.builder()
                        .metricName("MissingRecords")
                        .unit(StandardUnit.COUNT)
                        .value((double) missing)
                        .dimensions(dimensions)
                        .build(),
                MetricDatum.builder()
                        .metricName("DuplicateRecords")
                        .unit(StandardUnit.COUNT)
                        .value((double) duplicate)
                        .dimensions(dimensions)
                        .build()
        );

        cloudWatchClient.putMetricData(
                request -> request
                        .namespace(MarketDataUtils.CLOUDWATCH_NAMESPACE)
                        .metricData(metrics)
        );
    }

    private List<ByteBuffer> extractEntries(String key, int expectedSize) {
        InputStream stream = s3Client.getObject(request -> request.bucket(inputBucket).key(key));

        try (ZstdInputStream zstdStream = new ZstdInputStream(stream);
             ByteArrayOutputStream buffer = new ByteArrayOutputStream()) {
            zstdStream.transferTo(buffer);
            byte[] decompressedData = buffer.toByteArray();
            List<ByteBuffer> records = new ArrayList<>();

            int i;
            for (i = 0; i < decompressedData.length; i += expectedSize) {
                records.add(ByteBuffer.wrap(Arrays.copyOfRange(decompressedData, i, i + expectedSize)));
            }
            assert i == decompressedData.length : "Left over bytes in key %s: %d".formatted(key, decompressedData.length - i);
            return records;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Map<MarketDataKey, Set<String>> collectRawFiles() {
        var listResponse = this.s3Client.listObjectsV2(request -> request.bucket(this.inputBucket));
        Map<MarketDataKey, Set<String>> outputFiles = new LinkedHashMap<>();

        for (var s3Object : listResponse.contents()) {
            Matcher matcher = FILE_PATTERN.matcher(s3Object.key());
            if (matcher.matches()) {
                int exchangeId = Integer.parseInt(matcher.group(1));
                int listingId = Integer.parseInt(matcher.group(2));
                String timestamp = matcher.group(3);
                String schemaType = matcher.group(4);

                var s3Key = new MarketDataKey(exchangeId, listingId, timestamp, schemaType);
                outputFiles.computeIfAbsent(s3Key, k -> new HashSet<>()).add(s3Object.key());
            } else {
                throw new IllegalArgumentException("Illegal key found in %s bucket: %s".formatted(this.inputBucket, s3Object.key()));
            }
        }
        return outputFiles;
    }

    private record MarketDataKey(int exchangeId, int securityId, String timestamp, String schemaType) {
        @Override
        public String toString() {
            return "%d/%d/%s/%s.zst".formatted(exchangeId, securityId, timestamp, schemaType);
        }
    }

}
