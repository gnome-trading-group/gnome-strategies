package group.gnometrading.collector.merger;

import group.gnometrading.SecurityMaster;
import group.gnometrading.collector.MarketDataEntry;
import group.gnometrading.logging.LogMessage;
import group.gnometrading.logging.Logger;
import group.gnometrading.schemas.Schema;
import group.gnometrading.schemas.SchemaType;
import group.gnometrading.sm.Listing;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.S3Error;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

/**
 * This class takes in the raw S3 files dumped by
 * MarketDataCollector and merges them into a consolidated stream.
 * <p>
 * It adds an artificial delay latency to the data to make sure containers
 * have time to post their most data.
 */
public class MarketDataMerger {


    private static final Duration MERGE_DELAY = Duration.ofMinutes(60);

    private final Logger logger;
    private final Clock clock;
    private final S3Client s3Client;
    private final SecurityMaster securityMaster;
    private final String inputBucket;
    private final String outputBucket;
    private final String archiveBucket;

    public MarketDataMerger(
            Logger logger,
            Clock clock,
            S3Client s3Client,
            SecurityMaster securityMaster,
            String inputBucket,
            String outputBucket,
            String archiveBucket
    ) {
        this.logger = logger;
        this.clock = clock;
        this.s3Client = s3Client;
        this.securityMaster = securityMaster;
        this.inputBucket = inputBucket;
        this.outputBucket = outputBucket;
        this.archiveBucket = archiveBucket;
    }

    public Set<Listing> runMerger() {
        Map<MarketDataEntry, List<MarketDataEntry>> input = collectRawFiles();
        if (input.isEmpty()) {
            logger.logf(LogMessage.DEBUG, "No new S3 files produced to run merging on");
            return Set.of();
        }

        for (var item : input.entrySet()) {
            logger.logf(LogMessage.DEBUG, "Running key %s with %d entries", item.getKey().toString(), item.getValue().size());
            mergeKeys(item.getKey(), item.getValue());
        }

        Set<String> allKeys = input.values().stream()
                .flatMap(Collection::stream)
                .map(MarketDataEntry::getKey)
                .collect(Collectors.toSet());
        archiveKeys(allKeys);

        return input.keySet().stream()
                .map(entry -> securityMaster.getListing(entry.getExchangeId(), entry.getSecurityId()))
                .collect(Collectors.toSet());
    }

    private void archiveKeys(Set<String> keys) {
        logger.logf(LogMessage.DEBUG, "Moving %s keys to archive bucket", keys.size());

        List<ObjectIdentifier> targetKeys = keys.stream()
                .map(key -> ObjectIdentifier.builder().key(key).build())
                .toList();
        for (ObjectIdentifier object : targetKeys) {
            var copyResponse = s3Client.copyObject(request -> request
                    .destinationBucket(archiveBucket)
                    .destinationKey(object.key())
                    .sourceBucket(inputBucket)
                    .sourceKey(object.key())
            );
            logger.logf(LogMessage.DEBUG, "Copied key %s to archive bucket", object.key());
            if (!copyResponse.sdkHttpResponse().isSuccessful()) {
                throw new RuntimeException("Failed to copy key %s to archive bucket".formatted(object.key()));
            }
        }

        var response = s3Client.deleteObjects(
                request -> request
                        .bucket(inputBucket)
                        .delete(delete -> delete.objects(targetKeys))
        );
        if (response.hasErrors()) {
            for (S3Error error : response.errors()) {
                logger.logf(LogMessage.UNKNOWN_ERROR, "Error deleting key %s: %s", error.key(), error.message());
            }
            throw new RuntimeException("Error while deleting keys. Please check the logs");
        } else {
            logger.logf(LogMessage.DEBUG, "Successfully deleted keys");
        }
    }

    private void mergeKeys(MarketDataEntry mergedEntry, List<MarketDataEntry> rawEntries) {
        Map<String, List<Schema>> entries = new LinkedHashMap<>();
        for (MarketDataEntry entry : rawEntries) {
            entries.put(entry.getUUID(), entry.loadFromS3(s3Client, inputBucket));
        }

        int totalRecords = entries.values().stream().mapToInt(List::size).sum();
        SchemaMergeStrategy strategy = getMergeStrategy(mergedEntry.getSchemaType());
        var outputEntries = strategy.mergeRecords(logger, entries);

        try {
            mergedEntry.saveToS3(s3Client, outputBucket, outputEntries);
        } catch (IOException e) {
            logger.logf(LogMessage.UNKNOWN_ERROR, "Error trying to write merged key: %s", e.getMessage());
            throw new RuntimeException(e);
        }
        logger.logf(LogMessage.DEBUG, "Wrote %d (out of %d) records to merged key %s", outputEntries.size(), totalRecords, mergedEntry);
    }

    /**
     * Collects all the raw files in the input bucket that are older than the merge delay.
     *
     * @return a map of merged entries to the list of raw entries that make up the merged entry
     */
    private Map<MarketDataEntry, List<MarketDataEntry>> collectRawFiles() {
        var paginator = this.s3Client.listObjectsV2Paginator(request -> request.bucket(this.inputBucket));
        Map<MarketDataEntry, List<MarketDataEntry>> outputFiles = new LinkedHashMap<>();

        for (var s3Object : paginator.contents()) {
            MarketDataEntry entry = MarketDataEntry.fromKey(s3Object.key());
            assert entry.getEntryType() == MarketDataEntry.EntryType.RAW : "Expected raw entry, got " + entry.getEntryType();

            if (!entry.getTimestamp().plus(MERGE_DELAY).isBefore(LocalDateTime.now(clock))) {
                continue;
            }

            MarketDataEntry mergedEntry = new MarketDataEntry(entry.getSecurityId(), entry.getExchangeId(), entry.getSchemaType(), entry.getTimestamp(), MarketDataEntry.EntryType.AGGREGATED);
            outputFiles.computeIfAbsent(mergedEntry, k -> new ArrayList<>()).add(entry);
        }
        return outputFiles;
    }

    private SchemaMergeStrategy getMergeStrategy(SchemaType schemaType) {
        return switch (schemaType) {
            case MBP_10 -> new MBP10MergeStrategy();
            default -> throw new IllegalArgumentException("Unsupported schema type: " + schemaType);
        };
    }
}
