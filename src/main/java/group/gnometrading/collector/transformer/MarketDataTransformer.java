package group.gnometrading.collector.transformer;

import group.gnometrading.collector.MarketDataEntry;
import group.gnometrading.logging.LogMessage;
import group.gnometrading.logging.Logger;
import group.gnometrading.schemas.Schema;
import group.gnometrading.schemas.SchemaType;
import group.gnometrading.schemas.converters.SchemaConversionRegistry;
import group.gnometrading.schemas.converters.SchemaConverter;
import group.gnometrading.sm.Listing;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * This class takes in the consolidated stream produced by MarketDataMerger
 * and transforms it into lower-level schemas.
 */
public class MarketDataTransformer {

    private final Logger logger;
    private final Clock clock;
    private final S3Client s3Client;
    private final DynamoDbClient dynamoDbClient;
    private final String tableName;
    private final String bucket;

    public MarketDataTransformer(
            Logger logger,
            Clock clock,
            S3Client s3Client,
            DynamoDbClient dynamoDbClient,
            String tableName,
            String bucket
    ) {
        this.logger = logger;
        this.clock = clock;
        this.s3Client = s3Client;
        this.dynamoDbClient = dynamoDbClient;
        this.tableName = tableName;
        this.bucket = bucket;
    }

    public void runTransformer(Set<Listing> keys) {
        for (Listing listing : keys) {
            logger.logf(LogMessage.DEBUG, "Running transformation for listing %s", listing);
            transformKey(listing);
        }
    }

    private void transformKey(Listing listing) {
        for (SchemaType toSchemaType : SchemaType.values()) {
            if (toSchemaType == listing.exchange().schemaType() || !SchemaConversionRegistry.hasConverter(listing.exchange().schemaType(), toSchemaType)) {
                continue;
            }
            transformKey(listing, toSchemaType);
        }
    }

    private void transformKey(Listing listing, SchemaType toSchemaType) {
        logger.logf(LogMessage.DEBUG, "Transforming listing %s to %s", listing, toSchemaType);

        var response = dynamoDbClient.getItem(request -> request
                .tableName(tableName)
                .key(
                        Map.of(
                                "listingId", AttributeValue.builder().n(String.valueOf(listing.listingId())).build(),
                                "schemaType", AttributeValue.builder().s(toSchemaType.getIdentifier()).build()
                        )
                )
        );
        List<MarketDataEntry> keys;
        if (response.hasItem() && response.item().containsKey("nextAggregationTimestamp")) {
            LocalDateTime nextAggregationTimestamp = LocalDateTime.parse(response.item().get("nextAggregationTimestamp").s());
            keys = getKeysForListingIteratively(listing, nextAggregationTimestamp);
        } else {
            keys = MarketDataEntry.getAllKeysForListing(s3Client, bucket, listing);
        }

        if (keys.isEmpty()) {
            logger.logf(LogMessage.DEBUG, "No keys found for listing %s", listing);
            return;
        }

        List<Schema> schemas = keys.parallelStream()
                .map(key -> key.loadFromS3(s3Client, bucket))
                .flatMap(Collection::stream)
                .toList();
        SchemaConverter converter = SchemaConversionRegistry.getConverter(listing.exchange().schemaType(), toSchemaType);
        Schema lastSchemaTransformed = null;
        List<Schema> outputSchemas = new ArrayList<>();
        for (Schema schema : schemas) {
            Schema transformed = converter.convert(schema);
            if (transformed != null ) {
                Schema toSave = toSchemaType.newInstance();
                toSave.copyFrom(transformed);
                outputSchemas.add(toSave);

                lastSchemaTransformed = schema;
            }
        }

        if (lastSchemaTransformed == null) {
            logger.logf(LogMessage.DEBUG, "No schemas transformed for listing %s", listing);
            return;
        }

        Map<MarketDataEntry, List<Schema>> outputEntries = groupSchemasByEntry(listing, toSchemaType, outputSchemas);
        outputEntries.entrySet().parallelStream().forEach(entry -> {
            try {
                entry.getKey().saveToS3(s3Client, bucket, entry.getValue());
            } catch (IOException e) {
                logger.logf(LogMessage.UNKNOWN_ERROR, "Error trying to write transformed key: %s", e.getMessage());
                throw new RuntimeException(e);
            }
        });

        var putItemRequest = PutItemRequest.builder()
                .tableName(tableName)
                .item(
                    Map.of(
                        "listingId", AttributeValue.builder().n(String.valueOf(listing.listingId())).build(),
                        "schemaType", AttributeValue.builder().s(toSchemaType.getIdentifier()).build(),
                        "nextAggregationTimestamp", AttributeValue.builder().s(lastSchemaTransformed.toString()).build()
                    )
                ).build();
        dynamoDbClient.putItem(putItemRequest);
        logger.logf(LogMessage.DEBUG, "Successfully transformed listing %s to %s", listing, toSchemaType);
    }

    private LocalDateTime getSchemaDateTime(Schema schema) {
        long epochMillis = TimeUnit.NANOSECONDS.toMillis(schema.getEventTimestamp());
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(epochMillis), clock.getZone()).truncatedTo(MarketDataEntry.CYCLE_CHRONO_UNIT);
    }

    private Map<MarketDataEntry, List<Schema>> groupSchemasByEntry(Listing listing, SchemaType toSchemaType, List<Schema> schemas) {
        Map<MarketDataEntry, List<Schema>> outputEntries = new LinkedHashMap<>();
        for (Schema schema : schemas) {
            LocalDateTime timestamp = getSchemaDateTime(schema);
            MarketDataEntry entry = new MarketDataEntry(listing.security().securityId(), listing.exchange().exchangeId(), toSchemaType, timestamp, MarketDataEntry.EntryType.AGGREGATED);
            outputEntries.computeIfAbsent(entry, k -> new ArrayList<>()).add(schema);
        }
        return outputEntries;
    }

    private List<MarketDataEntry> getKeysForListingIteratively(Listing listing, LocalDateTime nextDateTime) {
        LocalDateTime currentDateTime = nextDateTime.truncatedTo(ChronoUnit.DAYS);
        LocalDateTime endDate = LocalDateTime.now(clock).truncatedTo(ChronoUnit.DAYS).plusDays(1);
        List<MarketDataEntry> keys = new ArrayList<>();
        while (currentDateTime.isBefore(endDate)) {
            keys.addAll(MarketDataEntry.getKeysForListingByDay(s3Client, bucket, listing, currentDateTime));
            currentDateTime = currentDateTime.plusDays(1);
        }
        return keys.stream().filter(key -> !key.getTimestamp().isBefore(nextDateTime)).toList();
    }
}
