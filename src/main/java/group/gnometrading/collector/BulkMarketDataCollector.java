package group.gnometrading.collector;

import com.lmax.disruptor.EventHandler;
import group.gnometrading.logging.LogMessage;
import group.gnometrading.logging.Logger;
import group.gnometrading.schemas.Schema;
import group.gnometrading.schemas.SchemaType;
import group.gnometrading.schemas.converters.SchemaConversionRegistry;
import group.gnometrading.schemas.converters.SchemaConverter;
import group.gnometrading.sm.Listing;
import software.amazon.awssdk.services.s3.S3Client;

import java.time.Clock;
import java.util.HashMap;
import java.util.Map;

public class BulkMarketDataCollector implements EventHandler<Schema> {

    private final Logger logger;
    private final Map<SchemaType, MarketDataCollector> collectors;
    private final Map<SchemaType, SchemaConverter<Schema, Schema>> converters;
    private final SchemaType originalType;

    @SuppressWarnings("unchecked")
    public BulkMarketDataCollector(
            Logger logger,
            Clock clock,
            S3Client s3Client,
            Listing listing,
            String bucketName,
            SchemaType originalType
    ) {
        this.logger = logger;
        this.originalType = originalType;
        this.collectors = new HashMap<>();
        this.converters = new HashMap<>();
        for (SchemaType other : SchemaType.values()) {
            if (other == this.originalType) {
                this.collectors.put(other, new MarketDataCollector(logger, clock, s3Client, listing, bucketName, other));
            } else if (SchemaConversionRegistry.hasConverter(originalType, other)) {
                this.collectors.put(other, new MarketDataCollector(logger, clock, s3Client, listing, bucketName, other));
                this.converters.put(other, (SchemaConverter<Schema, Schema>) SchemaConversionRegistry.getConverter(this.originalType, other));
            }
        }
    }

    @Override
    public void onEvent(Schema schema, long sequence, boolean endOfBatch) throws Exception {
        for (var item : this.collectors.entrySet()) {
            if (item.getKey() == this.originalType) {
                item.getValue().onEvent(schema, sequence, endOfBatch);
            } else {
                Schema conversion = this.converters.get(item.getKey()).convert(schema);
                if (conversion != null) {
                    item.getValue().onEvent(conversion, sequence, endOfBatch);
                }
            }
        }
    }

    @Override
    public void onShutdown() {
        logger.logf(LogMessage.DEBUG, "Market data collector is exiting... attempting to cycle files.");
        for (MarketDataCollector collector : this.collectors.values()) {
            try {
                collector.close();
            } catch (Exception e) {
                logger.logf(LogMessage.UNKNOWN_ERROR, "Error trying to cycle files: %s", e.getMessage());
            }
        }
    }
}
