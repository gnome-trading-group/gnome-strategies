package group.gnometrading.collector;

import com.lmax.disruptor.EventHandler;
import group.gnometrading.schemas.Schema;
import group.gnometrading.schemas.SchemaType;
import group.gnometrading.schemas.converters.SchemaConversionRegistry;
import group.gnometrading.schemas.converters.SchemaConverter;
import group.gnometrading.sm.Listing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;

import java.time.Clock;
import java.util.HashMap;
import java.util.Map;

public class BulkMarketDataCollector implements EventHandler<Schema> {

    private static final Logger logger = LoggerFactory.getLogger(BulkMarketDataCollector.class);

    private final Map<SchemaType, MarketDataCollector> collectors;
    private final Map<SchemaType, SchemaConverter<Schema, Schema>> converters;
    private final SchemaType originalType;

    @SuppressWarnings("unchecked")
    public BulkMarketDataCollector(
            Clock clock,
            S3Client s3Client,
            Listing listing,
            String bucketName,
            SchemaType originalType
    ) {
        this.originalType = originalType;
        this.collectors = new HashMap<>();
        this.converters = new HashMap<>();
        for (SchemaType other : SchemaType.values()) {
            if (other == this.originalType) {
                this.collectors.put(other, new MarketDataCollector(clock, s3Client, listing, bucketName, other));
            } else if (SchemaConversionRegistry.hasConverter(originalType, other)) {
                this.collectors.put(other, new MarketDataCollector(clock, s3Client, listing, bucketName, other));
                this.converters.put(other, (SchemaConverter<Schema, Schema>) SchemaConversionRegistry.getConverter(this.originalType, other));
            }
        }
    }

    @Override
    public void onEvent(Schema schema, long sequence, boolean endOfBatch) throws Exception {
        for (var item : this.collectors.entrySet()) {
            if (item.getKey() == this.originalType) {
                item.getValue().onEvent(schema);
            } else {
                Schema conversion = this.converters.get(item.getKey()).convert(schema);
                if (conversion != null) {
                    item.getValue().onEvent(conversion);
                }
            }
        }
    }

    @Override
    public void onShutdown() {
        logger.info("Market data collector is exiting... attempting to cycle files.");
        for (MarketDataCollector collector : this.collectors.values()) {
            collector.cycleFile();
        }
    }
}
