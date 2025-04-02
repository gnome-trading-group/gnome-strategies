package group.gnometrading.collector;

import group.gnometrading.ipc.IPCManager;
import group.gnometrading.schemas.Schema;
import group.gnometrading.schemas.SchemaType;
import group.gnometrading.schemas.converters.SchemaConversionRegistry;
import group.gnometrading.schemas.converters.SchemaConverter;
import group.gnometrading.sm.Listing;
import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.Agent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;

import java.time.Clock;
import java.util.HashMap;
import java.util.Map;

public class BulkMarketDataCollector implements FragmentHandler, Agent {

    private static final int FRAGMENT_LIMIT = 1;
    private static final Logger logger = LoggerFactory.getLogger(BulkMarketDataCollector.class);

    private final Subscription subscription;
    private final Map<SchemaType, MarketDataCollector> collectors;
    private final Map<SchemaType, SchemaConverter<Schema<?, ?>, Schema<?, ?>>> converters;
    private final SchemaType originalType;

    @SuppressWarnings("unchecked")
    public BulkMarketDataCollector(
            Clock clock,
            IPCManager ipcManager,
            String streamName,
            S3Client s3Client,
            Listing listing,
            String bucketName,
            String identifier,
            SchemaType originalType
    ) {
        this.subscription = ipcManager.addSubscription(streamName);
        this.originalType = originalType;

        this.collectors = new HashMap<>();
        this.converters = new HashMap<>();
        for (SchemaType other : SchemaType.values()) {
            if (other == this.originalType) {
                this.collectors.put(other, new MarketDataCollector(clock, s3Client, listing, bucketName, identifier, other));
            } else if (SchemaConversionRegistry.hasConverter(originalType, other)) {
                this.collectors.put(other, new MarketDataCollector(clock, s3Client, listing, bucketName, identifier, other));
                this.converters.put(other, (SchemaConverter<Schema<?, ?>, Schema<?, ?>>) SchemaConversionRegistry.getConverter(this.originalType, other));
            }
        }
    }

    @Override
    public void onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header) {
        for (var item : this.collectors.entrySet()) {
            if (item.getKey() == this.originalType) {
                item.getValue().onFragment(buffer, offset, length, header);
            } else {
                assert length == this.originalType.getInstance().totalMessageSize() : "Invalid length";
                this.originalType.getInstance().buffer.putBytes(0, buffer, offset, length);
                Schema<?, ?> conversion = this.converters.get(item.getKey()).convert(this.originalType.getInstance());
                if (conversion != null) {
                    item.getValue().onFragment(conversion.buffer, 0, conversion.totalMessageSize(), null);
                }
            }
        }
    }

    @Override
    public int doWork() throws Exception {
        this.subscription.poll(this, FRAGMENT_LIMIT);
        return 0; // TODO: Do we want to sleep on no fragments? Or return priority > 0?
    }

    @Override
    public void onClose() {
        logger.info("Agent is exiting... attempting to cycle files.");
        for (MarketDataCollector collector : this.collectors.values()) {
            collector.cycleFile();
        }
    }

    @Override
    public String roleName() {
        return "collector";
    }
}
