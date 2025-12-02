package group.gnometrading.collector;

import com.github.luben.zstd.ZstdOutputStream;
import com.lmax.disruptor.EventHandler;
import group.gnometrading.logging.LogMessage;
import group.gnometrading.logging.Logger;
import group.gnometrading.schemas.Schema;
import group.gnometrading.schemas.SchemaType;
import group.gnometrading.sm.Listing;
import org.agrona.ExpandableArrayBuffer;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.UUID;

public class MarketDataCollector implements EventHandler<Schema>, Closeable {

    private static final DateTimeFormatter TIME_FORMAT = DateTimeFormatter.ofPattern("yyyyMMddHHmm");

    private final Logger logger;
    private final Clock clock;
    private final S3Client s3Client;
    private final Listing listing;
    private final String bucketName;
    private final SchemaType schemaType;
    private final Duration cycleDuration;

    private String key;

    private final ExpandableArrayBuffer purgatory;
    private final ByteArrayOutputStream rawBuffer;
    private ZstdOutputStream outputStream;

    private LocalDateTime cycleStart;
    private volatile boolean closed = false;

    public MarketDataCollector(
            Logger logger,
            Clock clock,
            S3Client s3Client,
            Listing listing,
            String bucketName,
            SchemaType schemaType
    ) {
        this.logger = logger;
        this.clock = clock;
        this.s3Client = s3Client;
        this.listing = listing;
        this.bucketName = bucketName;
        this.schemaType = schemaType;
        this.cycleDuration = mapSchemaToCycleDuration(schemaType);
        this.purgatory = new ExpandableArrayBuffer(1 << 12);
        this.rawBuffer = new ByteArrayOutputStream();

        this.cycleStart = LocalDateTime.now(clock).truncatedTo(getCycleChronoUnit());
        this.openNewStream();
        this.attachShutdownHook();
    }

    public void onEvent(final Schema schema, long sequence, boolean endOfBatch) throws Exception {
        if (closed) {
            return;
        }

        long epochSeconds = schema.getEventTimestamp() / 1_000_000_000L;
        long nanoAdjustment = schema.getEventTimestamp() % 1_000_000_000L;

        Instant instant = Instant.ofEpochSecond(epochSeconds, nanoAdjustment);
        LocalDateTime now = LocalDateTime.ofInstant(instant, clock.getZone());

        if (!now.minus(cycleDuration).isBefore(this.cycleStart)) {
            logger.logf(LogMessage.DEBUG, "Switching cycle to %s from %s", now.truncatedTo(getCycleChronoUnit()), this.cycleStart);
            this.uploadFile();

            this.cycleStart = now.truncatedTo(getCycleChronoUnit());
            this.openNewStream();
        }

        try {
            schema.buffer.getBytes(0, this.purgatory, 0, schema.totalMessageSize());
            outputStream.write(this.purgatory.byteArray(), 0, schema.totalMessageSize());
        } catch (IOException e) {
            logger.logf(LogMessage.UNKNOWN_ERROR, "Error trying to write to file stream: %s", e.getMessage());
            throw new RuntimeException(e);
        }
    }

    private String buildKey() {
        String uniqueId = UUID.randomUUID().toString().substring(0, 8);
        return "%d/%d/%s/%s/%s.zst".formatted(
                listing.securityId(),
                listing.exchangeId(),
                this.cycleStart.format(TIME_FORMAT),
                this.schemaType.getIdentifier(),
                uniqueId
        );
    }

    private void uploadFile() throws IOException {
        this.outputStream.close();

        if (rawBuffer.size() == 0) {
            logger.logf(LogMessage.DEBUG, "Skipping upload for empty buffer");
            return;
        }

        logger.logf(LogMessage.DEBUG, "Uploading file: %s", this.key);
        var request = PutObjectRequest.builder()
                .bucket(this.bucketName)
                .key(this.key)
                .build();
        this.s3Client.putObject(request, RequestBody.fromBytes(this.rawBuffer.toByteArray()));
    }

    private void openNewStream() {
        this.key = buildKey();
        logger.logf(LogMessage.DEBUG, "Opening new S3 file stream: %s", this.key);

        try {
            this.rawBuffer.reset();
            this.outputStream = new ZstdOutputStream(this.rawBuffer);
        } catch (IOException e) {
            logger.logf(LogMessage.UNKNOWN_ERROR, "Error trying to create new file stream: %s", e.getMessage());
            throw new RuntimeException(e);
        }
    }

    private void attachShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                this.logger.logf(LogMessage.DEBUG, "Market data collector is exiting... attempting to cycle files.");
                this.close();
            } catch (Exception e) {
                this.logger.logf(LogMessage.UNKNOWN_ERROR, "Error trying to cycle files: %s", e.getMessage());
            }
        }));
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            logger.logf(LogMessage.DEBUG, "MarketDataCollector already closed, skipping");
            return;
        }

        closed = true;

        this.uploadFile();

        logger.logf(LogMessage.DEBUG, "MarketDataCollector closed successfully");
    }

    private Duration mapSchemaToCycleDuration(SchemaType schemaType) {
        return switch (schemaType) {
            case MBO, MBP_10, MBP_1, BBO_1S, BBO_1M, TRADES, OHLCV_1S, OHLCV_1M -> Duration.ofMinutes(1);
            case OHLCV_1H -> Duration.ofHours(1);
        };
    }

    private ChronoUnit getCycleChronoUnit() {
        if (cycleDuration.toMinutes() == 1) {
            return ChronoUnit.MINUTES;
        } else if (cycleDuration.toHours() == 1) {
            return ChronoUnit.HOURS;
        } else if (cycleDuration.toDays() == 1) {
            return ChronoUnit.DAYS;
        }
        throw new IllegalArgumentException("Unsupported cycle duration: " + cycleDuration);
    }
}
