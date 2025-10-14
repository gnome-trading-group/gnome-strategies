package group.gnometrading.collector;

import com.github.luben.zstd.ZstdOutputStream;
import group.gnometrading.schemas.Schema;
import group.gnometrading.schemas.SchemaType;
import group.gnometrading.sm.Listing;
import org.agrona.ExpandableArrayBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Clock;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;

class MarketDataCollector {

    private static final String OUTPUT_DIRECTORY = "market-data";
    private static final DateTimeFormatter HOUR_FORMAT = DateTimeFormatter.ofPattern("yyyyMMddHH");
    private static final Logger logger = LoggerFactory.getLogger(MarketDataCollector.class);

    private final Clock clock;
    private final S3Client s3Client;
    private final Listing listing;
    private final String bucketName;
    private final SchemaType schemaType;
    private final ExpandableArrayBuffer purgatory;

    private ZstdOutputStream currentFileStream;
    private LocalDateTime currentHour;
    private String currentFileName;

    public MarketDataCollector(
            Clock clock,
            S3Client s3Client,
            Listing listing,
            String bucketName,
            SchemaType schemaType
    ) {
        this.clock = clock;
        this.s3Client = s3Client;
        this.listing = listing;
        this.bucketName = bucketName;
        this.schemaType = schemaType;
        this.purgatory = new ExpandableArrayBuffer(1 << 16); // 64kb
        this.currentHour = LocalDateTime.now(this.clock);
        openNewFile();
    }

    public void onEvent(final Schema schema) throws Exception {
        LocalDateTime now = LocalDateTime.now(this.clock);
        if (!now.truncatedTo(ChronoUnit.HOURS).equals(currentHour.truncatedTo(ChronoUnit.HOURS))) {
            logger.info("Switching hour to {} from {}", now.truncatedTo(ChronoUnit.HOURS), currentHour.truncatedTo(ChronoUnit.HOURS));
            cycleFile();
            currentHour = now;
            openNewFile();
        }

        try {
            schema.buffer.getBytes(0, this.purgatory, 0, schema.totalMessageSize());
            currentFileStream.write(this.purgatory.byteArray(), 0, schema.totalMessageSize());
        } catch (IOException e) {
            logger.error("Error trying to write to file stream", e);
            throw new RuntimeException(e);
        }
    }

    public void cycleFile() {
        try {
            if (currentFileStream != null) {
                currentFileStream.close();
            }

            uploadToS3();
            Files.deleteIfExists(Paths.get(currentFileName));

            logger.info("File uploaded to S3 and deleted locally: {}", currentFileName);
        } catch (IOException e) {
            throw new RuntimeException("Error cycling file", e);
        }
    }

    private String buildKey() {
        return "%d/%d/%s/%s.zst".formatted(listing.securityId(), listing.exchangeId(), currentHour.format(HOUR_FORMAT), this.schemaType.getIdentifier());
    }

    private void uploadToS3() {
        try {
            final File file = new File(this.currentFileName);
            final String key = buildKey();

            PutObjectRequest request = PutObjectRequest.builder()
                    .bucket(this.bucketName)
                    .key(key)
                    .build();

            s3Client.putObject(request, file.toPath());
            logger.info("Uploaded file to S3: {}", key);

        } catch (S3Exception e) {
            throw new RuntimeException("Failed to upload file to S3: " + e.getMessage(), e);
        }
    }

    private void openNewFile() {
        currentFileName = "./%s/%d/%s/%s.zst".formatted(OUTPUT_DIRECTORY, this.listing.listingId(), this.schemaType.getIdentifier(), currentHour.format(HOUR_FORMAT));
        logger.info("Opening new file: {}", currentFileName);
        try {
            File targetFile = new File(currentFileName);
            File parent = targetFile.getParentFile();
            if (parent != null && !parent.exists() && !parent.mkdirs()) {
                throw new IllegalStateException("Unable to create directory: " + parent);
            }
            currentFileStream = new ZstdOutputStream(new FileOutputStream(currentFileName));
        } catch (IOException e) {
            throw new RuntimeException("Error while creating new file", e);
        }
    }
}
