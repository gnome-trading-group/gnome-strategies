package group.gnometrading.collector;

import com.github.luben.zstd.ZstdOutputStream;
import group.gnometrading.ipc.IPCManager;
import group.gnometrading.objects.MarketUpdateDecoder;
import group.gnometrading.objects.MessageHeaderDecoder;
import group.gnometrading.resources.Properties;
import group.gnometrading.sm.Listing;
import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.Agent;
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
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;

public class MarketUpdateCollector implements FragmentHandler, Agent {

    private static final int FRAGMENT_LIMIT = 1;
    private static final String OUTPUT_DIRECTORY = "./market-data/";
    private static final String S3_BUCKET_KEY = "collector.s3.bucket";
    private static final DateTimeFormatter HOUR_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd_HH");
    private static final Logger logger = LoggerFactory.getLogger(MarketUpdateCollector.class);

    private final Subscription subscription;
    private final S3Client s3Client;
    private final MarketUpdateDecoder decoder;
    private final MessageHeaderDecoder headerDecoder;
    private final Listing listing;
    private final String bucketName;
    private final ExpandableArrayBuffer purgatory;

    private ZstdOutputStream currentFileStream;
    private LocalDateTime currentHour;
    private String currentFileName;

    public MarketUpdateCollector(
            IPCManager ipcManager,
            String streamName,
            S3Client s3Client,
            Listing listing,
            Properties properties
    ) {
        this.s3Client = s3Client;
        this.listing = listing;
        this.subscription = ipcManager.addSubscription(streamName);
        this.bucketName = properties.getStringProperty(S3_BUCKET_KEY);
        this.purgatory = new ExpandableArrayBuffer(1 << 14);
        this.decoder = new MarketUpdateDecoder();
        this.headerDecoder = new MessageHeaderDecoder();
        this.currentHour = LocalDateTime.now(ZoneOffset.UTC);
        openNewFile();
    }

    @Override
    public void onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header) {
        this.decoder.wrapAndApplyHeader(buffer, offset, this.headerDecoder);
        // TODO: Should we use the local clock time or the timestamp of the event? Does it matter?

        LocalDateTime now = LocalDateTime.now(ZoneOffset.UTC);
        if (!now.truncatedTo(ChronoUnit.HOURS).equals(currentHour.truncatedTo(ChronoUnit.HOURS))) {
            cycleFile();
            currentHour = now;
            openNewFile();
        }

        try {
            buffer.getBytes(offset, this.purgatory, 0, length);
            currentFileStream.write(this.purgatory.byteArray(), 0, length);
        } catch (IOException e) {
            logger.error("Error trying to write to file stream", e);
            throw new RuntimeException(e);
        }
    }

    private void cycleFile() {
        try {
            if (currentFileStream != null) {
                currentFileStream.close();
            }

            uploadToS3(currentFileName);
            Files.deleteIfExists(Paths.get(currentFileName));

            logger.info("File uploaded to S3 and deleted locally: {}", currentFileName);
        } catch (IOException e) {
            throw new RuntimeException("Error cycling file", e);
        }
    }

    private String buildKey(final String fileName) {
        String[] parts = fileName.substring(0, fileName.lastIndexOf('.')).split("_");
        String name = (
                this.listing.exchangeSecuritySymbol() == null || this.listing.exchangeSecuritySymbol().isEmpty()
        ) ? "dump" : this.listing.exchangeSecuritySymbol();
        return listing.exchangeId() + "/" + listing.securityId() + "/" + parts[0] + "/" + parts[1] + "/" + name + ".zst";
    }

    private void uploadToS3(final String filePath) {
        try {
            final File file = new File(filePath);
            final String key = buildKey(file.getName());

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
        currentFileName = OUTPUT_DIRECTORY + currentHour.format(HOUR_FORMAT) + ".zst";
        logger.info("Opening new file: {}", currentFileName);
        try {
            Files.createDirectories(Paths.get(OUTPUT_DIRECTORY));
            currentFileStream = new ZstdOutputStream(new FileOutputStream(currentFileName));
        } catch (IOException e) {
            throw new RuntimeException("Error while creating new file", e);
        }
    }

    @Override
    public int doWork() throws Exception {
        this.subscription.poll(this, FRAGMENT_LIMIT);
        return 0; // TODO: Do we want to sleep on no fragments? Or return priority > 0?
    }

    @Override
    public String roleName() {
        return "collector";
    }
}
