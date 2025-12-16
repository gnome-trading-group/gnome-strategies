package group.gnometrading.collector;

import com.github.luben.zstd.ZstdInputStream;
import com.github.luben.zstd.ZstdOutputStream;
import group.gnometrading.schemas.Schema;
import group.gnometrading.schemas.SchemaType;
import group.gnometrading.sm.Listing;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class MarketDataEntry {

    private static final Pattern AGGREGATED_FILE_PATTERN = Pattern.compile(
            "^(\\d+)/(\\d+)/(\\d+)/(\\d+)/(\\d+)/(\\d+)/(\\d+)/([^/]+)\\.zst$"
    );

    private static final Pattern RAW_FILE_PATTERN = Pattern.compile(
            "^(\\d+)/(\\d+)/(\\d+)/(\\d+)/(\\d+)/(\\d+)/(\\d+)/([^/]+)/([^/]+)\\.zst$"
    );

    public static final ChronoUnit CYCLE_CHRONO_UNIT = ChronoUnit.MINUTES;

    public enum EntryType {
        RAW,
        AGGREGATED
    }

    private final int securityId;
    private final int exchangeId;
    private final SchemaType schemaType;
    private final LocalDateTime timestamp;
    private final EntryType entryType;
    private final String uuid;

    public MarketDataEntry(Listing listing, LocalDateTime timestamp, EntryType entryType) {
        this(listing.securityId(), listing.exchangeId(), listing.schemaType(), timestamp, entryType, UUID.randomUUID().toString().substring(0, 8));
    }

    public MarketDataEntry(Listing listing, LocalDateTime timestamp, EntryType entryType, String uuid) {
        this(listing.securityId(), listing.exchangeId(), listing.schemaType(), timestamp, entryType, uuid);
    }

    public MarketDataEntry(int securityId, int exchangeId, SchemaType schemaType, LocalDateTime timestamp, EntryType entryType) {
        this(securityId, exchangeId, schemaType, timestamp, entryType, UUID.randomUUID().toString().substring(0, 8));
    }

    public MarketDataEntry(int securityId, int exchangeId, SchemaType schemaType, LocalDateTime timestamp, EntryType entryType, String uuid) {
        this.securityId = securityId;
        this.exchangeId = exchangeId;
        this.schemaType = schemaType;
        this.timestamp = timestamp;
        this.entryType = entryType;
        this.uuid = uuid;
    }

    public List<Schema> loadFromS3(S3Client s3Client, String bucket) {
        InputStream stream = s3Client.getObject(request -> request.bucket(bucket).key(this.getKey()));
        List<Schema> schemas = new ArrayList<>();
        int expectedSize = this.schemaType.getInstance().totalMessageSize();

        try (ZstdInputStream zstdStream = new ZstdInputStream(stream);
             ByteArrayOutputStream buffer = new ByteArrayOutputStream()) {
            zstdStream.transferTo(buffer);
            byte[] decompressedData = buffer.toByteArray();

            int i;
            for (i = 0; i < decompressedData.length; i += expectedSize) {
                byte[] recordData = Arrays.copyOfRange(decompressedData, i, i + expectedSize);
                Schema schema = this.schemaType.newInstance();
                schema.buffer.putBytes(0, recordData, 0, recordData.length);
                schemas.add(schema);
            }
            assert i == decompressedData.length : "Left over bytes in key %s: %d".formatted(this.getKey(), decompressedData.length - i);
            return schemas;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void saveToS3(S3Client s3Client, String bucket, List<Schema> schemas) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        for (Schema schema : schemas) {
            byte[] schemaBytes = new byte[schema.totalMessageSize()];
            schema.buffer.getBytes(0, schemaBytes, 0, schema.totalMessageSize());
            outputStream.write(schemaBytes);
        }
        byte[] uncompressedData = outputStream.toByteArray();

        ByteArrayOutputStream compressedOutput = new ByteArrayOutputStream();
        try (ZstdOutputStream zstdStream = new ZstdOutputStream(compressedOutput)) {
            zstdStream.write(uncompressedData);
        }
        saveToS3(s3Client, bucket, compressedOutput.toByteArray());
    }

    public void saveToS3(S3Client s3Client, String bucket, byte[] data) {
        String key = getKey();
        s3Client.putObject(
                request -> request.key(key).bucket(bucket),
                RequestBody.fromBytes(data)
        );
    }

    public int getSecurityId() {
        return securityId;
    }

    public int getExchangeId() {
        return exchangeId;
    }

    public SchemaType getSchemaType() {
        return schemaType;
    }

    public LocalDateTime getTimestamp() {
        return timestamp;
    }

    public EntryType getEntryType() {
        return entryType;
    }

    public String getUUID() {
        return uuid;
    }

    public String getKey() {
        return switch (entryType) {
            case AGGREGATED -> "%d/%d/%d/%d/%d/%d/%d/%s.zst".formatted(
                    securityId,
                    exchangeId,
                    timestamp.getYear(),
                    timestamp.getMonthValue(),
                    timestamp.getDayOfMonth(),
                    timestamp.getHour(),
                    timestamp.getMinute(),
                    schemaType.getIdentifier()
            );
            case RAW -> "%d/%d/%d/%d/%d/%d/%d/%s/%s.zst".formatted(
                    securityId,
                    exchangeId,
                    timestamp.getYear(),
                    timestamp.getMonthValue(),
                    timestamp.getDayOfMonth(),
                    timestamp.getHour(),
                    timestamp.getMinute(),
                    schemaType.getIdentifier(),
                    uuid
            );
        };
    }

    @Override
    public String toString() {
        return "MarketDataEntry{" +
                "securityId=" + securityId +
                ", exchangeId=" + exchangeId +
                ", schemaType=" + schemaType +
                ", timestamp=" + timestamp +
                ", entryType=" + entryType +
                ", uuid='" + uuid + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MarketDataEntry that = (MarketDataEntry) o;
        return securityId == that.securityId &&
                exchangeId == that.exchangeId &&
                schemaType == that.schemaType &&
                Objects.equals(timestamp, that.timestamp) &&
                entryType == that.entryType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(securityId, exchangeId, schemaType, timestamp, entryType);
    }

    public static List<MarketDataEntry> getAllKeysForListing(S3Client s3Client, String bucket, Listing listing) {
        return getKeysForListingByDay(s3Client, bucket, listing, null);
    }

    public static List<MarketDataEntry> getKeysForListingByDay(S3Client s3Client, String bucket, Listing listing, LocalDateTime day) {
        final String keyPrefix;
        if (day != null) {
            keyPrefix = "%d/%d/%d/%d/%d/".formatted(
                    listing.securityId(),
                    listing.exchangeId(),
                    day.getYear(),
                    day.getMonthValue(),
                    day.getDayOfMonth()
            );
        } else {
            keyPrefix = "%d/%d/".formatted(
                    listing.securityId(),
                    listing.exchangeId()
            );
        }
        var response = s3Client.listObjectsV2Paginator(request ->
                request
                        .bucket(bucket)
                        .prefix(keyPrefix)
        );
        return response.contents().stream()
                .map(S3Object::key)
                .map(MarketDataEntry::fromKey)
                .sorted(Comparator.comparing(MarketDataEntry::getTimestamp))
                .collect(Collectors.toList());
    }

    public static MarketDataEntry fromKey(String key) {
        if (AGGREGATED_FILE_PATTERN.matcher(key).matches()) {
            return fromAggregatedKey(key);
        }
        if (RAW_FILE_PATTERN.matcher(key).matches()) {
            return fromRawKey(key);
        }
        throw new IllegalArgumentException("Invalid key: " + key);
    }

    private static MarketDataEntry fromAggregatedKey(String key) {
        var matcher = AGGREGATED_FILE_PATTERN.matcher(key);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid key: " + key);
        }
        int securityId = Integer.parseInt(matcher.group(1));
        int exchangeId = Integer.parseInt(matcher.group(2));
        SchemaType schemaType = SchemaType.findById(matcher.group(8));
        LocalDateTime timestamp = parseTimestamp(matcher);
        return new MarketDataEntry(securityId, exchangeId, schemaType, timestamp, EntryType.AGGREGATED);
    }

    private static MarketDataEntry fromRawKey(String key) {
        var matcher = RAW_FILE_PATTERN.matcher(key);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid key: " + key);
        }
        int securityId = Integer.parseInt(matcher.group(1));
        int exchangeId = Integer.parseInt(matcher.group(2));
        SchemaType schemaType = SchemaType.findById(matcher.group(8));
        LocalDateTime timestamp = parseTimestamp(matcher);
        String uuid = matcher.group(9);
        return new MarketDataEntry(securityId, exchangeId, schemaType, timestamp, EntryType.RAW, uuid);
    }

    private static LocalDateTime parseTimestamp(Matcher matcher) {
        int year = Integer.parseInt(matcher.group(3));
        int month = Integer.parseInt(matcher.group(4));
        int day = Integer.parseInt(matcher.group(5));
        int hour = Integer.parseInt(matcher.group(6));
        int minute = Integer.parseInt(matcher.group(7));
        return LocalDateTime.of(year, month, day, hour, minute);
    }

}
