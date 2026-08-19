package group.gnometrading.collector;

import com.github.luben.zstd.ZstdOutputStream;
import group.gnometrading.logging.LogMessage;
import group.gnometrading.logging.Logger;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.LocalDateTime;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;

final class CycleBuffer {

    private final LocalDateTime timestamp;
    private final String s3Key;
    private final ByteArrayOutputStream rawBuffer;
    private final ZstdOutputStream outputStream;

    CycleBuffer(LocalDateTime timestamp, String s3Key) {
        this.timestamp = timestamp;
        this.s3Key = s3Key;
        this.rawBuffer = new ByteArrayOutputStream();
        try {
            this.outputStream = new ZstdOutputStream(rawBuffer);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    LocalDateTime timestamp() {
        return timestamp;
    }

    ZstdOutputStream outputStream() {
        return outputStream;
    }

    int compressedSize() {
        return rawBuffer.size();
    }

    void upload(S3Client s3Client, String bucketName, Logger logger) throws IOException {
        outputStream.close();
        if (rawBuffer.size() == 0) {
            logger.logf(LogMessage.DEBUG, "Skipping upload for empty buffer");
            return;
        }
        logger.logf(LogMessage.DEBUG, "Uploading file: %s (%d bytes)", s3Key, rawBuffer.size());
        s3Client.putObject(
                request -> request.key(s3Key).bucket(bucketName), RequestBody.fromBytes(rawBuffer.toByteArray()));
    }

    void close() throws IOException {
        outputStream.close();
        rawBuffer.close();
    }
}
