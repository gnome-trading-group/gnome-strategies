package group.gnometrading.testing;

import com.lmax.disruptor.EventHandler;
import group.gnometrading.logging.LogMessage;
import group.gnometrading.logging.Logger;
import group.gnometrading.schemas.Schema;
import org.agrona.ExpandableArrayBuffer;

import java.io.FileOutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;

public class MarketDataWriter implements EventHandler<Schema> {

    private final ExpandableArrayBuffer purgatory;
    private final FileOutputStream outputStream;
    private final Logger logger;
    private int messageCount;

    public MarketDataWriter(Logger logger, String filename) {
        this.logger = logger;
        this.messageCount = 0;
        this.purgatory = new ExpandableArrayBuffer(1 << 12);

        try {
            Files.createDirectories(Paths.get(filename).getParent());
            this.outputStream = new FileOutputStream(filename);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        logger.logf(LogMessage.DEBUG, "MarketDataWriter created for %s", filename);
    }

    @Override
    public void onEvent(Schema schema, long sequence, boolean endOfBatch) throws Exception {
        schema.buffer.getBytes(0, this.purgatory, 0, schema.totalMessageSize());
        this.outputStream.write(this.purgatory.byteArray(), 0, schema.totalMessageSize());
        this.messageCount++;

        if (this.messageCount % 100 == 0) {
            logger.logf(LogMessage.DEBUG, "Wrote %d messages", this.messageCount);
        }
    }

}
