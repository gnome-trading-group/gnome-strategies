package group.gnometrading.testing;

import group.gnometrading.logging.NullLogger;
import group.gnometrading.schemas.Schema;
import group.gnometrading.schemas.SchemaType;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

class MarketDataWriterTest {

    @TempDir
    Path tempDir;

    private Path testFile;
    private MarketDataWriter writer;

    @BeforeEach
    void setup() throws IOException {
        testFile = tempDir.resolve("test-output.dat");
    }

    @AfterEach
    void cleanup() throws Exception {
        // Clean up any resources if needed
    }

    @Test
    void testWriteSingleMessage() throws Exception {
        writer = new MarketDataWriter(new NullLogger(), testFile.toString());

        // Create a test schema with known data
        TestSchema schema = new TestSchema("testdata1");

        // Write the message
        writer.onEvent(schema, 0, true);

        // Verify file was created
        assertTrue(Files.exists(testFile), "Output file should exist");

        // Read and verify content
        byte[] fileContent = Files.readAllBytes(testFile);
        assertEquals(schema.totalMessageSize(), fileContent.length, "File should contain exactly one message");

        // Verify the content matches what we wrote
        byte[] expected = new byte[schema.totalMessageSize()];
        schema.buffer.getBytes(0, expected, 0, schema.totalMessageSize());
        assertArrayEquals(expected, fileContent, "File content should match written data");
    }

    @Test
    void testWriteMultipleMessages() throws Exception {
        writer = new MarketDataWriter(new NullLogger(), testFile.toString());

        // Create multiple test schemas with different data
        TestSchema schema1 = new TestSchema("message1");
        TestSchema schema2 = new TestSchema("message2");
        TestSchema schema3 = new TestSchema("message3");

        // Write multiple messages
        writer.onEvent(schema1, 0, false);
        writer.onEvent(schema2, 1, false);
        writer.onEvent(schema3, 2, true);

        // Verify file was created
        assertTrue(Files.exists(testFile), "Output file should exist");

        // Read and verify content
        byte[] fileContent = Files.readAllBytes(testFile);
        int expectedSize = schema1.totalMessageSize() * 3;
        assertEquals(expectedSize, fileContent.length, "File should contain three messages");

        // Verify each message in the file
        int msgSize = schema1.totalMessageSize();
        
        byte[] expected1 = new byte[msgSize];
        schema1.buffer.getBytes(0, expected1, 0, msgSize);
        byte[] actual1 = new byte[msgSize];
        System.arraycopy(fileContent, 0, actual1, 0, msgSize);
        assertArrayEquals(expected1, actual1, "First message should match");

        byte[] expected2 = new byte[msgSize];
        schema2.buffer.getBytes(0, expected2, 0, msgSize);
        byte[] actual2 = new byte[msgSize];
        System.arraycopy(fileContent, msgSize, actual2, 0, msgSize);
        assertArrayEquals(expected2, actual2, "Second message should match");

        byte[] expected3 = new byte[msgSize];
        schema3.buffer.getBytes(0, expected3, 0, msgSize);
        byte[] actual3 = new byte[msgSize];
        System.arraycopy(fileContent, msgSize * 2, actual3, 0, msgSize);
        assertArrayEquals(expected3, actual3, "Third message should match");
    }

    @Test
    void testWriteLargeNumberOfMessages() throws Exception {
        writer = new MarketDataWriter(new NullLogger(), testFile.toString());

        int messageCount = 250;
        TestSchema[] schemas = new TestSchema[messageCount];
        
        // Create and write messages
        for (int i = 0; i < messageCount; i++) {
            schemas[i] = new TestSchema("msg" + i);
            writer.onEvent(schemas[i], i, i == messageCount - 1);
        }

        // Verify file was created
        assertTrue(Files.exists(testFile), "Output file should exist");

        // Read and verify content
        byte[] fileContent = Files.readAllBytes(testFile);
        int msgSize = schemas[0].totalMessageSize();
        int expectedSize = msgSize * messageCount;
        assertEquals(expectedSize, fileContent.length, "File should contain all messages");

        // Spot check a few messages
        for (int i : new int[]{0, 50, 100, 150, 200, 249}) {
            byte[] expected = new byte[msgSize];
            schemas[i].buffer.getBytes(0, expected, 0, msgSize);
            byte[] actual = new byte[msgSize];
            System.arraycopy(fileContent, i * msgSize, actual, 0, msgSize);
            assertArrayEquals(expected, actual, "Message " + i + " should match");
        }
    }

    @Test
    void testWriteWithRandomData() throws Exception {
        writer = new MarketDataWriter(new NullLogger(), testFile.toString());

        Random random = new Random(42);
        int messageCount = 10;
        TestSchema[] schemas = new TestSchema[messageCount];

        // Create schemas with random data
        for (int i = 0; i < messageCount; i++) {
            byte[] randomData = new byte[64];
            random.nextBytes(randomData);
            schemas[i] = new TestSchema(randomData);
            writer.onEvent(schemas[i], i, i == messageCount - 1);
        }

        // Verify file content
        byte[] fileContent = Files.readAllBytes(testFile);
        int msgSize = schemas[0].totalMessageSize();
        assertEquals(msgSize * messageCount, fileContent.length);

        // Verify each message
        for (int i = 0; i < messageCount; i++) {
            byte[] expected = new byte[msgSize];
            schemas[i].buffer.getBytes(0, expected, 0, msgSize);
            byte[] actual = new byte[msgSize];
            System.arraycopy(fileContent, i * msgSize, actual, 0, msgSize);
            assertArrayEquals(expected, actual, "Random message " + i + " should match");
        }
    }

    @Test
    void testFileCreatesParentDirectories() throws Exception {
        Path nestedFile = tempDir.resolve("nested/dir/structure/output.dat");
        writer = new MarketDataWriter(new NullLogger(), nestedFile.toString());

        TestSchema schema = new TestSchema("test");
        writer.onEvent(schema, 0, true);

        assertTrue(Files.exists(nestedFile), "File should be created with parent directories");
        assertTrue(Files.exists(nestedFile.getParent()), "Parent directories should exist");
    }

    @Test
    void testEmptyFileBeforeFirstWrite() throws Exception {
        writer = new MarketDataWriter(new NullLogger(), testFile.toString());

        // File should exist but be empty
        assertTrue(Files.exists(testFile), "File should be created on initialization");
        assertEquals(0, Files.size(testFile), "File should be empty before first write");
    }

    /**
     * Test schema implementation for testing purposes.
     * Creates a schema with a fixed message size and allows setting custom data.
     */
    private static class TestSchema extends Schema {
        private static final int MESSAGE_SIZE = 128; // Fixed size for testing

        public TestSchema(String data) {
            super(SchemaType.MBP_10);
            byte[] bytes = data.getBytes();
            this.buffer.putBytes(0, bytes, 0, Math.min(bytes.length, MESSAGE_SIZE));
        }

        public TestSchema(byte[] data) {
            super(SchemaType.MBP_10);
            this.buffer.putBytes(0, data, 0, Math.min(data.length, MESSAGE_SIZE));
        }

        @Override
        protected int getEncodedBlockLength() {
            return MESSAGE_SIZE - 8; // Subtract header size
        }

        @Override
        public void wrap(MutableDirectBuffer mutableDirectBuffer) {
            // No-op for testing
        }

        @Override
        public long getSequenceNumber() {
            return 0;
        }

        @Override
        public long getEventTimestamp() {
            return 0;
        }

        @Override
        public int totalMessageSize() {
            return MESSAGE_SIZE;
        }
    }
}

