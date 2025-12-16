package group.gnometrading.collector.merger;

import group.gnometrading.logging.LogMessage;
import group.gnometrading.logging.Logger;
import group.gnometrading.schemas.MBP10Schema;
import group.gnometrading.schemas.Schema;
import group.gnometrading.schemas.SchemaType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.*;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class MBP10MergeStrategyTest {

    @Mock
    private Logger logger;

    private MBP10MergeStrategy strategy;

    @BeforeEach
    void setUp() {
        strategy = new MBP10MergeStrategy();
    }

    /**
     * Helper to create a schema with a specific sequence number.
     * Makes it easy to track which sequence a schema belongs to.
     */
    private Schema schema(long sequenceNumber) {
        MBP10Schema schema = (MBP10Schema) SchemaType.MBP_10.newInstance();
        schema.encoder.sequence(sequenceNumber);
        return schema;
    }

    /**
     * Helper to build a collector map with readable syntax.
     * Example: collector("collector-1", 100, 100, 101)
     * Creates a collector with 3 records: two with sequence 100, one with sequence 101
     */
    private Map<String, List<Schema>> collectors(Object... args) {
        Map<String, List<Schema>> result = new LinkedHashMap<>();
        int i = 0;
        while (i < args.length) {
            String collectorId = (String) args[i];
            List<Schema> schemas = new ArrayList<>();
            i++;
            while (i < args.length && args[i] instanceof Long) {
                schemas.add(schema((Long) args[i]));
                i++;
            }
            result.put(collectorId, schemas);
        }
        return result;
    }

    /**
     * Parses a compact collector notation into a map.
     * Format: "collectorId:seq,seq,seq;collectorId:seq,seq"
     * Example: "c1:100,100,101;c2:100,101,101" creates two collectors
     */
    private Map<String, List<Schema>> parseCollectors(String notation) {
        Map<String, List<Schema>> result = new LinkedHashMap<>();
        if (notation == null || notation.isEmpty()) {
            return result;
        }
        for (String collectorPart : notation.split(";")) {
            String[] parts = collectorPart.split(":");
            String collectorId = parts[0];
            List<Schema> schemas = new ArrayList<>();
            if (parts.length > 1 && !parts[1].isEmpty()) {
                for (String seq : parts[1].split(",")) {
                    schemas.add(schema(Long.parseLong(seq.trim())));
                }
            }
            result.put(collectorId, schemas);
        }
        return result;
    }

    /**
     * Parses expected output notation into a list of expected sequence numbers.
     * Format: "seq,seq,seq" or empty string for no output
     */
    private long[] parseExpectedOutput(String notation) {
        if (notation == null || notation.isEmpty()) {
            return new long[0];
        }
        return Arrays.stream(notation.split(","))
                .map(String::trim)
                .mapToLong(Long::parseLong)
                .toArray();
    }

    // ============================================================================
    // PARAMETERIZED MERGE TESTS
    // ============================================================================

    /**
     * Test cases for merge strategy. Each case is:
     * - description: Human-readable description
     * - input: Collector notation "c1:seq,seq;c2:seq,seq"
     * - expectedOutput: Expected sequence numbers "seq,seq,seq"
     * - expectMissingLogs: Whether missing record logs are expected
     */
    static Stream<Arguments> mergeTestCases() {
        return Stream.of(
            // Empty input
            Arguments.of("Empty records", "", "", false),

            // Single collector cases
            Arguments.of("Single collector, single sequence, single record",
                "c1:100", "100", false),
            Arguments.of("Single collector, single sequence, multiple records",
                "c1:100,100", "100,100", false),
            Arguments.of("Single collector, multiple sequences",
                "c1:100,101,102", "100,101,102", false),
            Arguments.of("Single collector, many records",
                "c1:1,2,3,4,5,6,7,8,9,10", "1,2,3,4,5,6,7,8,9,10", false),
            Arguments.of("Single collector, large sequence numbers",
                "c1:999999,1000000", "999999,1000000", false),

            // Two collectors - equal counts (no missing logs)
            Arguments.of("Two collectors, equal single record per sequence",
                "c1:100;c2:100", "100", false),
            Arguments.of("Two collectors, equal multiple records per sequence",
                "c1:100,100;c2:100,100", "100,100", false),
            Arguments.of("Two collectors, equal records across multiple sequences",
                "c1:100,100,101,101;c2:100,100,101,101", "100,100,101,101", false),

            // Two collectors - unequal counts (missing logs expected)
            Arguments.of("Two collectors, c2 missing one record for sequence",
                "c1:100,100;c2:100", "100,100", true),
            Arguments.of("Two collectors, c2 missing entire sequence",
                "c1:100,100,101,101;c2:100,100", "100,100,101,101", true),
            Arguments.of("Two collectors, c1 has more records",
                "c1:100,100,100;c2:100,100", "100,100,100", true),
            Arguments.of("Two collectors, different max per sequence",
                "c1:100,100,101;c2:100,101,101,101", "100,100,101,101,101", true),

            // Two collectors - one empty
            Arguments.of("Two collectors, c2 empty",
                "c1:100,100;c2:", "100,100", true),

            // Three collectors
            Arguments.of("Three collectors, all equal",
                "c1:100,101;c2:100,101;c3:100,101", "100,101", false),
            Arguments.of("Three collectors, c2 missing sequence",
                "c1:100,101;c2:100;c3:100,101", "100,101", true),
            Arguments.of("Three collectors, varying counts per sequence",
                "c1:100,100,101;c2:100,101,101;c3:100,100,100,101", "100,100,100,101,101", true),
            Arguments.of("Three collectors, c3 has max for all sequences",
                "c1:100;c2:100;c3:100,100", "100,100", true),

            // Edge cases with gaps
            Arguments.of("Non-contiguous sequences",
                "c1:100,200,300", "100,200,300", false),
            Arguments.of("Two collectors, non-contiguous, one missing middle",
                "c1:100,200,300;c2:100,300", "100,200,300", true),

            // Complex scenarios - c1 missing 101, c2 missing 100,102, c3 missing 101, c4 missing 100
            // For seq 100: c1,c3,c4 have 1 each -> c1 wins
            // For seq 101: c1,c3,c4 have 1 each, c2 missing -> c1 wins (but c3 has 2!) -> c3 wins with 2
            Arguments.of("Four collectors, mixed coverage",
                "c1:100,101;c2:100;c3:101,101;c4:100,101", "100,101,101", true),

            // Stress test - many sequences
            Arguments.of("Single collector, 20 sequential records",
                "c1:1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20",
                "1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20", false),

            // Multiple records per sequence with varying counts
            Arguments.of("Two collectors, multiple records per sequence, c1 wins all",
                "c1:100,100,100,101,101,101;c2:100,100,101,101", "100,100,100,101,101,101", true),
            Arguments.of("Two collectors, c2 wins for one sequence",
                "c1:100,101;c2:100,100,101", "100,100,101", true),

            // Out-of-order sequences within collector - output preserves encounter order (NOT sorted)
            // Note: sortRecords uses LinkedHashMap which preserves insertion order, not sorted order
            Arguments.of("Single collector, out-of-order sequences preserves order",
                "c1:102,100,101", "102,100,101", false),
            // c1 adds 102, 100 to map; c2 adds 101 (100 already exists) -> order is 102, 100, 101
            Arguments.of("Two collectors, out-of-order sequences preserves order",
                "c1:102,100;c2:101,100", "102,100,101", true),

            // Second collector wins (c2 has more records than c1)
            Arguments.of("Two collectors, c2 wins with more records",
                "c1:100;c2:100,100", "100,100", true),
            Arguments.of("Two collectors, c2 wins for all sequences",
                "c1:100,101;c2:100,100,101,101", "100,100,101,101", true),

            // Tie-breaking - first collector in iteration order wins
            Arguments.of("Two collectors, tie goes to first (c1)",
                "c1:100,100;c2:100,100", "100,100", false),
            Arguments.of("Three collectors, tie goes to first with max",
                "c1:100;c2:100,100;c3:100,100", "100,100", true),

            // Sequence number edge cases
            Arguments.of("Sequence number 0",
                "c1:0,1,2", "0,1,2", false),
            Arguments.of("Two collectors with sequence 0",
                "c1:0,0;c2:0", "0,0", true),

            // Many collectors stress test
            Arguments.of("Five collectors, all equal",
                "c1:100;c2:100;c3:100;c4:100;c5:100", "100", false),
            Arguments.of("Five collectors, one missing",
                "c1:100,101;c2:100,101;c3:100;c4:100,101;c5:100,101", "100,101", true),

            // Complex interleaving - each collector has different sequences
            Arguments.of("Three collectors, each has unique sequence",
                "c1:100;c2:101;c3:102", "100,101,102", true),

            // All collectors missing records for same sequence (but at least one has it)
            Arguments.of("Three collectors, all have fewer than max for one seq",
                "c1:100,100,100,101;c2:100,100,101;c3:100,101", "100,100,100,101", true)
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("mergeTestCases")
    void testMergeStrategy(String description, String input, String expectedOutput, boolean expectMissingLogs) {
        // Given: Parsed input
        Map<String, List<Schema>> collectors = parseCollectors(input);
        long[] expected = parseExpectedOutput(expectedOutput);

        // When: Merging
        List<Schema> result = strategy.mergeRecords(logger, collectors);

        // Then: Verify output size and sequence numbers
        assertEquals(expected.length, result.size(),
            "Output size mismatch for: " + description);
        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i], result.get(i).getSequenceNumber(),
                "Sequence mismatch at index " + i + " for: " + description);
        }

        // Verify logging behavior - check that at least one "Missing" log was made
        if (expectMissingLogs) {
            // Try to verify with 3 args first (for "Missing %d records..." format)
            // If that fails, try with 2 args (for "Missing records..." format)
            boolean foundMissingLog = false;
            try {
                ArgumentCaptor<String> formatCaptor = ArgumentCaptor.forClass(String.class);
                verify(logger, atLeastOnce()).logf(eq(LogMessage.DEBUG), formatCaptor.capture(), any(), any(), any());
                foundMissingLog = formatCaptor.getAllValues().stream().anyMatch(f -> f.contains("Missing"));
            } catch (AssertionError e) {
                // Try with 2 args
                try {
                    ArgumentCaptor<String> formatCaptor = ArgumentCaptor.forClass(String.class);
                    verify(logger, atLeastOnce()).logf(eq(LogMessage.DEBUG), formatCaptor.capture(), any(), any());
                    foundMissingLog = formatCaptor.getAllValues().stream().anyMatch(f -> f.contains("Missing"));
                } catch (AssertionError e2) {
                    // Neither worked
                }
            }
            assertTrue(foundMissingLog, "Expected missing record logs for: " + description);
        }
    }

    // ============================================================================
    // ADDITIONAL EDGE CASE TESTS (not easily parameterized)
    // ============================================================================

    @Test
    void testEmptyRecordsLogsDebugMessage() {
        Map<String, List<Schema>> records = new HashMap<>();
        strategy.mergeRecords(logger, records);
        verify(logger).logf(LogMessage.DEBUG, "No records to process");
    }

    @Test
    void testNoMissingLogsWhenAllCollectorsEqual() {
        Map<String, List<Schema>> input = collectors(
                "collector-1", 100L, 100L,
                "collector-2", 100L, 100L
        );
        strategy.mergeRecords(logger, input);
        // Verify no "Missing" logs with either 2 or 3 args
        verify(logger, never()).logf(eq(LogMessage.DEBUG), contains("Missing"), any(), any(), any());
        verify(logger, never()).logf(eq(LogMessage.DEBUG), contains("Missing"), any(), any());
    }
}

