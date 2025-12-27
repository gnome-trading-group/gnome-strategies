package group.gnometrading.collector.merger;

import group.gnometrading.logging.LogMessage;
import group.gnometrading.logging.Logger;
import group.gnometrading.schemas.Schema;

import java.util.*;

/**
 * Merge strategy for MBP10 schema type.
 * Produces output entries in sequence order.
 */
public class MBP10MergeStrategy implements SchemaMergeStrategy {

    @Override
    public List<Schema> mergeRecords(Logger logger, Map<String, List<Schema>> entries) {
        if (entries.isEmpty()) {
            logger.logf(LogMessage.DEBUG, "No records to process");
            return List.of();
        }

        // Find the collector with the most records
        String winningCollector = null;
        int maxRecords = -1;
        for (var entry : entries.entrySet()) {
            if (entry.getValue().size() > maxRecords) {
                maxRecords = entry.getValue().size();
                winningCollector = entry.getKey();
            }
        }

        // Log differences for other collectors (only when there's actually a difference)
        for (var entry : entries.entrySet()) {
            if (!entry.getKey().equals(winningCollector)) {
                int missing = maxRecords - entry.getValue().size();
                if (missing > 0) {
                    logger.logf(LogMessage.DEBUG, "Collector %s has %d fewer records than %s (%d vs %d)",
                            entry.getKey(), missing, winningCollector, entry.getValue().size(), maxRecords);
                }
            }
        }

        return new ArrayList<>(entries.get(winningCollector));
    }
}

