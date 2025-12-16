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

        Set<String> uniqueCollectors = entries.keySet();
        List<Schema> output = new ArrayList<>();

        Map<Long, Map<String, List<Schema>>> records = sortRecords(entries);
        for (var entry : records.entrySet()) {
            int numUniqueCollectors = entry.getValue().size();
            if (numUniqueCollectors < uniqueCollectors.size()) {
                for (String collector : uniqueCollectors) {
                    if (!entry.getValue().containsKey(collector)) {
                        logger.logf(LogMessage.DEBUG, "Missing records for sequence %d from key %s", entry.getKey(), collector);
                    }
                }
            }

            int maxCount = entry.getValue().values().stream().mapToInt(List::size).max().orElse(0);
            boolean hasSaved = false;
            for (var keyEntry : entry.getValue().entrySet()) {
                if (keyEntry.getValue().size() < maxCount) {
                    logger.logf(LogMessage.DEBUG, "Missing %d records for sequence %d from key %s",
                            maxCount - keyEntry.getValue().size(), entry.getKey(), keyEntry.getKey());
                } else {
                    // Yes, this could be (else if) in one line, but it reads easier this way.
                    // Side note: I first wrote "Yes, " and let AI fill in the rest, and it filled it in as:
                    // "Yes, this is a hack." <- absolutely brutal by AI, it thinks my code is hacky... damn.
                    if (!hasSaved) {
                        output.addAll(keyEntry.getValue());
                        hasSaved = true;
                    }
                }
            }

        }

        return output;
    }

    private Map<Long, Map<String, List<Schema>>> sortRecords(Map<String, List<Schema>> entries) {
        Map<Long, Map<String, List<Schema>>> records = new LinkedHashMap<>();
        for (var entry : entries.entrySet()) {
            for (Schema schema : entry.getValue()) {
                records.computeIfAbsent(schema.getSequenceNumber(), k -> new LinkedHashMap<>()).computeIfAbsent(entry.getKey(), k -> new ArrayList<>()).add(schema);
            }
        }
        return records;
    }
}

