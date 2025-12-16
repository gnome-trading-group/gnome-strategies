package group.gnometrading.collector.merger;

import group.gnometrading.logging.Logger;
import group.gnometrading.schemas.Schema;

import java.util.List;
import java.util.Map;

public interface SchemaMergeStrategy {

    /**
     * Merges the records for a given schema type into a consolidated output.
     * @return the list of merged records
     */
    List<Schema> mergeRecords(Logger logger, Map<String, List<Schema>> entries);
}

