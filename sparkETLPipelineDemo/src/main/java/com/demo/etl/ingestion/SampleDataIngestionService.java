package com.demo.etl.ingestion;

import com.demo.etl.config.EtlProperties;
import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Ingests the sample JSON files into MongoDB (the bronze layer).
 *
 * WORKFLOW - every file is processed one by one, with these steps:
 *   1. RESOLVE  file name -> target collection      payments.json -> payments
 *   2. READ     parse the JSON array into documents
 *   3. PREPARE  create the collection (REPLACE drops it first, SKIP_IF_EXISTS stops here if it has data)
 *   4. INSERT   insert the documents in batches of 500
 *   5. VERIFY   count the documents in the collection
 *
 * A failing file does NOT stop the workflow: it is reported as FAILED and the next file is processed.
 */
@Service
public class SampleDataIngestionService {

    private static final Logger log = LoggerFactory.getLogger(SampleDataIngestionService.class);

    private static final int BATCH_SIZE = 500;
    private static final String FILE_EXTENSION = ".json";
    private static final Pattern VALID_COLLECTION_NAME = Pattern.compile("^[a-z][a-z0-9_]*$");

    /** What to do when the target collection already exists. */
    public enum IngestionMode {
        /** Drop the collection and load the file again (fresh data, all flags back to 'N'). */
        REPLACE,
        /** Keep existing documents and add the file's documents. */
        APPEND,
        /** Do nothing if the collection already has documents. */
        SKIP_IF_EXISTS;

        public static IngestionMode from(String value) {
            return Arrays.stream(values())
                    .filter(mode -> mode.name().equalsIgnoreCase(value))
                    .findFirst()
                    .orElseThrow(() -> new IllegalArgumentException(
                            "Invalid mode '" + value + "'. Allowed values: " + Arrays.toString(values())));
        }
    }

    private final MongoTemplate mongoTemplate;
    private final String filesLocation;

    public SampleDataIngestionService(MongoTemplate mongoTemplate, EtlProperties properties) {
        this.mongoTemplate = mongoTemplate;
        this.filesLocation = properties.sampleData().location();
    }

    /** Names of all files that can be ingested, e.g. [accounts.json, branches.json, ...]. */
    public List<String> listFiles() {
        return findFiles().stream().map(Resource::getFilename).toList();
    }

    /** Runs the workflow for every file. */
    public Map<String, Object> ingestAll(IngestionMode mode) {
        return runWorkflow(findFiles(), mode);
    }

    /** Runs the workflow for one file. {@code fileName} may be "payments" or "payments.json". */
    public Map<String, Object> ingestOne(String fileName, IngestionMode mode) {
        String wanted = fileName.endsWith(FILE_EXTENSION) ? fileName : fileName + FILE_EXTENSION;
        // Only files from the sample-data folder are accepted (no free paths -> no path traversal)
        Resource file = findFiles().stream()
                .filter(resource -> wanted.equals(resource.getFilename()))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(
                        "File '" + wanted + "' not found. Available files: " + listFiles()));
        return runWorkflow(List.of(file), mode);
    }

    // ================================================================== workflow

    private Map<String, Object> runWorkflow(List<Resource> files, IngestionMode mode) {
        long startMillis = System.currentTimeMillis();
        log.info("Ingestion started: {} file(s), mode={}", files.size(), mode);

        List<Map<String, Object>> fileResults = new ArrayList<>();
        for (Resource file : files) {
            fileResults.add(ingestFile(file, mode));
        }

        long success = countByStatus(fileResults, "SUCCESS");
        long skipped = countByStatus(fileResults, "SKIPPED");
        long failed = countByStatus(fileResults, "FAILED");
        long inserted = fileResults.stream().mapToLong(result -> (long) result.getOrDefault("documentsInserted", 0L)).sum();

        String overallStatus = failed == 0 ? "SUCCESS" : (failed == files.size() ? "FAILED" : "PARTIAL_SUCCESS");

        Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("status", overallStatus);
        summary.put("mode", mode.name());
        summary.put("database", mongoTemplate.getDb().getName());
        summary.put("totalFiles", files.size());
        summary.put("successFiles", success);
        summary.put("skippedFiles", skipped);
        summary.put("failedFiles", failed);
        summary.put("totalDocumentsInserted", inserted);
        summary.put("durationMs", System.currentTimeMillis() - startMillis);
        summary.put("files", fileResults);

        log.info("Ingestion finished: status={}, success={}, skipped={}, failed={}, documents={}",
                overallStatus, success, skipped, failed, inserted);
        return summary;
    }

    /** The 5 workflow steps for ONE file. Never throws: errors are returned as status FAILED. */
    private Map<String, Object> ingestFile(Resource file, IngestionMode mode) {
        long startMillis = System.currentTimeMillis();
        String fileName = file.getFilename();
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("file", fileName);

        try {
            // STEP 1 - RESOLVE: file name -> collection name
            String collectionName = toCollectionName(fileName);
            result.put("collection", collectionName);

            // STEP 2 - READ: parse the JSON array
            List<Document> documents = readDocuments(file);
            result.put("documentsInFile", documents.size());

            // STEP 3 - PREPARE: create / drop / skip the collection
            boolean exists = mongoTemplate.collectionExists(collectionName);
            long existingCount = exists ? mongoTemplate.getCollection(collectionName).countDocuments() : 0;

            if (mode == IngestionMode.SKIP_IF_EXISTS && existingCount > 0) {
                result.put("status", "SKIPPED");
                result.put("message", "Collection already has " + existingCount + " documents");
                result.put("documentsInCollection", existingCount);
                log.info("[{}] skipped: collection '{}' already has {} documents", fileName, collectionName, existingCount);
                return result;
            }
            if (mode == IngestionMode.REPLACE && exists) {
                mongoTemplate.dropCollection(collectionName);
                exists = false;
            }
            if (!exists) {
                mongoTemplate.createCollection(collectionName);
            }
            result.put("collectionCreated", !exists);

            // STEP 4 - INSERT: in batches
            MongoCollection<Document> collection = mongoTemplate.getCollection(collectionName);
            long inserted = insertInBatches(collection, documents);
            result.put("documentsInserted", inserted);

            // STEP 5 - VERIFY: the collection must contain what we expect
            long countAfter = collection.countDocuments();
            long expected = (mode == IngestionMode.REPLACE ? 0 : existingCount) + documents.size();
            result.put("documentsInCollection", countAfter);
            if (countAfter != expected) {
                throw new IllegalStateException("Expected " + expected + " documents in '" + collectionName
                        + "' but found " + countAfter);
            }

            result.put("status", "SUCCESS");
            log.info("[{}] -> '{}': {} documents inserted ({} in collection)",
                    fileName, collectionName, inserted, countAfter);

        } catch (Exception e) {
            result.put("status", "FAILED");
            result.put("error", e.getClass().getSimpleName() + ": " + e.getMessage());
            log.error("[{}] ingestion FAILED: {}", fileName, e.getMessage(), e);
        } finally {
            result.put("durationMs", System.currentTimeMillis() - startMillis);
        }
        return result;
    }

    // ================================================================== helpers

    private List<Resource> findFiles() {
        try {
            Resource[] files = new PathMatchingResourcePatternResolver().getResources(filesLocation);
            return Arrays.stream(files)
                    .sorted(Comparator.comparing(Resource::getFilename))
                    .toList();
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot read sample data folder " + filesLocation, e);
        }
    }

    /** payments.json -> payments, fund_transfers.json -> fund_transfers */
    private String toCollectionName(String fileName) {
        String name = fileName.substring(0, fileName.length() - FILE_EXTENSION.length()).toLowerCase();
        if (!VALID_COLLECTION_NAME.matcher(name).matches()) {
            throw new IllegalArgumentException("'" + name + "' is not a valid collection name "
                    + "(use lower case letters, digits and _)");
        }
        return name;
    }

    private List<Document> readDocuments(Resource file) throws IOException {
        String json = file.getContentAsString(StandardCharsets.UTF_8).trim();
        if (!json.startsWith("[")) {
            throw new IllegalArgumentException("File must contain a JSON array: [ {...}, {...} ]");
        }
        // Document.parse expects an object, so wrap the array: {"documents": [...]}
        return Document.parse("{\"documents\": " + json + "}").getList("documents", Document.class);
    }

    private long insertInBatches(MongoCollection<Document> collection, List<Document> documents) {
        long inserted = 0;
        for (int from = 0; from < documents.size(); from += BATCH_SIZE) {
            List<Document> batch = documents.subList(from, Math.min(from + BATCH_SIZE, documents.size()));
            collection.insertMany(batch);
            inserted += batch.size();
        }
        return inserted;
    }

    private long countByStatus(List<Map<String, Object>> results, String status) {
        return results.stream().filter(result -> status.equals(result.get("status"))).count();
    }
}
