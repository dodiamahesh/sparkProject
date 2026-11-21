package com.demo.etl.devdata;

import com.demo.etl.config.EtlProperties;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Loads src/main/resources/sample-data/*.json into MongoDB (file name = collection name).
 * On startup it only fills EMPTY collections, so documents you edited are never overwritten.
 */
@Component
public class SampleDataLoader implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(SampleDataLoader.class);

    private final MongoTemplate mongoTemplate;
    private final EtlProperties.SampleData sampleData;

    public SampleDataLoader(MongoTemplate mongoTemplate, EtlProperties properties) {
        this.mongoTemplate = mongoTemplate;
        this.sampleData = properties.sampleData();
    }

    @Override
    public void run(ApplicationArguments args) {
        if (!sampleData.loadOnStartup()) {
            return;
        }
        try {
            Map<String, Integer> loaded = load(false);
            if (!loaded.isEmpty()) {
                log.info("Sample data loaded into MongoDB: {}", loaded);
            }
        } catch (RuntimeException e) {
            // The application still starts, e.g. when MongoDB is not running yet
            log.warn("Could not load sample data into MongoDB: {}", e.getMessage());
        }
    }

    /**
     * @param dropExisting true = drop and reload every sample collection (all flags back to 'N')
     * @return documents inserted per collection
     */
    public Map<String, Integer> load(boolean dropExisting) {
        Map<String, Integer> inserted = new LinkedHashMap<>();
        for (Resource file : sampleFiles()) {
            String collection = file.getFilename().replace(".json", "");
            if (dropExisting) {
                mongoTemplate.dropCollection(collection);
            } else if (mongoTemplate.collectionExists(collection)
                    && mongoTemplate.getCollection(collection).estimatedDocumentCount() > 0) {
                continue;
            }
            List<Document> documents = readJsonArray(file);
            mongoTemplate.getCollection(collection).insertMany(documents);
            inserted.put(collection, documents.size());
        }
        return inserted;
    }

    private Resource[] sampleFiles() {
        try {
            return new PathMatchingResourcePatternResolver().getResources(sampleData.location());
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot find sample data files at " + sampleData.location(), e);
        }
    }

    private List<Document> readJsonArray(Resource file) {
        try {
            String json = file.getContentAsString(StandardCharsets.UTF_8);
            // Document.parse expects an object, so wrap the array: {"documents": [...]}
            return Document.parse("{\"documents\": " + json + "}").getList("documents", Document.class);
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot read " + file.getFilename(), e);
        }
    }
}
