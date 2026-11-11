package com.demo.etl.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.Map;

/**
 * All "etl.*" settings from application.yml in one typed object.
 * Inject it anywhere: {@code public MyClass(EtlProperties properties)}.
 */
@ConfigurationProperties(prefix = "etl")
public record EtlProperties(
        Spark spark,
        Mongo mongo,
        Postgres postgres,
        Source source,
        Job job,
        Quality quality,
        SampleData sampleData) {

    /**
     * Data-quality checks done for every source.
     *
     * @param rejectGateMinRecords the reject-% gate is skipped for batches smaller than this
     *                             (2 bad rows out of 3 is 66 %, but not a broken source)
     * @param schemaSampleSize     how many MongoDB documents the schema-drift check looks at
     */
    public record Quality(int rejectGateMinRecords, int schemaSampleSize) {
    }

    /** Spark session settings. {@code config} is passed to Spark as-is. */
    public record Spark(String appName, String master, Map<String, String> config) {
    }

    /** Bronze layer (source). */
    public record Mongo(String uri, String database) {
    }

    /** Silver layer (target). */
    public record Postgres(String url, String username, String password, String stagingSchema, int batchSize) {
    }

    /** Name of the flag field in every MongoDB document (drives incremental load). */
    public record Source(String flagField) {
    }

    /** Thread pool used when a job is triggered with async=true. */
    public record Job(int threadPoolSize, int queueCapacity) {
    }

    /** Dev helper: load JSON files into MongoDB on startup. */
    public record SampleData(boolean loadOnStartup, String location) {
    }
}
