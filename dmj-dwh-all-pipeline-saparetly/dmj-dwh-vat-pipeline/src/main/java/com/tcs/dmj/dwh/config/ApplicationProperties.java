package com.tcs.dmj.dwh.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.Map;

/** All "application.dwh.*" settings of application.yml. */
@ConfigurationProperties(prefix = "application.dwh")
public record ApplicationProperties(
        Spark spark,
        Mongo mongo,
        Postgres postgres,
        Quality quality,
        Batch batch) {

    public ApplicationProperties {
        batch = batch == null ? new Batch(false) : batch;
    }

    /** failOnRejectedRecords = true: a SUCCESS_WITH_FAILED run exits with code 1 in batch mode. */
    public record Batch(boolean failOnRejectedRecords) {
    }

    /** The reject gate is skipped for batches smaller than rejectGateMinRecords. */
    public record Quality(int rejectGateMinRecords) {
    }

    public record Spark(String appName, String master, Map<String, String> config) {
    }

    public record Mongo(String uri, String database) {
    }

    public record Postgres(String url, String username, String password, String stagingSchema, int batchSize) {
    }
}
