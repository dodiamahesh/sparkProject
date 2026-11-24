package com.tcs.dmj.dwh.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.Map;

/**
 * All "application.dwh.*" settings from application.yml in one typed object.
 * Inject it anywhere: {@code public MyClass(ApplicationProperties properties)}.
 */
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

    /**
     * Batch mode: java -jar dmj-dwh.jar --run-pipeline=&lt;name&gt; (used by Airflow) runs ONE pipeline and exits.
     *
     * @param failOnRejectedRecords true = a run that ends SUCCESS_WITH_FAILED exits with code 1 (the Airflow task
     *                              fails and the dependent DAGs do not start); false (default) = exit code 0
     */
    public record Batch(boolean failOnRejectedRecords) {
    }

    /**
     * Data-quality checks done for every source.
     *
     * @param rejectGateMinRecords the reject-% gate is skipped for batches smaller than this
     *                             (2 bad rows out of 3 is 66 %, but not a broken source)
     */
    public record Quality(int rejectGateMinRecords) {
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
}
