package com.shop.etl.common.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.ZoneId;
import java.util.Map;

/**
 * application*.yml na {@code etl:} section ne Java object ma map kare che.
 *
 * Kem aa class? Connection, collection naam, schema naam, batch size, job settings: aa badhu
 * code ma hard-code karvane badle EK j jagyae (yml) rakhvu che. Dev ane prod ma fakt yml badlay,
 * code nahi. Badha components aa ek object inject kari ne j settings vanche che (reusable config).
 *
 * Structure (yml sathe match):
 * <pre>
 * etl:
 *   timezone:   Asia/Kolkata
 *   spark:      Spark session settings
 *   mongo:      MongoDB connection + logical collection → actual collection naam
 *   postgres:   PostgreSQL connection + schema naam (silver / gold / meta) + read/write tuning
 *   jobs:       darek job na settings (enabled, cron, reject threshold, params)
 *   scheduler:  in-app scheduler chalu/bandh
 *   executor:   API thi async job mate thread pool
 *   cli:        Airflow CLI mode (job naam + window end)
 * </pre>
 */
@ConfigurationProperties(prefix = "etl")
public record EtlProperties(
        @DefaultValue("Asia/Kolkata") String timezone,
        Spark spark,
        Mongo mongo,
        Postgres postgres,
        Map<String, JobConfig> jobs,
        @DefaultValue Scheduler scheduler,
        @DefaultValue Executor executor,
        @DefaultValue Cli cli) {

    /**
     * Business timezone (IST). Date key (20261021), run id, ane window end aa zone ma ganay che.
     * Mongo ma time UTC ma store hoy, pan business "divas" IST ma che, etle aa jaruri che.
     */
    public ZoneId zoneId() {
        return ZoneId.of(timezone);
    }

    /**
     * Job na settings aape che. yml ma job na hoy to default settings (enabled, threshold 100% = gate off).
     *
     * @param jobName jem ke "J2_SALES"
     */
    public JobConfig job(String jobName) {
        if (jobs == null || !jobs.containsKey(jobName)) {
            return new JobConfig(true, false, null, 100.0, 0, Map.of());
        }
        return jobs.get(jobName);
    }

    /**
     * Spark session settings.
     *
     * @param appName Spark UI / logs ma dekhatu naam
     * @param master  local[*] (ek machine) ke spark://host:7077 (cluster)
     * @param config  koi pan spark.* setting (yml ma "[spark.x.y]" bracket sathe lakho)
     */
    public record Spark(String appName, @DefaultValue("local[*]") String master, Map<String, String> config) {
    }

    /**
     * MongoDB connection settings. Ek j jagyae che, etle badha jobs same connection vapare che.
     *
     * @param uri             mongodb://host:27017 (prod ma ?readPreference=secondaryPreferred umero)
     * @param database        shopdb
     * @param partitionSizeMb Spark ek partition ma ketlo data vanche (connector SamplePartitioner)
     * @param collections     logical naam → actual collection naam. Code fakt logical naam vapare,
     *                        etle prod ma collection naam alag hoy to fakt yml badlo.
     */
    public record Mongo(String uri,
                        String database,
                        @DefaultValue("64") int partitionSizeMb,
                        Map<String, String> collections) {

        /**
         * Logical naam ("customers") mathi actual collection naam kadhe che.
         * yml ma mapping na hoy to logical naam j actual naam mani le che.
         */
        public String collection(String logicalName) {
            if (collections == null) {
                return logicalName;
            }
            return collections.getOrDefault(logicalName, logicalName);
        }
    }

    /**
     * PostgreSQL / EPAS settings. Spring DataSource (meta tables) ane Spark JDBC (data) banne
     * aa j url / user / password vapare che (spring.datasource.* yml ma ahiya thi j aave che).
     *
     * @param writeBatchSize   ek JDBC batch ma ketli rows (1000–5000 saru)
     * @param writeParallelism ek sathe ketla DB connections thi lakhvu (Spark partitions)
     * @param readFetchSize    JDBC read vakhte ek round-trip ma ketli rows
     * @param readPartitions   moti lookup table ne ketla parallel tukda ma vanchvi
     */
    public record Postgres(String url,
                           String username,
                           String password,
                           @DefaultValue Schemas schema,
                           @DefaultValue("1000") int writeBatchSize,
                           @DefaultValue("4") int writeParallelism,
                           @DefaultValue("10000") int readFetchSize,
                           @DefaultValue("4") int readPartitions) {
    }

    /**
     * Schema naam. Code ma "curated" ke "meta" hard-code nathi, aahi thi aave che.
     *
     * @param silver curated layer (dim + fact)  → Job 1–4
     * @param gold   reporting layer (summary)   → Job-5 (customer_360_summary)
     * @param meta   watermark, rejected, audit
     */
    public record Schemas(@DefaultValue("curated") String silver,
                          @DefaultValue("gold") String gold,
                          @DefaultValue("meta") String meta) {
    }

    /**
     * Ek job na settings.
     *
     * @param enabled                 false hoy to koi pan trigger thi job na chale
     * @param scheduleEnabled         in-app scheduler aa job chalave ke nahi
     * @param scheduleCron            Spring cron (6 field): "0 0 0/4 * * *" = darek 4 kalake
     * @param rejectThresholdPercent  koi source ma reject % aathi vadhe to job FAIL (data quality gate)
     * @param rejectGateMinRecords    aatha ochha records hoy to gate check na karo (nana batch ma % no arth nathi)
     * @param params                  job specific settings (jem ke payment-order-lookback-days)
     */
    public record JobConfig(@DefaultValue("true") boolean enabled,
                            @DefaultValue("false") boolean scheduleEnabled,
                            String scheduleCron,
                            @DefaultValue("100") double rejectThresholdPercent,
                            @DefaultValue("0") long rejectGateMinRecords,
                            Map<String, String> params) {

        /** Job param vanche che. yml ma na hoy to default value. */
        public String param(String key, String defaultValue) {
            if (params == null) {
                return defaultValue;
            }
            return params.getOrDefault(key, defaultValue);
        }
    }

    /**
     * In-app scheduler.
     *
     * @param enabled  master switch. false hoy to koi job schedule nahi thay (prod: Airflow chalave)
     * @param poolSize scheduler threads
     */
    public record Scheduler(@DefaultValue("false") boolean enabled, @DefaultValue("2") int poolSize) {
    }

    /**
     * API thi async (background) job chalavva mate thread pool.
     *
     * @param poolSize      ek sathe ketla jobs chali shake
     * @param queueCapacity line ma ketla jobs rahi shake (bharai jay to API 503 aape)
     */
    public record Executor(@DefaultValue("2") int poolSize, @DefaultValue("10") int queueCapacity) {
    }

    /**
     * Airflow CLI mode: {@code --etl.cli.job=J2_SALES --etl.cli.window-end=2026-10-21T04:00+05:30}
     *
     * @param job       chalavvano job (khali hoy to CLI mode bandh)
     * @param windowEnd data kya sudhi vanchvo (khali hoy to "have")
     */
    public record Cli(String job, String windowEnd) {
    }
}
