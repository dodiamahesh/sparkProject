package com.tcs.dmj.dwh.config;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/**
 * Creates ONE SparkSession for the whole application.
 * A SparkSession is thread-safe, so every pipeline can share it.
 *
 * Where Spark runs is decided by the Spring profile (application-&lt;profile&gt;.yml):
 *   offDev           master = local[*]: driver and executors inside this JVM
 *   dev / test / prod master = spark://...:7077 (standalone cluster), CLIENT mode: the driver is this JVM
 *                    (REST service or Airflow task), the executors run on the cluster workers.
 * On a cluster the executors read MongoDB and PostgreSQL themselves, so they need the connector jars
 * (application.dwh.spark.executor-jars-dir) and must be able to reach the driver (spark.driver.host).
 */
@Configuration
public class SparkConfig {

    private static final Logger log = LoggerFactory.getLogger(SparkConfig.class);
    private static final String SPARK_JARS = "spark.jars";

    @Bean(destroyMethod = "stop")
    public SparkSession sparkSession(ApplicationProperties properties) {
        ApplicationProperties.Spark sparkProperties = properties.spark();
        requireResolved("application.dwh.spark.master", sparkProperties.master());
        requireResolved("application.dwh.spark.executor-jars-dir", sparkProperties.executorJarsDir());
        if (sparkProperties.config() != null) {
            sparkProperties.config().forEach((key, value) -> requireResolved(key, value));
        }

        SparkConf conf = new SparkConf()
                .setAppName(sparkProperties.appName())
                .setMaster(sparkProperties.master());

        if (sparkProperties.config() != null) {
            // blank = "not set" (e.g. ${SPARK_EXTRA_JARS:} without a value)
            sparkProperties.config().forEach((key, value) -> {
                if (value != null && !value.isBlank()) {
                    conf.set(key, value);
                }
            });
        }

        List<String> executorJars = executorJars(sparkProperties.executorJarsDir());
        if (!executorJars.isEmpty()) {
            List<String> jars = new ArrayList<>();
            if (!conf.get(SPARK_JARS, "").isBlank()) {
                jars.add(conf.get(SPARK_JARS));
            }
            jars.addAll(executorJars);
            conf.set(SPARK_JARS, String.join(",", jars));
        }

        log.info("Starting SparkSession (master={}, app={}, driver.host={}, executor jars={})",
                sparkProperties.master(), sparkProperties.appName(), conf.get("spark.driver.host", "-"),
                executorJars.size());
        return SparkSession.builder().config(conf).getOrCreate();
    }

    /**
     * Spring Boot keeps "${SPARK_DRIVER_HOST}" as plain text when the environment variable is missing.
     * Stop here with a clear message instead of a confusing Spark network error later.
     */
    static void requireResolved(String key, String value) {
        if (value != null && value.contains("${")) {
            throw new IllegalStateException("Spark setting " + key + " = " + value + " is not set. Set the environment "
                    + "variable for the active profile (see application-<profile>.yml), e.g. SPARK_DRIVER_HOST.");
        }
    }

    /** Every *.jar of the folder (sorted). Folder set but missing / empty = start-up error, never a silent gap. */
    static List<String> executorJars(String folder) {
        if (folder == null || folder.isBlank()) {
            return List.of();
        }
        Path dir = Path.of(folder);
        if (!Files.isDirectory(dir)) {
            throw new IllegalStateException("application.dwh.spark.executor-jars-dir = " + folder
                    + " is not a folder. Copy target/spark-jars (made by mvn package) there.");
        }
        try (Stream<Path> files = Files.list(dir)) {
            List<String> jars = files.filter(file -> file.getFileName().toString().endsWith(".jar"))
                    .map(file -> file.toAbsolutePath().toString())
                    .sorted()
                    .toList();
            if (jars.isEmpty()) {
                throw new IllegalStateException("No *.jar in " + folder + " (application.dwh.spark.executor-jars-dir)");
            }
            return jars;
        } catch (IOException e) {
            throw new IllegalStateException("Cannot read " + folder, e);
        }
    }
}
