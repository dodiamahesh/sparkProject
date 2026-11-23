package com.tcs.dmj.dwh;

import com.tcs.dmj.dwh.batch.PipelineBatchRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * Entry point. ONE jar with every pipeline, two ways to run it:
 *
 *   REST service (default)   java -jar dmj-dwh-1.0.0.jar
 *                            POST http://localhost:8080/api/admin/dwh/pipelines/{pipelineName}/run
 *
 *   Batch (Airflow)          java -jar dmj-dwh-1.0.0.jar --run-pipeline={pipelineName}
 *                            no web server: runs that ONE pipeline, then exits with 0 (success) or 1 (failed),
 *                            see PipelineBatchRunner and airflow/README.md
 *
 * Bronze (MongoDB)  --Spark-->  validate / clean / de-duplicate  --Spark-->  Silver (PostgreSQL star schema)
 * Every run is incremental, by timestamp (meta.meta_pipeline.last_run_time).
 */
@SpringBootApplication
@ConfigurationPropertiesScan
public class DmjDwhApp {

    public static void main(String[] args) {
        SpringApplication application = new SpringApplication(DmjDwhApp.class);
        boolean batch = PipelineBatchRunner.isBatchMode(args);
        if (batch) {
            application.setWebApplicationType(WebApplicationType.NONE);
        }
        if (!batch) {
            application.run(args);
            return;
        }
        int exitCode;
        try {
            ConfigurableApplicationContext context = application.run(args);
            // stops Spark and the connection pool, then returns the exit code of PipelineBatchRunner
            exitCode = SpringApplication.exit(context);
        } catch (Throwable startupOrRunError) {
            // unknown pipeline, already running, database down ... (Spring has logged it and closed the context)
            exitCode = 1;
        }
        // explicit exit: a leftover non-daemon thread must never keep an Airflow task hanging
        System.exit(exitCode);
    }
}
