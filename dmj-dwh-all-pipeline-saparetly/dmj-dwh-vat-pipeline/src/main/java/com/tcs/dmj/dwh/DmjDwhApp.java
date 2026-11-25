package com.tcs.dmj.dwh;

import com.tcs.dmj.dwh.batch.PipelineBatchRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * MongoDB (bronze) -> Spark -> PostgreSQL silver star schema. Two ways to run the same jar:
 * REST service:  java -jar dmj-dwh-1.0.0.jar
 * Batch mode:    java -jar dmj-dwh-1.0.0.jar --run-pipeline=<name>   (runs one pipeline, exits 0 / 1)
 */
@SpringBootApplication
@ConfigurationPropertiesScan
public class DmjDwhApp {

    public static void main(String[] args) {
        SpringApplication application = new SpringApplication(DmjDwhApp.class);
        if (!PipelineBatchRunner.isBatchMode(args)) {
            application.run(args);
            return;
        }
        application.setWebApplicationType(WebApplicationType.NONE);
        System.exit(runBatch(application, args));
    }

    /** Exit explicitly afterwards: a leftover thread must never keep an Airflow task hanging. */
    private static int runBatch(SpringApplication application, String[] args) {
        try {
            ConfigurableApplicationContext context = application.run(args);
            return SpringApplication.exit(context);   // stops Spark, returns the runner's exit code
        } catch (Throwable startupOrRunError) {
            return 1;   // Spring has logged it and closed the context
        }
    }
}
