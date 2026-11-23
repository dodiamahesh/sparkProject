package com.tcs.dmj.dwh;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

/**
 * Entry point.
 *
 * Bronze (MongoDB)  --Spark-->  validate / clean / de-duplicate  --Spark-->  Silver (PostgreSQL star schema)
 *
 * Pipelines run ONLY through the REST API (no scheduler, no startup runner):
 *   POST http://localhost:8080/api/admin/dwh/pipelines/{pipelineName}/run   (always incremental, by timestamp)
 */
@SpringBootApplication
@ConfigurationPropertiesScan
public class DmjDwhApp {

    public static void main(String[] args) {
        SpringApplication.run(DmjDwhApp.class, args);
    }
}
