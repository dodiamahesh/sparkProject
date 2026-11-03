package com.demo.etl.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

/**
 * Thread pool for jobs started with async=true.
 * The REST call returns immediately with the jobRunId, the job runs on this pool.
 */
@Configuration
public class AsyncConfig {

    public static final String ETL_JOB_EXECUTOR = "etlJobExecutor";

    @Bean(name = ETL_JOB_EXECUTOR)
    public ThreadPoolTaskExecutor etlJobExecutor(EtlProperties properties) {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(properties.job().threadPoolSize());
        executor.setMaxPoolSize(properties.job().threadPoolSize());
        executor.setQueueCapacity(properties.job().queueCapacity());
        executor.setThreadNamePrefix("etl-job-");
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setAwaitTerminationSeconds(60);
        executor.initialize();
        return executor;
    }
}
