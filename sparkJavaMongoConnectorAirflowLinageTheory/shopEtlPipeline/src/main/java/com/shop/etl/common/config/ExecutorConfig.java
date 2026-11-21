package com.shop.etl.common.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

/**
 * API thi aavta job ne BACKGROUND ma chalavva mate thread pool.
 *
 * Kem? Job ne minuto lagi shake. API request etlo samay khulli na rakhay (timeout thay).
 * Etle API job ne aa pool ma mukine turant runId pachho aape che (HTTP 202). Pachhi
 * GET /api/jobs/runs/{runId} thi status joi shakay (Airflow sensor aa j kare che).
 *
 * Pool nano rakhyo che (default 2): darek Spark job pote j ghana cores vapare che.
 * Ek sathe ghana jobs chalavo to badha dheema thay ane memory pan khutay.
 */
@Configuration
public class ExecutorConfig {

    /**
     * "etlJobExecutor" naam no thread pool.
     * Queue bharai jay to navo job reject thay (JobLauncher ene 503 ma badle che).
     * Application bandh thay tyare chalta jobs puro thay e mate wait kare che.
     */
    @Bean(name = "etlJobExecutor")
    public ThreadPoolTaskExecutor etlJobExecutor(EtlProperties properties) {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(properties.executor().poolSize());
        executor.setMaxPoolSize(properties.executor().poolSize());
        executor.setQueueCapacity(properties.executor().queueCapacity());
        executor.setThreadNamePrefix("etl-job-");
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setAwaitTerminationSeconds(300);
        executor.initialize();
        return executor;
    }
}
