package com.shop.etl.common.trigger;

import com.shop.etl.common.config.EtlProperties;
import com.shop.etl.common.job.JobLauncher;
import com.shop.etl.common.job.LaunchResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

/**
 * TRIGGER-2 (Airflow CLI mode): jar ne command line thi chalavo, job chale, process bandh thay.
 *
 * <pre>
 *   java -jar shop-etl-pipeline-1.0.0-exec.jar \
 *        --spring.profiles.active=prod \
 *        --spring.main.web-application-type=none \     ← web server nahi
 *        --etl.scheduler.enabled=false \                ← scheduler nahi
 *        --etl.cli.job=J2_SALES \
 *        --etl.cli.window-end=2026-10-21T04:00:00+05:30
 * </pre>
 *
 * Exit code: 0 = SUCCESS / SKIPPED, 1 = FAILED. Airflow BashOperator exit code jove che:
 * 1 aave to task FAILED, ane Airflow pote retry kare.
 *
 * Kyare vaparvu? Jyare app hammesha chalu (server) na hoy: Airflow worker par, ke
 * KubernetesPodOperator thi container chalavo. Darek run ma JVM + Spark start thay (≈10–20 s vadhare).
 * App hammesha chalu hoy to API vali rit (HttpOperator) saari che.
 *
 * {@code etl.cli.job} khali hoy to aa class kai nathi karti (normal server mode).
 */
@Component
public class CliJobRunner implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(CliJobRunner.class);

    private final EtlProperties properties;
    private final JobLauncher launcher;
    private final ApplicationContext context;

    public CliJobRunner(EtlProperties properties, JobLauncher launcher, ApplicationContext context) {
        this.properties = properties;
        this.launcher = launcher;
        this.context = context;
    }

    /** Spring start thai jay pachhi aa chale. CLI job hoy to chalavi ne process bandh. */
    @Override
    public void run(ApplicationArguments args) {
        String job = properties.cli().job();
        if (job == null || job.isBlank()) {
            return;
        }
        int exitCode;
        try {
            LaunchResult result = launcher.launch(job, properties.cli().windowEnd(), false, "CLI");
            log.info("CLI run: {} → {} ({})", job, result.status(), result.message());
            exitCode = result.isFailure() ? 1 : 0;
        } catch (RuntimeException e) {
            log.error("CLI run fail: {}", job, e);
            exitCode = 1;
        }
        int code = exitCode;
        System.exit(SpringApplication.exit(context, () -> code));
    }
}
