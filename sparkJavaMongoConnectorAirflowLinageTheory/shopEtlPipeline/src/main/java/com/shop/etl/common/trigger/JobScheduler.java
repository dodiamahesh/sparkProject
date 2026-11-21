package com.shop.etl.common.trigger;

import com.shop.etl.common.config.EtlProperties;
import com.shop.etl.common.job.JobExceptions;
import com.shop.etl.common.job.JobLauncher;
import com.shop.etl.common.job.LaunchResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.SchedulingConfigurer;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;
import org.springframework.scheduling.support.CronTrigger;
import org.springframework.stereotype.Component;

/**
 * TRIGGER-3: application ni ANDAR scheduler (Spring cron).
 *
 * yml thi CHALE che, code ma cron nathi:
 * <pre>
 * etl:
 *   scheduler:
 *     enabled: true                  ← master switch
 *   jobs:
 *     "[J2_SALES]":
 *       schedule-enabled: true
 *       schedule-cron: "0 0 0/4 * * *"   ← sec min hour day month weekday (darek 4 kalake)
 * </pre>
 * Cron IST timezone ma chale che (etl.timezone).
 *
 * Kyare vaparvu? Airflow na hoy evu nanu setup / dev. Prod ma Airflow hoy to aa BANDH rakho,
 * nahi to banne chalave. (Lock to pan bijo run rokse, pan khoti "already running" warnings aave.)
 *
 * Run SYNC chale che (scheduler na thread ma): ek tick puro thay pachhi j aagli tick.
 */
@Component
public class JobScheduler implements SchedulingConfigurer {

    private static final Logger log = LoggerFactory.getLogger(JobScheduler.class);

    private final EtlProperties properties;
    private final JobLauncher launcher;

    public JobScheduler(EtlProperties properties, JobLauncher launcher) {
        this.properties = properties;
        this.launcher = launcher;
    }

    /**
     * Spring startup vakhte aa call kare che. yml ma schedule-enabled vala darek job mate
     * ek cron task register thay che.
     */
    @Override
    public void configureTasks(ScheduledTaskRegistrar registrar) {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(properties.scheduler().poolSize());
        scheduler.setThreadNamePrefix("etl-scheduler-");
        scheduler.initialize();
        registrar.setTaskScheduler(scheduler);

        if (!properties.scheduler().enabled() || properties.jobs() == null) {
            log.info("In-app scheduler BANDH che (etl.scheduler.enabled=false)");
            return;
        }
        properties.jobs().forEach((jobName, config) -> {
            if (config.enabled() && config.scheduleEnabled() && config.scheduleCron() != null) {
                registrar.addTriggerTask(() -> runScheduled(jobName),
                        new CronTrigger(config.scheduleCron(), properties.zoneId()));
                log.info("Scheduled: {} cron='{}' ({})", jobName, config.scheduleCron(), properties.timezone());
            }
        });
    }

    /**
     * Ek scheduled tick. Error aave to log j kare: scheduler thread mari na jay ane aagli tick chale.
     */
    private void runScheduled(String jobName) {
        try {
            LaunchResult result = launcher.launch(jobName, null, false, "SCHEDULER");
            log.info("Scheduler run: {} → {} ({})", jobName, result.status(), result.message());
        } catch (JobExceptions.JobAlreadyRunning e) {
            log.warn("Scheduler: {} haju chale che, aa tick skip", jobName);
        } catch (RuntimeException e) {
            log.error("Scheduler: {} launch fail", jobName, e);
        }
    }
}
