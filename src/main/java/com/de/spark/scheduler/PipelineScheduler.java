package com.de.spark.scheduler;

import com.de.spark.common.JobRunAuditService;
import com.de.spark.pipeline.bronze2silver.BronzeToSilverJob;
import com.de.spark.pipeline.silver2gold.SilverToGoldJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Runs the pipeline: Job #1 (Bronze -> Silver), then Job #2 (Silver -> Gold).
 * fixedDelay = the next run starts pipeline.interval-ms after the previous one FINISHED, so runs never overlap.
 * Job #2 is skipped when Job #1 failed (gold would otherwise be built from an incomplete silver).
 */
@Component
public class PipelineScheduler {

    private static final Logger log = LoggerFactory.getLogger(PipelineScheduler.class);

    private final BronzeToSilverJob bronzeToSilver;
    private final SilverToGoldJob silverToGold;
    private final JobRunAuditService audit;

    public PipelineScheduler(BronzeToSilverJob bronzeToSilver, SilverToGoldJob silverToGold, JobRunAuditService audit) {
        this.bronzeToSilver = bronzeToSilver;
        this.silverToGold = silverToGold;
        this.audit = audit;
    }

    @Scheduled(initialDelay = 5000, fixedDelayString = "${pipeline.interval-ms}")
    public void runPipeline() {
        try {
            bronzeToSilver.run();
        } catch (Exception e) {
            log.error("Job #1 failed, Job #2 skipped", e);
            audit.skipped(SilverToGoldJob.JOB, "skipped because " + BronzeToSilverJob.JOB + " failed");
            return;
        }
        try {
            silverToGold.run();
        } catch (Exception e) {
            log.error("Job #2 failed", e);
        }
    }
}
