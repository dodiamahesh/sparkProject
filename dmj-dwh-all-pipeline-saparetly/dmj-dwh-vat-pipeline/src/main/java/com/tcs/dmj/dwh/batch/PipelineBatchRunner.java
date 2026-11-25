package com.tcs.dmj.dwh.batch;

import com.tcs.dmj.dwh.config.ApplicationProperties;
import com.tcs.dmj.dwh.domain.MetaPipelineRun;
import com.tcs.dmj.dwh.enums.PipelineStatus;
import com.tcs.dmj.dwh.service.DwhPipelineService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.ExitCodeGenerator;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Batch mode (Airflow): java -jar dmj-dwh-1.0.0.jar --run-pipeline=vat-return-pipeline
 * runs ONE pipeline and exits: 0 = SUCCESS / SUCCESS_WITH_FAILED, 1 = FAILED or any error.
 * (application.dwh.batch.fail-on-rejected-records = true: SUCCESS_WITH_FAILED exits with 1 too.)
 */
@Component
public class PipelineBatchRunner implements ApplicationRunner, ExitCodeGenerator {

    public static final String RUN_PIPELINE_OPTION = "run-pipeline";

    private static final Logger log = LoggerFactory.getLogger(PipelineBatchRunner.class);

    private final DwhPipelineService pipelineService;
    private final boolean failOnRejectedRecords;
    private int exitCode = 0;

    public PipelineBatchRunner(DwhPipelineService pipelineService, ApplicationProperties properties) {
        this.pipelineService = pipelineService;
        this.failOnRejectedRecords = properties.batch().failOnRejectedRecords();
    }

    /** Checked in main, before Spring starts. */
    public static boolean isBatchMode(String[] args) {
        for (String arg : args) {
            if (arg.startsWith("--" + RUN_PIPELINE_OPTION + "=")) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void run(ApplicationArguments args) {
        List<String> values = args.getOptionValues(RUN_PIPELINE_OPTION);
        if (values == null) {
            return;   // REST service mode
        }
        String pipelineName = singlePipelineName(values);
        logStart(pipelineName);

        MetaPipelineRun run = pipelineService.runPipeline(pipelineName);
        PipelineStatus status = PipelineStatus.valueOf(run.pipelineStatus());
        exitCode = isSuccess(status) ? 0 : 1;

        // one line that is easy to find in the Airflow task log
        log.info("DWH_RESULT pipeline={} pipelineRunId={} status={} total={} success={} failed={} errorCode={} exitCode={}",
                pipelineName, run.pipelineRunId(), status, run.totalRecords(), run.successRecords(),
                run.failedRecords(), run.errorCode(), exitCode);
    }

    @Override
    public int getExitCode() {
        return exitCode;
    }

    private boolean isSuccess(PipelineStatus status) {
        return status == PipelineStatus.SUCCESS
                || (status == PipelineStatus.SUCCESS_WITH_FAILED && !failOnRejectedRecords);
    }

    private static String singlePipelineName(List<String> values) {
        if (values.size() != 1 || values.get(0).isBlank()) {
            throw new IllegalArgumentException("Give exactly one pipeline: --" + RUN_PIPELINE_OPTION + "=<name>");
        }
        return values.get(0).trim();
    }

    private static void logStart(String pipelineName) {
        String airflowRun = System.getenv("AIRFLOW_CTX_DAG_RUN_ID");   // set by Airflow for every task
        String airflowInfo = airflowRun == null ? ""
                : " (Airflow DAG " + System.getenv("AIRFLOW_CTX_DAG_ID") + ", run " + airflowRun + ")";
        log.info("Batch mode: running {}{}", pipelineName, airflowInfo);
    }
}
