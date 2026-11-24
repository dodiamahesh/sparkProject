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
 * BATCH MODE, used by Airflow: the same jar runs ONE pipeline and exits.
 *
 * <pre>
 *   java -jar dmj-dwh-1.0.0.jar --spring.profiles.active=prod --run-pipeline=vat-return-pipeline
 * </pre>
 *
 * Without --run-pipeline the application is the REST service (nothing happens here). With it, there is no web
 * server (see DmjDwhApp), only the given pipeline runs, and the process exits with:
 *
 *   0 = SUCCESS, or SUCCESS_WITH_FAILED (rejected rows are in meta.meta_rejected_record)
 *       -&gt; the Airflow task succeeds and the DAGs that depend on this pipeline start
 *   1 = FAILED (or SUCCESS_WITH_FAILED when application.dwh.batch.fail-on-rejected-records = true), an unknown
 *       pipeline, the pipeline is already running, or a start-up error -&gt; the Airflow task fails (and retries)
 *
 * The run is recorded in meta.meta_pipeline_run exactly like a REST run.
 */
@Component
public class PipelineBatchRunner implements ApplicationRunner, ExitCodeGenerator {

    /** Command line option: --run-pipeline=&lt;pipeline name&gt; */
    public static final String RUN_PIPELINE_OPTION = "run-pipeline";

    private static final Logger log = LoggerFactory.getLogger(PipelineBatchRunner.class);

    private final DwhPipelineService pipelineService;
    private final boolean failOnRejectedRecords;
    private int exitCode = 0;

    public PipelineBatchRunner(DwhPipelineService pipelineService, ApplicationProperties properties) {
        this.pipelineService = pipelineService;
        this.failOnRejectedRecords = properties.batch().failOnRejectedRecords();
    }

    /** true when the command line asks for batch mode (checked in main, before Spring starts). */
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
        if (values.size() != 1 || values.get(0).isBlank()) {
            throw new IllegalArgumentException("Give exactly one pipeline: --" + RUN_PIPELINE_OPTION + "=<name>");
        }
        String pipelineName = values.get(0).trim();
        String airflowRun = System.getenv("AIRFLOW_CTX_DAG_RUN_ID");   // set by Airflow for every task
        log.info("Batch mode: running {}{}", pipelineName,
                airflowRun == null ? "" : " (Airflow DAG " + System.getenv("AIRFLOW_CTX_DAG_ID") + ", run " + airflowRun + ")");

        MetaPipelineRun run = pipelineService.runPipeline(pipelineName);
        PipelineStatus status = PipelineStatus.valueOf(run.pipelineStatus());
        boolean ok = status == PipelineStatus.SUCCESS
                || (status == PipelineStatus.SUCCESS_WITH_FAILED && !failOnRejectedRecords);
        exitCode = ok ? 0 : 1;

        // one line that is easy to find in the Airflow task log
        log.info("DWH_RESULT pipeline={} pipelineRunId={} status={} total={} success={} failed={} errorCode={} exitCode={}",
                pipelineName, run.pipelineRunId(), status, run.totalRecords(), run.successRecords(),
                run.failedRecords(), run.errorCode(), exitCode);
    }

    @Override
    public int getExitCode() {
        return exitCode;
    }
}
