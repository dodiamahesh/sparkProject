package com.tcs.dmj.dwh.enums;

/**
 * Status of a pipeline run (meta.meta_pipeline_run.pipeline_status, NOT NULL, at most 20 characters).
 */
public enum PipelineStatus {

    /** The run is still busy (end_time is NULL). A run left RUNNING by a crash becomes FAILED at the next start-up. */
    RUNNING,

    /** Every record that was read is loaded: nothing rejected. */
    SUCCESS,

    /** Some records loaded, some rejected (see meta.meta_rejected_record). */
    SUCCESS_WITH_FAILED,

    /**
     * Nothing useful was loaded, for one of these reasons (see error_code / error_message):
     *   - every record that was read was rejected (rejects are saved, last_run_time moves on)
     *   - the job threw an exception / a quality gate stopped it (nothing saved, last_run_time did not move,
     *     the next run reads the same data again)
     *   - the application stopped while the job was running (closed at the next start-up)
     */
    FAILED;

    /**
     * Status of a run that finished without an exception.
     *
     * @param processedCount records handled successfully: valid (loaded) + soft-deleted at the source
     */
    public static PipelineStatus of(long processedCount, long rejectedCount) {
        if (rejectedCount == 0) {
            return SUCCESS;
        }
        return processedCount == 0 ? FAILED : SUCCESS_WITH_FAILED;
    }
}
