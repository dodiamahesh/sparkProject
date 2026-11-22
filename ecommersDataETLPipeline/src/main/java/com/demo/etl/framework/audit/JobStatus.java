package com.demo.etl.framework.audit;

/**
 * Final status of a pipeline run (meta.meta_pipeline_run.status).
 * While a run is still busy its status is NULL (end_time is NULL too).
 */
public enum JobStatus {

    /** Every record that was read is loaded: nothing rejected. */
    SUCCESS,

    /** Some records loaded, some rejected (see meta.meta_rejected_record). */
    SUCCESS_WITH_FAILED,

    /**
     * Nothing useful was loaded, for one of these reasons (see error_message):
     *   - every record that was read was rejected (rejects are saved, the watermark moves on)
     *   - the job threw an exception / a quality gate stopped it (nothing saved, the watermark did not move,
     *     the next run reads the same data again)
     *   - the application stopped while the job was running (closed at the next start-up)
     */
    FAILED;

    /**
     * Status of a run that finished without an exception.
     *
     * @param processedCount records handled successfully: valid (loaded) + soft-deleted at the source
     */
    public static JobStatus of(long processedCount, long rejectedCount) {
        if (rejectedCount == 0) {
            return SUCCESS;
        }
        return processedCount == 0 ? FAILED : SUCCESS_WITH_FAILED;
    }
}
