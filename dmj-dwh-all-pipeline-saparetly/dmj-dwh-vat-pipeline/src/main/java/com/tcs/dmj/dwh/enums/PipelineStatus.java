package com.tcs.dmj.dwh.enums;

/** Status of a run (meta.meta_pipeline_run.pipeline_status). */
public enum PipelineStatus {

    RUNNING,
    /** Everything read was loaded. */
    SUCCESS,
    /** Some rows loaded, some rejected. */
    SUCCESS_WITH_FAILED,
    /** Every row rejected, an exception, or the application stopped during the run. */
    FAILED;

    /** Status of a run that ended without an exception. processedCount = valid + soft-deleted rows. */
    public static PipelineStatus of(long processedCount, long rejectedCount) {
        if (rejectedCount == 0) {
            return SUCCESS;
        }
        return processedCount == 0 ? FAILED : SUCCESS_WITH_FAILED;
    }
}
