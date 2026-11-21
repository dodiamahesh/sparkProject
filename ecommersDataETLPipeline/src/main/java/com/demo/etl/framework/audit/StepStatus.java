package com.demo.etl.framework.audit;

/** Final status of one step (meta.meta_pipeline_step.status). NULL while the step is still running. */
public enum StepStatus {
    SUCCESS,
    FAILED
}
