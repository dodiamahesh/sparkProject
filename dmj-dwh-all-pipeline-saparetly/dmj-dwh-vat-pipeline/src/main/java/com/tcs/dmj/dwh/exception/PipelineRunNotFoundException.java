package com.tcs.dmj.dwh.exception;

public class PipelineRunNotFoundException extends EtlException {

    public PipelineRunNotFoundException(long pipelineRunId) {
        super("Pipeline run " + pipelineRunId + " not found");
    }
}
