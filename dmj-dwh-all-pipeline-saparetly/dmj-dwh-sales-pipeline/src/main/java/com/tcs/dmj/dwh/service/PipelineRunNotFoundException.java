package com.tcs.dmj.dwh.service;

public class PipelineRunNotFoundException extends EtlException {

    public PipelineRunNotFoundException(long pipelineRunId) {
        super("Pipeline run " + pipelineRunId + " not found");
    }
}
