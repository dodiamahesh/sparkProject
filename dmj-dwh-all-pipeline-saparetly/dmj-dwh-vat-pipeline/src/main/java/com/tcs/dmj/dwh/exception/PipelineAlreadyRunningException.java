package com.tcs.dmj.dwh.exception;

public class PipelineAlreadyRunningException extends EtlException {

    public PipelineAlreadyRunningException(String pipelineName) {
        super("Pipeline '" + pipelineName + "' is already running. Wait for it to finish and try again.");
    }
}
