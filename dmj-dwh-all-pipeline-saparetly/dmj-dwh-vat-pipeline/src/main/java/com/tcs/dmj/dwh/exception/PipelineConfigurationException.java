package com.tcs.dmj.dwh.exception;

/** The code or the database is set up wrong (bad pipeline name, missing table / column, wrong load mode ...). */
public class PipelineConfigurationException extends EtlException {

    public PipelineConfigurationException(String message) {
        super(message);
    }

    public PipelineConfigurationException(String message, Throwable cause) {
        super(message, cause);
    }
}
