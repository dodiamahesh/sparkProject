package com.demo.etl.framework.exception;

public class JobRunNotFoundException extends EtlException {

    public JobRunNotFoundException(long jobRunId) {
        super("Job run " + jobRunId + " not found");
    }
}
