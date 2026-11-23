package com.tcs.dmj.dwh.service;

public class JobRunNotFoundException extends EtlException {

    public JobRunNotFoundException(long jobRunId) {
        super("Job run " + jobRunId + " not found");
    }
}
