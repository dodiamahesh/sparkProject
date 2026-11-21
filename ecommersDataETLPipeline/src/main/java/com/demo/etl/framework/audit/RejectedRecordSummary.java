package com.demo.etl.framework.audit;

/** Count of open rejected records per pipeline / collection / error type. */
public record RejectedRecordSummary(String pipelineName, String sourceCollection, String errorType, long recordCount) {
}
