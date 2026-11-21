package com.demo.etl.framework.audit;

/** Number of open rejected records per pipeline / collection / error (meta.meta_error_master row). */
public record RejectedRecordSummary(String pipelineName, String sourceCollection, int errorId, String errorType,
                                    String errorMessage, long recordCount) {
}
