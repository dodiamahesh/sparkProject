package com.tcs.dmj.dwh.service.dto;

/** Number of open rejected records per pipeline / collection / error (meta.meta_error_master row). */
public record RejectedRecordSummaryDTO(String pipelineName, String sourceCollection, int errorId, String errorType,
                                    String errorMessage, long recordCount) {
}
