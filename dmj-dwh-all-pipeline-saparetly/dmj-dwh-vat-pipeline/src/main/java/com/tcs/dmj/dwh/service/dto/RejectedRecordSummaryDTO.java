package com.tcs.dmj.dwh.service.dto;

/** Number of rejected records per pipeline / collection / error. */
public record RejectedRecordSummaryDTO(String pipelineName, String sourceCollection, int errorId, String errorType,
                                       String errorMessage, long recordCount) {
}
