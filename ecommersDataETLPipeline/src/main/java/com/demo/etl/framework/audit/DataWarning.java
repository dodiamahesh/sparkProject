package com.demo.etl.framework.audit;

import java.time.LocalDateTime;

/** One row of etl_audit.etl_data_warning. */
public record DataWarning(
        long dataWarningId,
        long jobRunId,
        String pipelineName,
        String sourceCollection,
        String warningType,
        String sourceId,
        String businessKey,
        String warningMessage,
        LocalDateTime createdAt) {
}
