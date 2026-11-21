package com.demo.etl.framework.exception;

import com.demo.etl.framework.common.ErrorMessages;

/**
 * Thrown when one step of a job fails. The message tells you WHICH step and WHY,
 * e.g. "Step 'Load silver.dim_payer' failed: PSQLException: connection refused".
 */
public class EtlStepException extends EtlException {

    private final String stepName;

    public EtlStepException(String stepName, Throwable cause) {
        super("Step '" + stepName + "' failed: " + ErrorMessages.rootCauseMessage(cause), cause);
        this.stepName = stepName;
    }

    public String getStepName() {
        return stepName;
    }
}
