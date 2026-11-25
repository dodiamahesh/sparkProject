package com.tcs.dmj.dwh.exception;

/** A source sent soft deletes but the pipeline never applied them: the run fails so no delete is lost. */
public class SoftDeletesNotAppliedException extends EtlException {

    public SoftDeletesNotAppliedException(String collection, long deletedCount) {
        super(deletedCount + " document(s) of " + collection + " were soft-deleted at the source, but the pipeline "
                + "did not call applyDeletes(...) for them. last_run_time was not moved.");
    }
}
