package com.tcs.dmj.dwh.service;

import com.tcs.dmj.dwh.core.PipelineContext;

/** Commit phase of a run: save the rejected records, then move last_run_time. */
public interface SourceCommitService {

    void commit(PipelineContext context);
}
