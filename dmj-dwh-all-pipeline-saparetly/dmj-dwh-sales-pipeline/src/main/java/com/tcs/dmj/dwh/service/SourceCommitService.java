package com.tcs.dmj.dwh.service;

import com.tcs.dmj.dwh.core.PipelineContext;

/**
 * The "commit" phase of a run: save the rejected records and advance meta.meta_pipeline.last_run_time.
 * Called only after the pipeline loaded all its tables successfully.
 * Implementation: {@link com.tcs.dmj.dwh.service.impl.SourceCommitServiceImpl}.
 */
public interface SourceCommitService {

    void commit(PipelineContext context);
}
