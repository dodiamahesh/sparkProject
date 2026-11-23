package com.tcs.dmj.dwh.service;

import com.tcs.dmj.dwh.pipeline.PipelineContext;

/**
 * The "commit" phase of a run: save the rejected records and advance the watermarks.
 * Called only after the pipeline loaded all its tables successfully.
 * Implementation: {@link com.tcs.dmj.dwh.service.impl.SourceCommitServiceImpl}.
 */
public interface SourceCommitService {

    void commit(PipelineContext context);
}
