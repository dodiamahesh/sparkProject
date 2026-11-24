package com.tcs.dmj.dwh.service;

import com.tcs.dmj.dwh.core.EtlPipeline;
import com.tcs.dmj.dwh.core.PipelineContext;

/**
 * Writes the lineage of a successful run: meta.meta_asset, meta.meta_lineage (table level, found automatically)
 * and meta.meta_column_lineage (selected columns, declared in {@link EtlPipeline#columnLineage()}).
 * Implementation: {@link com.tcs.dmj.dwh.service.impl.LineageServiceImpl}.
 */
public interface LineageService {

    void record(EtlPipeline pipeline, PipelineContext context);
}
