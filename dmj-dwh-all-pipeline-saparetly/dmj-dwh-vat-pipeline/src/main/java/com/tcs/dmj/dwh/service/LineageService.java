package com.tcs.dmj.dwh.service;

import com.tcs.dmj.dwh.core.EtlPipeline;
import com.tcs.dmj.dwh.core.PipelineContext;

/** Writes the lineage of a successful run (assets, table lineage, column lineage). */
public interface LineageService {

    void record(EtlPipeline pipeline, PipelineContext context);
}
