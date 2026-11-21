package com.demo.etl.framework.pipeline;

import com.demo.etl.framework.audit.StepTracker;
import com.demo.etl.framework.source.SourceProcessor;
import com.demo.etl.framework.target.DirectPostgresWriter;
import com.demo.etl.framework.target.PostgresReader;
import com.demo.etl.framework.target.PostgresWriter;
import com.demo.etl.framework.target.Scd2Writer;
import org.springframework.stereotype.Component;

/**
 * Bundles the framework services a pipeline needs, so a pipeline constructor has ONE parameter.
 */
@Component
public class PipelineToolkit {

    private final SourceProcessor sourceProcessor;
    private final PostgresWriter postgresWriter;
    private final DirectPostgresWriter directWriter;
    private final PostgresReader postgresReader;
    private final Scd2Writer scd2Writer;
    private final StepTracker stepTracker;

    public PipelineToolkit(SourceProcessor sourceProcessor, PostgresWriter postgresWriter, DirectPostgresWriter directWriter,
                           PostgresReader postgresReader, Scd2Writer scd2Writer, StepTracker stepTracker) {
        this.sourceProcessor = sourceProcessor;
        this.postgresWriter = postgresWriter;
        this.directWriter = directWriter;
        this.postgresReader = postgresReader;
        this.scd2Writer = scd2Writer;
        this.stepTracker = stepTracker;
    }

    public SourceProcessor sourceProcessor() {
        return sourceProcessor;
    }

    public PostgresWriter postgresWriter() {
        return postgresWriter;
    }

    public DirectPostgresWriter directWriter() {
        return directWriter;
    }

    public PostgresReader postgresReader() {
        return postgresReader;
    }

    public Scd2Writer scd2Writer() {
        return scd2Writer;
    }

    public StepTracker stepTracker() {
        return stepTracker;
    }
}
