package com.demo.etl.framework.pipeline;

import com.demo.etl.framework.audit.StepTracker;
import com.demo.etl.framework.source.SourceProcessor;
import com.demo.etl.framework.target.PostgresReader;
import com.demo.etl.framework.target.PostgresWriter;
import org.springframework.stereotype.Component;

/**
 * Bundles the framework services a pipeline needs, so a pipeline constructor has ONE parameter.
 */
@Component
public class PipelineToolkit {

    private final SourceProcessor sourceProcessor;
    private final PostgresWriter postgresWriter;
    private final PostgresReader postgresReader;
    private final StepTracker stepTracker;

    public PipelineToolkit(SourceProcessor sourceProcessor, PostgresWriter postgresWriter,
                           PostgresReader postgresReader, StepTracker stepTracker) {
        this.sourceProcessor = sourceProcessor;
        this.postgresWriter = postgresWriter;
        this.postgresReader = postgresReader;
        this.stepTracker = stepTracker;
    }

    public SourceProcessor sourceProcessor() {
        return sourceProcessor;
    }

    public PostgresWriter postgresWriter() {
        return postgresWriter;
    }

    public PostgresReader postgresReader() {
        return postgresReader;
    }

    public StepTracker stepTracker() {
        return stepTracker;
    }
}
