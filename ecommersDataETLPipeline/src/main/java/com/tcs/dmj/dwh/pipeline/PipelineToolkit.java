package com.tcs.dmj.dwh.pipeline;

import com.tcs.dmj.dwh.connector.source.SourceProcessor;
import com.tcs.dmj.dwh.connector.target.DirectPostgresWriter;
import com.tcs.dmj.dwh.connector.target.PostgresReader;
import com.tcs.dmj.dwh.connector.target.PostgresWriter;
import com.tcs.dmj.dwh.connector.target.Scd2Writer;
import com.tcs.dmj.dwh.connector.target.SoftDeleteWriter;
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
    private final SoftDeleteWriter softDeleteWriter;
    private final LineageCollector lineageCollector;

    public PipelineToolkit(SourceProcessor sourceProcessor, PostgresWriter postgresWriter, DirectPostgresWriter directWriter,
                           PostgresReader postgresReader, Scd2Writer scd2Writer, SoftDeleteWriter softDeleteWriter,
                           LineageCollector lineageCollector) {
        this.sourceProcessor = sourceProcessor;
        this.postgresWriter = postgresWriter;
        this.directWriter = directWriter;
        this.postgresReader = postgresReader;
        this.scd2Writer = scd2Writer;
        this.softDeleteWriter = softDeleteWriter;
        this.lineageCollector = lineageCollector;
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

    public SoftDeleteWriter softDeleteWriter() {
        return softDeleteWriter;
    }

    public LineageCollector lineageCollector() {
        return lineageCollector;
    }
}
