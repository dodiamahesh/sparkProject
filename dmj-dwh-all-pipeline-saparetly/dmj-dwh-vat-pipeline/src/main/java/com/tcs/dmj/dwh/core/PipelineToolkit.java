package com.tcs.dmj.dwh.core;

import com.tcs.dmj.dwh.connector.source.SourceProcessor;
import com.tcs.dmj.dwh.connector.target.DirectPostgresWriter;
import com.tcs.dmj.dwh.connector.target.PostgresReader;
import com.tcs.dmj.dwh.connector.target.PostgresWriter;
import com.tcs.dmj.dwh.connector.target.Scd2Writer;
import com.tcs.dmj.dwh.connector.target.SoftDeleteWriter;
import org.springframework.stereotype.Component;

/** The framework services a pipeline needs, in one constructor parameter. */
@Component
public record PipelineToolkit(
        SourceProcessor sourceProcessor,
        PostgresWriter postgresWriter,
        DirectPostgresWriter directWriter,
        PostgresReader postgresReader,
        Scd2Writer scd2Writer,
        SoftDeleteWriter softDeleteWriter,
        LineageCollector lineageCollector) {
}
