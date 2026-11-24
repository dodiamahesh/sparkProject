package com.tcs.dmj.dwh.repository;

import com.tcs.dmj.dwh.connector.source.LoadWindow;
import com.tcs.dmj.dwh.domain.MetaPipeline;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

/**
 * SQL for meta.meta_pipeline: the pipeline registry AND the incremental watermark (last_run_time).
 *
 * last_run_time rules:
 *   1. NULL              -> first run: read everything up to the run's start time
 *   2. next runs         -> read updatedAt &gt; last_run_time AND &lt;= run start time (for every source of the pipeline)
 *   3. moved forward ONLY after the whole run succeeded (commit phase), so a failed run changes nothing
 *      and the next run reads the same window again (the UPSERTs make that safe)
 */
@Repository
public class MetaPipelineRepository {

    public static final int MAX_NAME_LENGTH = 30;
    public static final int MAX_DESCRIPTION_LENGTH = 100;
    public static final int MAX_JOB_NAME_LENGTH = 50;
    public static final int MAX_DAG_ID_LENGTH = 30;

    private static final RowMapper<MetaPipeline> ROW_MAPPER = (rs, rowNum) -> new MetaPipeline(
            rs.getLong("pipeline_id"),
            rs.getString("pipeline_name"),
            rs.getString("pipeline_type"),
            rs.getString("airflow_dag_id"),
            rs.getString("application_job_name"),
            rs.getString("pipeline_description"),
            rs.getObject("last_run_time", OffsetDateTime.class),
            rs.getString("active_flag"),
            rs.getObject("created_at", OffsetDateTime.class),
            rs.getObject("updated_at", OffsetDateTime.class));

    private final JdbcTemplate jdbc;

    public MetaPipelineRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Inserts the pipeline, or updates its type / description / job name / Airflow DAG id (= pipeline name) when they
     * changed in the code.
     * last_run_time is never touched here. A re-registered pipeline becomes active again.
     * The INSERT runs only for a new pipeline (WHERE NOT EXISTS), so pipeline_id has no gaps.
     */
    public void register(String pipelineName, String pipelineType, String description, String applicationJobName,
                         String airflowDagId) {
        if (pipelineName.length() > MAX_NAME_LENGTH) {
            throw new IllegalStateException("Pipeline name '" + pipelineName + "' is longer than " + MAX_NAME_LENGTH
                    + " characters (meta.meta_pipeline.pipeline_name)");
        }
        if (!pipelineName.matches("[A-Za-z0-9._-]+")) {
            throw new IllegalStateException("Pipeline name '" + pipelineName + "' may only contain letters, digits, '-', '_'"
                    + " and '.': it is also the Airflow DAG id");
        }
        String jobName = truncate(applicationJobName, MAX_JOB_NAME_LENGTH);
        String shortDescription = truncate(description, MAX_DESCRIPTION_LENGTH);
        String dagId = truncate(airflowDagId, MAX_DAG_ID_LENGTH);
        jdbc.update("""
                INSERT INTO meta.meta_pipeline
                       (pipeline_name, pipeline_type, application_job_name, pipeline_description, airflow_dag_id)
                SELECT CAST(? AS VARCHAR), CAST(? AS VARCHAR), CAST(? AS VARCHAR), CAST(? AS VARCHAR), CAST(? AS VARCHAR)
                 WHERE NOT EXISTS (SELECT 1 FROM meta.meta_pipeline WHERE pipeline_name = ?)
                ON CONFLICT DO NOTHING
                """, pipelineName, pipelineType, jobName, shortDescription, dagId, pipelineName);
        jdbc.update("""
                UPDATE meta.meta_pipeline
                   SET pipeline_type = ?, application_job_name = ?, pipeline_description = ?, airflow_dag_id = ?,
                       active_flag = 'Y'
                 WHERE pipeline_name = ?
                   AND (pipeline_type IS DISTINCT FROM CAST(? AS VARCHAR)
                        OR application_job_name IS DISTINCT FROM CAST(? AS VARCHAR)
                        OR pipeline_description IS DISTINCT FROM CAST(? AS VARCHAR)
                        OR airflow_dag_id IS DISTINCT FROM CAST(? AS VARCHAR)
                        OR active_flag <> 'Y')
                """, pipelineType, jobName, shortDescription, dagId, pipelineName, pipelineType, jobName,
                shortDescription, dagId);
    }

    /** Pipelines that are in the table but no longer in the code: active_flag = 'N' (their history stays). */
    public int deactivateAllExcept(Collection<String> pipelineNames) {
        return jdbc.update("UPDATE meta.meta_pipeline SET active_flag = 'N' "
                        + "WHERE active_flag = 'Y' AND NOT (pipeline_name = ANY (?))",
                (Object) pipelineNames.toArray(String[]::new));
    }

    public List<MetaPipeline> findAll() {
        return jdbc.query("SELECT * FROM meta.meta_pipeline ORDER BY pipeline_name", ROW_MAPPER);
    }

    public Optional<MetaPipeline> findByName(String pipelineName) {
        return jdbc.query("SELECT * FROM meta.meta_pipeline WHERE pipeline_name = ?", ROW_MAPPER, pipelineName)
                .stream().findFirst();
    }

    /** The window this run reads (the same for every source of the pipeline). */
    public LoadWindow window(String pipelineName, Instant runEnd) {
        List<OffsetDateTime> last = jdbc.queryForList(
                "SELECT last_run_time FROM meta.meta_pipeline WHERE pipeline_name = ?",
                OffsetDateTime.class, pipelineName);
        Instant start = last.isEmpty() || last.get(0) == null ? null : last.get(0).toInstant();
        return new LoadWindow(start, runEnd);
    }

    /** After a successful run: every source of the pipeline has been read up to {@code runEnd}. */
    public void advanceLastRunTime(String pipelineName, Instant runEnd) {
        int updated = jdbc.update("UPDATE meta.meta_pipeline SET last_run_time = ? WHERE pipeline_name = ?",
                OffsetDateTime.ofInstant(runEnd, ZoneOffset.UTC), pipelineName);
        if (updated != 1) {
            throw new IllegalStateException("Pipeline " + pipelineName + " is not registered in meta.meta_pipeline");
        }
    }

    private static String truncate(String text, int maxLength) {
        return text == null || text.length() <= maxLength ? text : text.substring(0, maxLength);
    }
}
