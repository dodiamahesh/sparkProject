package com.tcs.dmj.dwh.repository;

import com.tcs.dmj.dwh.connector.source.LoadWindow;
import com.tcs.dmj.dwh.domain.MetaPipeline;
import com.tcs.dmj.dwh.exception.PipelineConfigurationException;
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
 * meta.meta_pipeline: the pipeline registry and the incremental watermark (last_run_time).
 * last_run_time moves forward only after a successful run, so a failed run is read again next time.
 */
@Repository
public class MetaPipelineRepository {

    public static final int MAX_NAME_LENGTH = 30;
    public static final int MAX_DESCRIPTION_LENGTH = 100;
    public static final int MAX_JOB_NAME_LENGTH = 50;
    public static final int MAX_DAG_ID_LENGTH = 30;

    private static final String VALID_NAME = "[A-Za-z0-9._-]+";

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

    private static final String INSERT_IF_NEW = """
            INSERT INTO meta.meta_pipeline
                   (pipeline_name, pipeline_type, application_job_name, pipeline_description, airflow_dag_id)
            SELECT CAST(? AS VARCHAR), CAST(? AS VARCHAR), CAST(? AS VARCHAR), CAST(? AS VARCHAR), CAST(? AS VARCHAR)
             WHERE NOT EXISTS (SELECT 1 FROM meta.meta_pipeline WHERE pipeline_name = ?)
            ON CONFLICT DO NOTHING
            """;

    private static final String UPDATE_IF_CHANGED = """
            UPDATE meta.meta_pipeline
               SET pipeline_type = ?, application_job_name = ?, pipeline_description = ?, airflow_dag_id = ?,
                   active_flag = 'Y'
             WHERE pipeline_name = ?
               AND (pipeline_type IS DISTINCT FROM CAST(? AS VARCHAR)
                    OR application_job_name IS DISTINCT FROM CAST(? AS VARCHAR)
                    OR pipeline_description IS DISTINCT FROM CAST(? AS VARCHAR)
                    OR airflow_dag_id IS DISTINCT FROM CAST(? AS VARCHAR)
                    OR active_flag <> 'Y')
            """;

    private final JdbcTemplate jdbc;

    public MetaPipelineRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /** Inserts a new pipeline or updates a changed one (never touches last_run_time). */
    public void register(String pipelineName, String pipelineType, String description, String applicationJobName,
                         String airflowDagId) {
        validateName(pipelineName);
        String jobName = truncate(applicationJobName, MAX_JOB_NAME_LENGTH);
        String shortDescription = truncate(description, MAX_DESCRIPTION_LENGTH);
        String dagId = truncate(airflowDagId, MAX_DAG_ID_LENGTH);

        jdbc.update(INSERT_IF_NEW, pipelineName, pipelineType, jobName, shortDescription, dagId, pipelineName);
        jdbc.update(UPDATE_IF_CHANGED, pipelineType, jobName, shortDescription, dagId, pipelineName,
                pipelineType, jobName, shortDescription, dagId);
    }

    /** Pipelines no longer in the code get active_flag = 'N' (their history stays). */
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

    /** The window of this run: (last_run_time, runEnd]. */
    public LoadWindow window(String pipelineName, Instant runEnd) {
        List<OffsetDateTime> lastRunTime = jdbc.queryForList(
                "SELECT last_run_time FROM meta.meta_pipeline WHERE pipeline_name = ?",
                OffsetDateTime.class, pipelineName);
        Instant start = lastRunTime.isEmpty() || lastRunTime.get(0) == null ? null : lastRunTime.get(0).toInstant();
        return new LoadWindow(start, runEnd);
    }

    public void advanceLastRunTime(String pipelineName, Instant runEnd) {
        int updated = jdbc.update("UPDATE meta.meta_pipeline SET last_run_time = ? WHERE pipeline_name = ?",
                OffsetDateTime.ofInstant(runEnd, ZoneOffset.UTC), pipelineName);
        if (updated != 1) {
            throw new PipelineConfigurationException("Pipeline " + pipelineName + " is not registered in meta.meta_pipeline");
        }
    }

    /** The name is also the Airflow DAG id. */
    private static void validateName(String pipelineName) {
        if (pipelineName.length() > MAX_NAME_LENGTH) {
            throw new PipelineConfigurationException("Pipeline name '" + pipelineName + "' is longer than "
                    + MAX_NAME_LENGTH + " characters");
        }
        if (!pipelineName.matches(VALID_NAME)) {
            throw new PipelineConfigurationException("Pipeline name '" + pipelineName
                    + "' may only contain letters, digits, '-', '_' and '.'");
        }
    }

    private static String truncate(String text, int maxLength) {
        return text == null || text.length() <= maxLength ? text : text.substring(0, maxLength);
    }
}
