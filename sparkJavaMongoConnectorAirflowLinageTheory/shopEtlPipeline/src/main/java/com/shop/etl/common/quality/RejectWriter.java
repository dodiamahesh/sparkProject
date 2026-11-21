package com.shop.etl.common.quality;

import com.shop.etl.common.config.EtlProperties;
import com.shop.etl.common.io.JdbcBatchWriter;
import com.shop.etl.common.io.JdbcStep;
import com.shop.etl.common.io.RowBinder;
import com.shop.etl.common.job.JobContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.struct;
import static org.apache.spark.sql.functions.to_json;

/**
 * Badha jobs na rejected records ne EK j table {@code meta.meta_rejected} ma lakhe che.
 *
 * Darek reject row ma:
 *   job_name, run_id, source_collection, source_key (P022 / O1008/line-2),
 *   reject_level (DOCUMENT / LINE), reject_type, rule_code, reason, bad_value,
 *   raw_document (reject vakhte ni aakhi row JSON ma: debug ane reprocess mate)
 *
 * Re-run safe: run shuru thay tyare {@link #deleteForRun} e run_id na juna rejects kadhe che.
 * Etle fail thayelo run farithi chalavo to rejects bev vaar nathi lakhata.
 *
 * Performance: raw_document JSON FAKT rejected rows mate j banay che (sari rows mate nahi).
 */
@Component
public class RejectWriter {

    private static final Logger log = LoggerFactory.getLogger(RejectWriter.class);

    private final JdbcBatchWriter writer;
    private final JdbcTemplate jdbc;
    private final String table;

    public RejectWriter(JdbcBatchWriter writer, JdbcTemplate jdbc, EtlProperties properties) {
        this.writer = writer;
        this.jdbc = jdbc;
        this.table = properties.postgres().schema().meta() + ".meta_rejected";
    }

    /** Run shuru thay tyare: aa run_id na juna rejects kadho (re-run mate). */
    public void deleteForRun(String runId) {
        int deleted = jdbc.update("DELETE FROM " + table + " WHERE run_id = ?", runId);
        if (deleted > 0) {
            log.info("Re-run: {} juna rejects kadhya (run_id={})", deleted, runId);
        }
    }

    /**
     * Rejected rows lakhe che.
     *
     * @param ctx       run info (job_name, run_id)
     * @param source    source collection naam (customers)
     * @param rejected  {@code _reject} column vala rows (RuleEngine.rejected)
     * @param sourceKey business key expression (col("customer_code") ke concat(order_no,"/line-",line_no))
     * @param level     DOCUMENT ke LINE
     */
    public void write(JobContext ctx, String source, Dataset<Row> rejected, Column sourceKey, String level) {
        Column[] snapshot = Arrays.stream(rejected.columns())
                .filter(c -> !c.equals(RuleEngine.REJECT))
                .map(c -> col("`" + c + "`"))
                .toArray(Column[]::new);

        Dataset<Row> rows = rejected.select(
                sourceKey.cast("string").as("source_key"),
                RuleEngine.field(RuleEngine.F_REJECT_TYPE).as("reject_type"),
                RuleEngine.field(RuleEngine.F_RULE_CODE).as("rule_code"),
                RuleEngine.field(RuleEngine.F_REASON).as("reason"),
                RuleEngine.field(RuleEngine.F_BAD_VALUE).as("bad_value"),
                to_json(struct(snapshot)).as("raw_document"));

        String sql = "INSERT INTO " + table + " (job_name, run_id, source_collection, source_key, reject_level, "
                + "reject_type, rule_code, reason, bad_value, raw_document) "
                + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CAST(? AS jsonb))";

        writer.write(table, rows, List.of(new JdbcStep("meta-rejected", sql,
                binder(ctx.jobName(), ctx.runId(), source, level))));
    }

    /**
     * Binder STATIC method ma che: lambda fakt aa 4 String capture kare, bean (this) nahi.
     * Etle Spark ene executor par mokli shake (Serializable).
     */
    private static RowBinder binder(String jobName, String runId, String source, String level) {
        return (ps, row, v) -> {
            v.constant(ps, 1, jobName);
            v.constant(ps, 2, runId);
            v.constant(ps, 3, source);
            v.string(ps, 4, row, "source_key");
            v.constant(ps, 5, level);
            v.string(ps, 6, row, "reject_type");
            v.string(ps, 7, row, "rule_code");
            v.string(ps, 8, row, "reason");
            v.string(ps, 9, row, "bad_value");
            v.string(ps, 10, row, "raw_document");
        };
    }
}
