package com.demo.etl.pipelines.vatreturn;

import com.demo.etl.framework.pipeline.AbstractEtlPipeline;
import com.demo.etl.framework.pipeline.PipelineContext;
import com.demo.etl.framework.pipeline.PipelineToolkit;
import com.demo.etl.framework.source.SourceBatch;
import com.demo.etl.framework.source.SourceDefinition;
import com.demo.etl.framework.target.DateDimension;
import com.demo.etl.framework.target.DirectPostgresWriter.KeyLookup;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.stereotype.Component;

import java.util.List;

import static com.demo.etl.pipelines.vatreturn.VatReturnSchemaMapper.toDimTaxPeriod;
import static com.demo.etl.pipelines.vatreturn.VatReturnSchemaMapper.toDimTaxpayer;
import static com.demo.etl.pipelines.vatreturn.VatReturnSchemaMapper.toFactVatReturn;
import static com.demo.etl.pipelines.vatreturn.VatReturnSchemaMapper.toTyped;

/**
 * VAT returns -> STAR schema in silver, loaded DIRECTLY (no etl_staging tables) in ONE transaction.
 *
 *   vat_return_coll --+-- tin, name, category ...       -------------------> dim_taxpayer      (SCD1)
 *                     +-- month, year, from, to, due    -------------------> dim_tax_period
 *                     +-- filed / due / ack date        -------------------> silver.dim_date
 *                     +-- everything + 26 amounts, keys looked up in SQL --> fact_vat_return   (1 row per ackNo)
 *   seeded reference dimensions: dim_tax_form, dim_vat_return_type, dim_vat_return_status, dim_filing_channel
 *
 * directLoad() = true: every table above, the rejected records (+ new messages in meta_error_master) and the
 * watermark are written in ONE database transaction. COMMIT = all of it, any error = ROLLBACK = nothing.
 *
 * Design and sample data: sample-data/vat-return-pipeline/vat-return-pipeline.md
 * Trigger: POST /api/etl/pipelines/vat-return-pipeline/run
 */
@Component
public class VatReturnPipeline extends AbstractEtlPipeline {

    static final String DIM_TAXPAYER = "silver.dim_taxpayer";
    static final String DIM_TAX_PERIOD = "silver.dim_tax_period";
    static final String DIM_TAX_FORM = "silver.dim_tax_form";
    static final String DIM_RETURN_TYPE = "silver.dim_vat_return_type";
    static final String DIM_RETURN_STATUS = "silver.dim_vat_return_status";
    static final String DIM_FILING_CHANNEL = "silver.dim_filing_channel";
    static final String FACT_VAT_RETURN = "silver.fact_vat_return";

    /** Surrogate keys of the fact, resolved by sub-selects in the INSERT (same transaction as the dimensions). */
    private static final KeyLookup[] FACT_KEYS = {
            new KeyLookup("tin", DIM_TAXPAYER, "tin", "taxpayer_key"),
            new KeyLookup("form_id", DIM_TAX_FORM, "form_id", "tax_form_key"),
            new KeyLookup("return_type_code", DIM_RETURN_TYPE, "return_type_code", "return_type_key"),
            new KeyLookup("status_code", DIM_RETURN_STATUS, "status_code", "return_status_key"),
            new KeyLookup("channel_code", DIM_FILING_CHANNEL, "channel_code", "filing_channel_key")
    };

    public VatReturnPipeline(PipelineToolkit toolkit) {
        super(toolkit);
    }

    @Override
    public String name() {
        return "vat-return-pipeline";
    }

    @Override
    public String description() {
        return "VAT returns (vat_return_coll) -> star schema: dim_taxpayer, dim_tax_period, dim_tax_form, "
                + "dim_vat_return_type, dim_vat_return_status, dim_filing_channel, dim_date, fact_vat_return. "
                + "Direct load (no staging), all-or-nothing transaction";
    }

    @Override
    public List<SourceDefinition> sources() {
        return List.of(VatReturnSources.VAT_RETURNS);
    }

    @Override
    public List<String> targetTables() {
        return List.of(DIM_TAXPAYER, DIM_TAX_PERIOD, DateDimension.TABLE, FACT_VAT_RETURN);
    }

    /** All tables of this pipeline + rejects + watermark in ONE transaction, written without staging tables. */
    @Override
    public boolean directLoad() {
        return true;
    }

    @Override
    public void run(PipelineContext context) {
        // E + data quality (ORPHAN check: the form must exist in the seeded reference dimension)
        SourceBatch returns = extractAndValidate(context, VatReturnSources.VAT_RETURNS,
                VatReturnSources.sanitizer(), VatReturnSources.rules(readTable(DIM_TAX_FORM, "form_id")));

        // T
        Dataset<Row> typed = context.cache(toTyped(returns.valid()));

        // L: dimensions first (the fact references them), then the fact; all in the job's transaction
        loadDirect(context, toDimTaxpayer(typed), DIM_TAXPAYER, List.of("tin"));
        loadDirect(context, toDimTaxPeriod(typed), DIM_TAX_PERIOD, List.of("period_key"));
        loadDateDimensionDirect(context, typed, "filed_date", "due_date", "ack_date");
        loadDirect(context, toFactVatReturn(typed), FACT_VAT_RETURN, List.of("ack_no"), FACT_KEYS);

        // SOFT DELETES: returns deleted at the source (in the same transaction)
        applyDeletes(context, returns, FACT_VAT_RETURN, "ack_no");
    }
}
