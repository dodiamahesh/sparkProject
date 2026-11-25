package com.tcs.dmj.dwh.pipeline.vatreturn;

import com.tcs.dmj.dwh.connector.source.SourceBatch;
import com.tcs.dmj.dwh.connector.source.SourceDefinition;
import com.tcs.dmj.dwh.connector.target.DateDimension;
import com.tcs.dmj.dwh.connector.target.DirectPostgresWriter.KeyLookup;
import com.tcs.dmj.dwh.core.AbstractEtlPipeline;
import com.tcs.dmj.dwh.core.ColumnLineage;
import com.tcs.dmj.dwh.core.PipelineContext;
import com.tcs.dmj.dwh.core.PipelineToolkit;
import com.tcs.dmj.dwh.enums.MappingType;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.stereotype.Component;

import java.util.List;

import static com.tcs.dmj.dwh.pipeline.vatreturn.VatReturnSchemaMapper.toDimTaxPeriod;
import static com.tcs.dmj.dwh.pipeline.vatreturn.VatReturnSchemaMapper.toDimTaxpayer;
import static com.tcs.dmj.dwh.pipeline.vatreturn.VatReturnSchemaMapper.toFactVatReturn;
import static com.tcs.dmj.dwh.pipeline.vatreturn.VatReturnSchemaMapper.toTyped;

/**
 * vat_return_coll -> star schema in silver: dim_taxpayer, dim_tax_period, dim_date, fact_vat_return.
 * Loaded directly (no staging), the whole run in ONE transaction.
 * Design and sample data: sample-data/vat-return-pipeline/vat-return-pipeline.md
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

    /** Surrogate keys of the fact, looked up inside the INSERT (business key column -> dimension key). */
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
        return "VAT return star: vat_return_coll -> taxpayer, period, form dims, fact_vat_return (all-or-nothing)";
    }

    @Override
    public List<SourceDefinition> sources() {
        return List.of(VatReturnSources.VAT_RETURNS);
    }

    @Override
    public List<String> targetTables() {
        return List.of(DIM_TAXPAYER, DIM_TAX_PERIOD, DateDimension.TABLE, FACT_VAT_RETURN);
    }

    @Override
    public boolean directLoad() {
        return true;
    }

    @Override
    public List<ColumnLineage> columnLineage() {
        String source = VatReturnSources.VAT_RETURNS.collection();
        return List.of(
                ColumnLineage.of(source, "ackNo", FACT_VAT_RETURN, "ack_no", MappingType.RENAMED),
                ColumnLineage.of(source, "tin", DIM_TAXPAYER, "tin", MappingType.DIRECT),
                ColumnLineage.of(source, "tin", FACT_VAT_RETURN, "taxpayer_key", MappingType.LOOKUP),
                ColumnLineage.of(source, "taxpayerName", DIM_TAXPAYER, "taxpayer_name", MappingType.RENAMED),
                ColumnLineage.of(source, "rtnRegisterVatDTO.crtDt", FACT_VAT_RETURN, "filed_at", MappingType.DERIVED),
                ColumnLineage.of(source, "vatComputationData.totalOutputTaxValue", FACT_VAT_RETURN, "total_output_tax",MappingType.RENAMED),
                ColumnLineage.of(source, "vatComputationData.netOutputTaxValue", FACT_VAT_RETURN, "net_output_tax",MappingType.RENAMED),
                ColumnLineage.of(source, "vatComputationData.netPayableValue", FACT_VAT_RETURN, "net_payable",MappingType.RENAMED),
                ColumnLineage.of(source, "penaltyAmount", FACT_VAT_RETURN, "penalty_amount", MappingType.RENAMED),
                ColumnLineage.of(source, "interest", FACT_VAT_RETURN, "interest_amount", MappingType.RENAMED),
                ColumnLineage.of(source, "netTax", FACT_VAT_RETURN, "net_tax", MappingType.RENAMED));
    }

    @Override
    public void run(PipelineContext context) {
        // extract + validate (ORPHAN: the form must exist in the seeded dim_tax_form)
        SourceBatch returns = extractAndValidate(context, 
        										 VatReturnSources.VAT_RETURNS,
        										 VatReturnSources.sanitizer(), 
        										 VatReturnSources.rules(readTable(DIM_TAX_FORM, "form_id")));

        // transform
        Dataset<Row> typed = context.cache(toTyped(returns.valid()));

        // load: dimensions first, then the fact
        loadDirect(context, toDimTaxpayer(typed), DIM_TAXPAYER, List.of("tin"));
        loadDirect(context, toDimTaxPeriod(typed), DIM_TAX_PERIOD, List.of("period_key"));
        loadDateDimensionDirect(context, typed, "filed_date", "due_date", "ack_date");
        loadDirect(context, toFactVatReturn(typed), FACT_VAT_RETURN, List.of("ack_no"), FACT_KEYS);

        // returns deleted at the source
        applyDeletes(context, returns, FACT_VAT_RETURN, "ack_no");
    }
}
