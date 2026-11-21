package com.shop.etl.common.job;

import com.shop.etl.common.audit.SourceStats;
import com.shop.etl.common.io.MongoSource;

import java.util.List;
import java.util.Map;

/**
 * Darek ETL job aa interface implement kare che (J1_PRODUCT_INVENTORY, J2_SALES, ...).
 *
 * Job ne FAKT potano business logic lakhvano che. Baki badhu framework (JobLauncher, JobRunner)
 * kare che:
 *   lock, run id, watermark window, audit JOB row, meta_rejected cleanup, watermark commit.
 *
 * Navo job umervo hoy to: aa interface implement karo ane {@code @Component} lagavo.
 * JobLauncher Spring mathi badha EtlJob beans shodhi le che. API, scheduler ane CLI ne kai
 * badlavu nathi padtu.
 */
public interface EtlJob {

    /** Unique naam, jem ke "J2_SALES". API path, yml settings ane audit ma aa j naam. */
    String name();

    /**
     * Aa job kaya Mongo sources watermark thi vanche che. Order e j processing order che.
     * Darek mate meta.etl_watermark ma ek row hoy che.
     */
    List<MongoSource> sources();

    /**
     * Kharekhar kaam. Darek source na stats pacha aapo (JOB row no saravalo framework gane che).
     * Koi pan exception aave to framework run ne FAILED kare che ane watermark nathi vadhartu.
     */
    Map<String, SourceStats> run(JobContext ctx);
}
