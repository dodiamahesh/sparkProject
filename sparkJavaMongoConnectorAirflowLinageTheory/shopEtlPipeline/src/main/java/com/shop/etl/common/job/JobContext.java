package com.shop.etl.common.job;

import com.shop.etl.common.config.EtlProperties.JobConfig;
import com.shop.etl.common.watermark.LoadWindow;

import java.time.Instant;
import java.util.Map;

/**
 * Ek RUN ni badhi mahiti, je job na darek bhag ne joie che. Run shuru thay tyare EK j vaar
 * bane che (JobLauncher), ane pachhi badha components ne parameter tarike male che.
 *
 * Kem? Run id, window, settings: aa badhu global variable ma ke darek class ma alag nathi
 * rakhvu. Ek object pass karo, ane badha same mahiti vapare.
 *
 * @param jobName     J2_SALES
 * @param runId       J2_20261021_0400 (job short code + window end IST). Same window no re-run
 *                    pan same run id le, etle audit ane meta_rejected sachi rite overwrite thay.
 * @param windowEnd   aa run kya sudhi no data vanche (badha sources mate same)
 * @param windows     darek source ni potani window (start alag hoi shake)
 * @param config      aa job na yml settings
 * @param triggeredBy API / AIRFLOW / SCHEDULER / CLI
 */
public record JobContext(String jobName,
                         String runId,
                         Instant windowEnd,
                         Map<String, LoadWindow> windows,
                         JobConfig config,
                         String triggeredBy) {

    /** Ek source ni window. Source register na hoy to error (programming bhul). */
    public LoadWindow window(String source) {
        LoadWindow window = windows.get(source);
        if (window == null) {
            throw new IllegalStateException("Source '" + source + "' job " + jobName + " ma register nathi");
        }
        return window;
    }

    /** Koi pan source full load ma che? To JOB row ma FULL lakhay. */
    public String loadType() {
        return windows.values().stream().anyMatch(LoadWindow::isFullLoad) ? "FULL" : "INCREMENTAL";
    }
}
