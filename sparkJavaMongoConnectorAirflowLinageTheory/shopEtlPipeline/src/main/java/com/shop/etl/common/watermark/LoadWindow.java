package com.shop.etl.common.watermark;

import java.time.Instant;

/**
 * Ek source mate "kaya samay thi kaya samay sudhi no data vanchvo" e window.
 *
 * Niyam (design doc Section 3.3):
 *   watermark_field  >  start   AND   watermark_field  <=  end
 *
 * - start = meta.etl_watermark.last_watermark (chhello SUCCESS run kya sudhi vanchyu)
 * - start NULL hoy to FULL LOAD (koi lower bound nahi, badho data)
 * - end   = aa run no fix upper bound (Airflow data_interval_end ke trigger samay).
 *           End FIX rakhvo jaruri che: job chale tyare navo aavto data aa run ma nahi,
 *           aagla run ma jay. Etle koi record bev vaar nahi ke ek pan vaar chhutto nahi.
 *
 * @param start lower bound (exclusive), null = full load
 * @param end   upper bound (inclusive)
 */
public record LoadWindow(Instant start, Instant end) {

    /** Watermark nathi, etle aa source no pehlo (full) load che. */
    public boolean isFullLoad() {
        return start == null;
    }

    /**
     * Window khali che? (end <= start). Jem ke same window farithi trigger thayo pan
     * pehla j SUCCESS thai gayo hato. Aava case ma job skip thay che.
     */
    public boolean isEmpty() {
        return start != null && !end.isAfter(start);
    }

    /** Audit table na load_type column mate. */
    public String loadType() {
        return isFullLoad() ? "FULL" : "INCREMENTAL";
    }
}
