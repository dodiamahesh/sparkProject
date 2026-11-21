package com.shop.etl.common.audit;

/**
 * Ek source (collection) na ek run na counts. meta.etl_run_audit ni ek row.
 *
 * Niyam (SUCCESS hoy tyare hammesha sachu):  total = processed + rejected
 *
 * Counts SOURCE DOCUMENTS na hisabe che (target rows nahi):
 *   - orders: 24 docs read, 4 reject, 20 processed. E 20 mathi 2 orders ni ek line reject
 *     thai (partial = 2). Target fact_sales ma 30 lines gai (inserted = 30).
 *
 * @param total     window ma vanchela source documents
 * @param processed load thaya (partial pan aama)
 * @param rejected  DOCUMENT level reject
 * @param partial   load thaya, pan ek ke vadhare line reject thai
 * @param inserted  target table ma navi rows (target rows na hisabe)
 * @param updated   target table ma badlayeli rows (same value hoy to update nathi thatu)
 */
public record SourceStats(long total, long processed, long rejected, long partial, long inserted, long updated) {

    /** Khali stats (zero). */
    public static SourceStats empty() {
        return new SourceStats(0, 0, 0, 0, 0, 0);
    }

    /** Be stats no saravalo: JOB summary row mate. */
    public SourceStats plus(SourceStats o) {
        return new SourceStats(total + o.total, processed + o.processed, rejected + o.rejected,
                partial + o.partial, inserted + o.inserted, updated + o.updated);
    }
}
