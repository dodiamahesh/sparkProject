package com.shop.etl.common.quality;

import com.shop.etl.common.config.EtlProperties.JobConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DATA QUALITY GATE: ek source ma reject % bahu vadhe to job ne ROKO.
 *
 * Kem? Source system ma koi motu bug aave (jem ke date format badlai gayu) to 90% data reject
 * thay. Aava samaye adhuro data curated ma load karvo ane watermark aagal vadhaarvo khatarnak
 * che: dashboard khota numbers batave, ane e data farithi kyarey nahi aave.
 * Gate fail thay to:
 *   - rejects meta_rejected ma lakhay j che (tapas mate)
 *   - silver table ma kai load NATHI thatu
 *   - job FAILED, watermark aagal NATHI vadhatu → source sudhare pachhi re-run thi data aavshe
 *
 * Settings (yml, job wise):
 *   reject-threshold-percent: 5       → 5% thi vadhu reject = fail
 *   reject-gate-min-records:  1000    → 1000 thi ochha records hoy to check na karo
 *                                       (10 mathi 2 reject = 20%, pan e chinta ni vaat nathi)
 */
public final class QualityGate {

    private static final Logger log = LoggerFactory.getLogger(QualityGate.class);

    private QualityGate() {
    }

    /**
     * @throws DataQualityException reject % threshold thi vadhu hoy to
     */
    public static void check(String source, long total, long rejected, JobConfig config) {
        if (total == 0 || total < config.rejectGateMinRecords()) {
            return;
        }
        double percent = rejected * 100.0 / total;
        log.info("Quality gate: source={}, total={}, rejected={}, reject%={}, threshold={}",
                source, total, rejected, String.format("%.2f", percent), config.rejectThresholdPercent());
        if (percent > config.rejectThresholdPercent()) {
            throw new DataQualityException(String.format(
                    "Source '%s' ma %.2f%% records reject thaya (threshold %.2f%%). Load rokyo. meta_rejected tapaso.",
                    source, percent, config.rejectThresholdPercent()));
        }
    }
}
