package com.shop.etl.jobs.sales.customer;

import com.shop.etl.common.quality.Deduplicator;
import com.shop.etl.common.quality.RejectType;
import com.shop.etl.common.quality.Rule;
import com.shop.etl.common.quality.RuleEngine;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.stereotype.Component;

import java.util.List;

import static com.shop.etl.dataset.raw.RawCustomer.CUSTOMER_CODE;
import static com.shop.etl.dataset.raw.RawCustomer.EMAIL;
import static com.shop.etl.dataset.raw.RawCustomer.ID;
import static com.shop.etl.dataset.raw.RawCustomer.NAME;
import static com.shop.etl.dataset.raw.RawCustomer.PHONE;
import static com.shop.etl.dataset.raw.RawCustomer.TIER;
import static com.shop.etl.dataset.raw.RawCustomer.UPDATED_AT;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.not;

/**
 * CUSTOMERS VALIDATION SERVICE (Job-2, Part A). Design doc Section 6.4.
 *
 * Kram (doc Section 3.1): Corrupt → Dedupe → Validation.
 * Customers ma koi type-conversion field nathi, etle CORRUPT rule nathi.
 *
 * | Code   | Prakar     | Rule                           | Aa data ma          |
 * |--------|------------|--------------------------------|---------------------|
 * | J2-D01 | Duplicate  | same customer_code: latest     | C005 (09-01 vado)   |
 * | J2-V00 | Validation | customer_code required         | (koi nahi)          |
 * | J2-V01 | Validation | name required                  | C022                |
 * | J2-V02 | Validation | email format x@y.z             | C021 test@@mail     |
 * | J2-V03 | Validation | phone 10 digit                 | (koi nahi)          |
 * | J2-V04 | Validation | tier Bronze/Silver/Gold/Platinum | (koi nahi)        |
 *
 * Rules nu ORDER e priority che: ek row ma be bhul hoy to pehlo rule j reason bane.
 * Navo rule umervo hoy to fakt list ma ek line umero.
 */
@Component
public class CustomerValidator {

    /** Saral email check: @ ek j vaar, pachhi domain ma "." (x@y.z). */
    static final String EMAIL_REGEX = "^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$";

    private static final Rule DUPLICATE = Rule.of("J2-D01", RejectType.DUPLICATE,
            "Same customer_code 2 vaar. Juno version chhodyo.", null, UPDATED_AT, col(UPDATED_AT));

    private static final List<Rule> VALIDATION = List.of(
            Rule.of("J2-V00", RejectType.VALIDATION, "customer_code missing",
                    col(CUSTOMER_CODE).isNull(), CUSTOMER_CODE, col(CUSTOMER_CODE)),
            Rule.of("J2-V01", RejectType.VALIDATION, "Name missing",
                    col(NAME).isNull(), NAME, col(NAME)),
            Rule.of("J2-V02", RejectType.VALIDATION, "Email format khoto",
                    col(EMAIL).isNull().or(not(col(EMAIL).rlike(EMAIL_REGEX))), EMAIL, col(EMAIL)),
            Rule.of("J2-V03", RejectType.VALIDATION, "Phone 10 digit nathi",
                    col(PHONE).isNull(), PHONE, col(CustomerSanitizer.PHONE_RAW)),
            Rule.of("J2-V04", RejectType.VALIDATION, "Tier allowed list ma nathi",
                    not(col(TIER).isin("Bronze", "Silver", "Gold", "Platinum")), TIER, col(TIER)));

    /**
     * @param sanitized CustomerSanitizer nu output
     * @return same data + {@code _reject} column (null = saru)
     */
    public Dataset<Row> validate(Dataset<Row> sanitized) {
        Dataset<Row> checked = RuleEngine.init(sanitized);
        checked = Deduplicator.markDuplicates(checked, List.of(CUSTOMER_CODE), UPDATED_AT, ID, DUPLICATE);
        return RuleEngine.apply(checked, VALIDATION);
    }
}
