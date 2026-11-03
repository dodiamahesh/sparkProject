package com.demo.etl.pipelines.payment;

import com.demo.etl.framework.quality.DataQualityRules;
import com.demo.etl.framework.quality.DataSanitizer;
import com.demo.etl.framework.source.SourceDefinition;

/**
 * WHAT we read from MongoDB and HOW we clean + validate it.
 * (Only configuration: no Spark logic here, so it is easy to review with business users.)
 */
final class PaymentSource {

    static final SourceDefinition PAYMENTS = SourceDefinition.builder("payments")
            .fields("paymentId", "payerId", "payerName", "payerEmail", "payerPhone", "payerCity",
                    "paymentMethod", "paymentProvider", "billerName", "billerCategory",
                    "amount", "currency", "paymentDate", "status", "referenceNumber", "updatedAt")
            .businessKey("paymentId")
            .latestRecordColumn("updatedAt")
            .build();

    private PaymentSource() {
    }

    static DataSanitizer sanitizer() {
        return DataSanitizer.create()
                .trimAllAndBlankToNull()
                .toUpperCase("paymentMethod", "paymentProvider", "currency", "status", "billerCategory")
                .toLowerCase("payerEmail")
                .toProperCase("payerName", "payerCity")
                .normalizeIndianPhone("payerPhone")
                .removeThousandSeparators("amount");
    }

    static DataQualityRules rules() {
        return DataQualityRules.create()
                // CORRUPT: wrong data type
                .mustBeDecimal("amount")
                .mustBeTimestamp("paymentDate")
                // VALIDATION: business rules
                .required("paymentId", "payerId", "payerName", "paymentMethod", "paymentProvider",
                        "billerName", "amount", "currency", "paymentDate", "status")
                .mustMatch("paymentId", "^PAY-[0-9]{4}$", "must look like PAY-0001")
                .mustBePositive("amount")
                .mustNotBeInFuture("paymentDate")
                .mustBeOneOf("paymentMethod", "UPI", "CARD", "NETBANKING", "WALLET")
                .mustBeOneOf("currency", "INR", "USD")
                .mustBeOneOf("status", "SUCCESS", "FAILED", "PENDING")
                .mustBeValidEmail("payerEmail")
                .mustMatch("payerPhone", "^[6-9][0-9]{9}$", "must be a 10 digit Indian mobile number");
    }
}
