package com.demo.etl.pipelines.transaction;

import com.demo.etl.framework.quality.DataQualityRules;
import com.demo.etl.framework.quality.DataSanitizer;
import com.demo.etl.framework.source.SourceDefinition;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * WHAT we read from MongoDB and HOW we clean + validate it, for all 4 collections of this pipeline.
 */
final class TransactionSources {

    private static final String[] CURRENCIES = {"INR", "USD", "EUR", "GBP"};

    private TransactionSources() {
    }

    // =================================================================== customers
    static final SourceDefinition CUSTOMERS = SourceDefinition.builder("customers")
            .fields("customerId", "firstName", "lastName", "email", "phone", "dateOfBirth", "gender",
                    "city", "state", "country", "kycStatus", "createdAt", "updatedAt")
            .businessKey("customerId")
            .latestRecordColumn("updatedAt")
            .build();

    static DataSanitizer customerSanitizer() {
        return DataSanitizer.create()
                .trimAllAndBlankToNull()
                .toProperCase("firstName", "lastName", "city", "state")
                .toLowerCase("email")
                .toUpperCase("gender", "kycStatus")
                .normalizeIndianPhone("phone");
    }

    static DataQualityRules customerRules() {
        return DataQualityRules.create()
                .mustBeDate("dateOfBirth")
                .required("customerId", "firstName", "lastName", "email", "phone")
                .mustMatch("customerId", "^CUST-[0-9]{4}$", "must look like CUST-0001")
                .mustBeValidEmail("email")
                .mustMatch("phone", "^[6-9][0-9]{9}$", "must be a 10 digit Indian mobile number")
                .mustNotBeInFuture("dateOfBirth")
                .mustBeOneOf("gender", "M", "F", "O")
                .mustBeOneOf("kycStatus", "VERIFIED", "PENDING", "REJECTED");
    }

    // =================================================================== accounts
    static final SourceDefinition ACCOUNTS = SourceDefinition.builder("accounts")
            .fields("accountId", "customerId", "accountType", "branchCode", "ifscCode", "balance",
                    "currency", "status", "openedDate", "updatedAt")
            .businessKey("accountId")
            .latestRecordColumn("updatedAt")
            .build();

    static DataSanitizer accountSanitizer() {
        return DataSanitizer.create()
                .trimAllAndBlankToNull()
                .toUpperCase("accountType", "currency", "status", "ifscCode", "branchCode")
                .removeThousandSeparators("balance");
    }

    /** @param customerIds every customer_id already in silver.dim_customer */
    static DataQualityRules accountRules(Dataset<Row> customerIds) {
        return DataQualityRules.create()
                .mustBeDecimal("balance")
                .mustBeDate("openedDate")
                .required("accountId", "customerId", "accountType", "currency", "status")
                .mustMatch("accountId", "^ACC-[0-9]{4}$", "must look like ACC-0001")
                .mustNotBeNegative("balance")
                .mustNotBeInFuture("openedDate")
                .mustBeOneOf("accountType", "SAVINGS", "CURRENT", "SALARY", "NRI")
                .mustBeOneOf("currency", CURRENCIES)
                .mustBeOneOf("status", "ACTIVE", "DORMANT", "CLOSED")
                .mustMatch("ifscCode", "^[A-Z]{4}0[A-Z0-9]{6}$", "is not a valid IFSC code")
                .mustExistIn("customerId", customerIds, "customer_id", "dim_customer");
    }

    // =================================================================== merchants
    static final SourceDefinition MERCHANTS = SourceDefinition.builder("merchants")
            .fields("merchantId", "merchantName", "category", "mccCode", "city", "country", "status",
                    "onboardedDate", "updatedAt")
            .businessKey("merchantId")
            .latestRecordColumn("updatedAt")
            .build();

    static DataSanitizer merchantSanitizer() {
        return DataSanitizer.create()
                .trimAllAndBlankToNull()
                .toUpperCase("category", "status");
    }

    static DataQualityRules merchantRules() {
        return DataQualityRules.create()
                .mustBeDate("onboardedDate")
                .required("merchantId", "merchantName", "category")
                .mustMatch("merchantId", "^MER-[0-9]{4}$", "must look like MER-0001")
                .mustBeOneOf("category", "GROCERY", "ELECTRONICS", "FUEL", "TRAVEL", "DINING",
                        "HEALTHCARE", "ENTERTAINMENT", "UTILITIES")
                .mustMatch("mccCode", "^[0-9]{4}$", "must be a 4 digit MCC code")
                .mustNotBeInFuture("onboardedDate")
                .mustBeOneOf("status", "ACTIVE", "INACTIVE");
    }

    // =================================================================== transactions
    static final SourceDefinition TRANSACTIONS = SourceDefinition.builder("transactions")
            .fields("transactionId", "accountId", "customerId", "merchantId", "transactionType", "channel",
                    "amount", "currency", "transactionDate", "status", "description", "updatedAt")
            .businessKey("transactionId")
            .latestRecordColumn("updatedAt")
            .build();

    static DataSanitizer transactionSanitizer() {
        return DataSanitizer.create()
                .trimAllAndBlankToNull()
                .toUpperCase("transactionType", "channel", "currency", "status")
                .removeThousandSeparators("amount");
    }

    /** The three lookups make sure every transaction points to an existing customer / account / merchant. */
    static DataQualityRules transactionRules(Dataset<Row> customerIds, Dataset<Row> accountIds, Dataset<Row> merchantIds) {
        return DataQualityRules.create()
                .mustBeDecimal("amount")
                .mustBeTimestamp("transactionDate")
                .required("transactionId", "accountId", "customerId", "merchantId", "transactionType",
                        "channel", "amount", "currency", "transactionDate", "status")
                .mustMatch("transactionId", "^TXN-[0-9]{6}$", "must look like TXN-000001")
                .mustBePositive("amount")
                .mustNotBeInFuture("transactionDate")
                .mustBeOneOf("transactionType", "DEBIT", "CREDIT")
                .mustBeOneOf("channel", "POS", "ONLINE", "ATM", "UPI", "NETBANKING")
                .mustBeOneOf("currency", CURRENCIES)
                .mustBeOneOf("status", "SUCCESS", "FAILED", "PENDING", "REVERSED")
                .mustExistIn("customerId", customerIds, "customer_id", "dim_customer")
                .mustExistIn("accountId", accountIds, "account_id", "dim_account")
                .mustExistIn("merchantId", merchantIds, "merchant_id", "dim_merchant");
    }
}
