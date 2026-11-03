package com.demo.etl.pipelines.transaction;

import com.demo.etl.framework.common.SparkTypes;
import com.demo.etl.framework.target.DateDimension;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.concat_ws;

/**
 * TRANSFORM step: each source collection becomes one star-schema table.
 */
final class TransactionStarSchemaMapper {

    private TransactionStarSchemaMapper() {
    }

    static Dataset<Row> toDimCustomer(Dataset<Row> validCustomers) {
        return validCustomers.select(
                col("customerId").as("customer_id"),
                col("firstName").as("first_name"),
                col("lastName").as("last_name"),
                concat_ws(" ", col("firstName"), col("lastName")).as("full_name"),
                col("email"),
                col("phone"),
                col("dateOfBirth").cast(SparkTypes.DATE).as("date_of_birth"),
                col("gender"),
                col("city"),
                col("state"),
                col("country"),
                col("kycStatus").as("kyc_status"));
    }

    static Dataset<Row> toDimAccount(Dataset<Row> validAccounts) {
        return validAccounts.select(
                col("accountId").as("account_id"),
                col("customerId").as("customer_id"),
                col("accountType").as("account_type"),
                col("branchCode").as("branch_code"),
                col("ifscCode").as("ifsc_code"),
                col("currency"),
                col("status").as("account_status"),
                col("balance").cast(SparkTypes.MONEY).as("current_balance"),
                col("openedDate").cast(SparkTypes.DATE).as("opened_date"));
    }

    static Dataset<Row> toDimMerchant(Dataset<Row> validMerchants) {
        return validMerchants.select(
                col("merchantId").as("merchant_id"),
                col("merchantName").as("merchant_name"),
                col("category"),
                col("mccCode").as("mcc_code"),
                col("city"),
                col("country"),
                col("status").as("merchant_status"),
                col("onboardedDate").cast(SparkTypes.DATE).as("onboarded_date"));
    }

    static Dataset<Row> toTypedTransactions(Dataset<Row> validTransactions) {
        return validTransactions.select(
                col("transactionId").as("transaction_id"),
                col("customerId").as("customer_id"),
                col("accountId").as("account_id"),
                col("merchantId").as("merchant_id"),
                col("transactionType").as("transaction_type"),
                col("channel"),
                col("amount").cast(SparkTypes.MONEY).as("amount"),
                col("currency"),
                col("status").as("transaction_status"),
                col("description"),
                col("transactionDate").cast(SparkTypes.TIMESTAMP).as("transaction_ts"));
    }

    /** Expects customer_key, account_key and merchant_key to be added already. */
    static Dataset<Row> toFactTransaction(Dataset<Row> transactionsWithKeys) {
        return transactionsWithKeys.select(
                col("transaction_id"),
                col("customer_key"),
                col("account_key"),
                col("merchant_key"),
                DateDimension.dateKey(col("transaction_ts")).as("date_key"),
                col("transaction_type"),
                col("channel"),
                col("amount"),
                col("currency"),
                col("transaction_status"),
                col("description"),
                col("transaction_ts"));
    }
}
