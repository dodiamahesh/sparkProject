package com.demo.etl.pipelines.transaction;

import com.demo.etl.framework.pipeline.AbstractEtlPipeline;
import com.demo.etl.framework.pipeline.PipelineContext;
import com.demo.etl.framework.pipeline.PipelineToolkit;
import com.demo.etl.framework.source.SourceBatch;
import com.demo.etl.framework.source.SourceDefinition;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.stereotype.Component;

import java.util.List;

import static com.demo.etl.pipelines.transaction.TransactionStarSchemaMapper.toDimAccount;
import static com.demo.etl.pipelines.transaction.TransactionStarSchemaMapper.toDimCustomer;
import static com.demo.etl.pipelines.transaction.TransactionStarSchemaMapper.toDimMerchant;
import static com.demo.etl.pipelines.transaction.TransactionStarSchemaMapper.toFactTransaction;
import static com.demo.etl.pipelines.transaction.TransactionStarSchemaMapper.toTypedTransactions;

/**
 * EXAMPLE 2 - MULTIPLE collections -> star schema.
 *
 *   MongoDB: customers ---> dim_customer
 *            accounts  ---> dim_account    (account.customerId must exist in dim_customer)
 *            merchants ---> dim_merchant
 *            transactions -> fact_transaction  (+ dim_date)
 *                            every transaction must point to an existing customer, account and merchant,
 *                            otherwise it is rejected as ORPHAN
 *
 * Order matters: dimensions are loaded first, then the fact can find their keys.
 */
@Component
public class TransactionPipeline extends AbstractEtlPipeline {

    public TransactionPipeline(PipelineToolkit toolkit) {
        super(toolkit);
    }

    @Override
    public String name() {
        return "transaction-pipeline";
    }

    @Override
    public String description() {
        return "Multi collection example: customers + accounts + merchants + transactions -> "
                + "dim_customer, dim_account, dim_merchant, dim_date, fact_transaction";
    }

    @Override
    public List<SourceDefinition> sources() {
        return List.of(TransactionSources.CUSTOMERS, TransactionSources.ACCOUNTS,
                TransactionSources.MERCHANTS, TransactionSources.TRANSACTIONS);
    }

    @Override
    public List<String> targetTables() {
        return List.of("silver.dim_customer", "silver.dim_account", "silver.dim_merchant",
                "silver.dim_date", "silver.fact_transaction");
    }

    @Override
    public List<String> fullLoadTruncateTables() {
        return List.of("silver.fact_transaction");
    }

    @Override
    public void run(PipelineContext context) {
        // ---------------- 1. DIMENSIONS (one collection -> one dimension) ----------------
        SourceBatch customers = extractAndValidate(context, TransactionSources.CUSTOMERS,TransactionSources.customerSanitizer(), TransactionSources.customerRules());
        load(context, toDimCustomer(customers.valid()), "silver.dim_customer", "customer_id");

        // Lookups read the dimension tables, so they include rows loaded in THIS run and in EARLIER runs
        Dataset<Row> customerIds = readTable("silver.dim_customer", "customer_id");

        SourceBatch accounts = extractAndValidate(context, TransactionSources.ACCOUNTS,TransactionSources.accountSanitizer(), TransactionSources.accountRules(customerIds));
        load(context, toDimAccount(accounts.valid()), "silver.dim_account", "account_id");

        SourceBatch merchants = extractAndValidate(context, TransactionSources.MERCHANTS,TransactionSources.merchantSanitizer(), TransactionSources.merchantRules());
        load(context, toDimMerchant(merchants.valid()), "silver.dim_merchant", "merchant_id");

        // ---------------- 2. FACT (with ORPHAN checks against the dimensions) ----------------
        Dataset<Row> accountIds = readTable("silver.dim_account", "account_id");
        Dataset<Row> merchantIds = readTable("silver.dim_merchant", "merchant_id");

        SourceBatch transactions = extractAndValidate(context, TransactionSources.TRANSACTIONS,TransactionSources.transactionSanitizer(),TransactionSources.transactionRules(customerIds, accountIds, merchantIds));

        Dataset<Row> typedTransactions = toTypedTransactions(transactions.valid());
        loadDateDimension(context, typedTransactions, "transaction_ts");

        Dataset<Row> withKeys = addSurrogateKey(typedTransactions, "silver.dim_customer", "customer_key", "customer_id");
        withKeys = addSurrogateKey(withKeys, "silver.dim_account", "account_key", "account_id");
        withKeys = addSurrogateKey(withKeys, "silver.dim_merchant", "merchant_key", "merchant_id");
        load(context, toFactTransaction(withKeys), "silver.fact_transaction", "transaction_id");
    }
}
