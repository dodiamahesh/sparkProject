package com.shop.etl.jobs.sales.shared;

import com.shop.etl.common.config.EtlProperties;
import com.shop.etl.common.io.PostgresReader;
import com.shop.etl.dataset.silver.DimCustomer;
import com.shop.etl.dataset.silver.DimOrderStatus;
import com.shop.etl.dataset.silver.DimPaymentMethod;
import com.shop.etl.dataset.silver.DimProduct;
import com.shop.etl.dataset.silver.FactSales;
import com.shop.etl.dataset.silver.SilverTable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.stereotype.Component;

import static org.apache.spark.sql.functions.broadcast;

/**
 * Job-2 ne joita SILVER LOOKUP datasets (FK check ane surrogate key mate) ek jagyae.
 *
 * Orders ane payments banne aa vapare che. Darek method fakt jaruri COLUMNS j vanche che.
 *
 * Performance:
 *   - Nani dims (status 5, method 4, product hajaro) → {@code broadcast()}: darek Spark executor
 *     ne aakhi nani table ni copy male, ane moto dataset SHUFFLE vagar join thay.
 *   - Moti tables (dim_customer karodo rows sudhi jai shake) → partitioned parallel read.
 *     Spark AQE pote nakki kare ke broadcast join karvu ke sort-merge join.
 *   - fact_sales mathi fakt (order_no, customer_key), ane DB ma j GROUP BY + date filter.
 *
 * ⚠️ Aa datasets LAZY che: method call kare tyare read nathi thato. Join pachhi pehli Spark
 * action vakhte read thay. Etle orders part, customers load thai jay PACHHI j aa vanche che.
 */
@Component
public class SalesLookups {

    private final PostgresReader reader;
    private final String silver;

    public SalesLookups(PostgresReader reader, EtlProperties properties) {
        this.reader = reader;
        this.silver = properties.postgres().schema().silver();
    }

    /**
     * dim_customer ni BADHI versions (SCD2 point-in-time lookup mate):
     * customer_key, customer_code, valid_from, valid_to, is_current.
     */
    public Dataset<Row> customerVersions() {
        return reader.readPartitioned(
                "SELECT " + DimCustomer.CUSTOMER_KEY + ", " + DimCustomer.CUSTOMER_CODE + ", "
                        + DimCustomer.VALID_FROM + ", " + DimCustomer.VALID_TO + ", " + DimCustomer.IS_CURRENT
                        + " FROM " + table(DimCustomer.TABLE),
                DimCustomer.CUSTOMER_KEY);
    }

    /** dim_product: product_code → product_key (broadcast). */
    public Dataset<Row> products() {
        return broadcast(reader.read("SELECT " + DimProduct.PRODUCT_KEY + ", " + DimProduct.PRODUCT_CODE
                + " FROM " + table(DimProduct.TABLE)));
    }

    /** dim_order_status: status_code → order_status_key (broadcast). */
    public Dataset<Row> orderStatuses() {
        return broadcast(reader.read("SELECT " + DimOrderStatus.ORDER_STATUS_KEY + ", " + DimOrderStatus.STATUS_CODE
                + " FROM " + table(DimOrderStatus.TABLE)));
    }

    /** dim_payment_method: method_code → payment_method_key (broadcast). */
    public Dataset<Row> paymentMethods() {
        return broadcast(reader.read("SELECT " + DimPaymentMethod.PAYMENT_METHOD_KEY + ", " + DimPaymentMethod.METHOD_CODE
                + " FROM " + table(DimPaymentMethod.TABLE)));
    }

    /**
     * fact_sales mathi order_no → customer_key (payment nu FK check + customer_key).
     * Design doc: FK check fakt aa batch ma nahi, AAKHA curated table ma (juno order pan chale).
     *
     * @param fromDateKey null = badha orders (full load). Number (20260723) = fakt aa date pachhi na
     *                    orders (incremental: payment mostly order pachhi thoda divas ma aave).
     *                    Aakhi fact table scan na thay, ane ix_fact_sales_date index vapray.
     */
    public Dataset<Row> orderCustomers(Integer fromDateKey) {
        String where = fromDateKey == null ? "" : " WHERE " + FactSales.ORDER_DATE_KEY + " >= " + fromDateKey;
        return reader.read("SELECT " + FactSales.ORDER_NO + ", MIN(" + FactSales.CUSTOMER_KEY + ") AS "
                + FactSales.CUSTOMER_KEY + " FROM " + table(FactSales.TABLE) + where
                + " GROUP BY " + FactSales.ORDER_NO);
    }

    private String table(String name) {
        return SilverTable.qualified(silver, name);
    }
}
