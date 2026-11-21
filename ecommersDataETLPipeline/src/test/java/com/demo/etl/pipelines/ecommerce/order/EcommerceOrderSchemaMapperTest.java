package com.demo.etl.pipelines.ecommerce.order;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;

import static org.apache.spark.sql.functions.lit;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Plain unit test (no Spring, no databases) for the order-line transform:
 * typed columns, header keys taken from fact_order, and the gross / net amount measures.
 */
class EcommerceOrderSchemaMapperTest {

    private static SparkSession spark;

    @BeforeAll
    static void startSpark() {
        spark = SparkSession.builder()
                .master("local[1]")
                .appName("ecommerce-order-mapper-test")
                .config("spark.ui.enabled", "false")
                .config("spark.sql.ansi.enabled", "false")
                .config("spark.sql.shuffle.partitions", "1")
                .getOrCreate();
    }

    @AfterAll
    static void stopSpark() {
        spark.stop();
    }

    @Test
    void orderLineGetsCustomerAndDateFromItsOrderAndComputesAmounts() {
        StructType itemSchema = new StructType()
                .add("orderItemId", DataTypes.StringType)
                .add("orderId", DataTypes.StringType)
                .add("productId", DataTypes.StringType)
                .add("quantity", DataTypes.StringType)
                .add("unitPrice", DataTypes.StringType)
                .add("discountAmount", DataTypes.StringType);
        Dataset<Row> validItems = spark.createDataFrame(List.of(
                RowFactory.create("OIT-000001", "ORD-00001", "PRD-0001", "3", "1499.50", "150.00")), itemSchema);

        // what silver.fact_order already contains for that order
        StructType headerSchema = new StructType()
                .add("order_id", DataTypes.StringType)
                .add("customer_key", DataTypes.LongType)
                .add("date_key", DataTypes.IntegerType);
        Dataset<Row> orderHeaders = spark.createDataFrame(List.of(
                RowFactory.create("ORD-00001", 7L, 20260315)), headerSchema);

        Dataset<Row> withKeys = EcommerceOrderSchemaMapper
                .withOrderHeaderKeys(EcommerceOrderSchemaMapper.toTypedOrderItems(validItems), orderHeaders)
                .withColumn("product_key", lit(11L));   // normally added by addSurrogateKey
        Dataset<Row> fact = EcommerceOrderSchemaMapper.toFactOrderItem(withKeys);

        assertArrayEquals(new String[]{"order_item_id", "order_id", "product_key", "customer_key", "date_key",
                "quantity", "unit_price", "discount_amount", "gross_amount", "net_amount"}, fact.columns());

        Row row = fact.first();
        assertEquals(7L, row.getLong(row.fieldIndex("customer_key")));
        assertEquals(20260315, row.getInt(row.fieldIndex("date_key")));
        assertEquals(3, row.getInt(row.fieldIndex("quantity")));
        assertEquals(new BigDecimal("4498.50"), row.getDecimal(row.fieldIndex("gross_amount")));   // 3 * 1499.50
        assertEquals(new BigDecimal("4348.50"), row.getDecimal(row.fieldIndex("net_amount")));     // - 150.00
    }
}
