package com.demo.etl.pipelines.ecommerce.fulfillment;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.lit;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Plain unit test for the shipment transform: role-playing date keys and the delivery measures.
 */
class FulfillmentStarSchemaMapperTest {

    private static SparkSession spark;

    @BeforeAll
    static void startSpark() {
        spark = SparkSession.builder()
                .master("local[1]")
                .appName("fulfillment-mapper-test")
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
    void computesDeliveryDaysOnTimeFlagAndBothDateKeys() {
        StructType schema = new StructType()
                .add("shipmentId", DataTypes.StringType)
                .add("orderId", DataTypes.StringType)
                .add("warehouseId", DataTypes.StringType)
                .add("carrierId", DataTypes.StringType)
                .add("trackingNumber", DataTypes.StringType)
                .add("shipmentStatus", DataTypes.StringType)
                .add("shippingCost", DataTypes.StringType)
                .add("weightKg", DataTypes.StringType)
                .add("promisedDate", DataTypes.StringType)
                .add("shippedDate", DataTypes.StringType)
                .add("deliveredDate", DataTypes.StringType);
        Dataset<Row> valid = spark.createDataFrame(List.of(
                RowFactory.create("SHP-00001", "ORD-1", "WH-01", "CRR-01", "T1", "DELIVERED", "80", "1.5",
                        "2026-06-05", "2026-06-01T10:00:00", "2026-06-04T15:00:00"),   // 3 days, on time
                RowFactory.create("SHP-00002", "ORD-1", "WH-01", "CRR-01", "T2", "DELIVERED", "80", "1.5",
                        "2026-06-05", "2026-06-01T10:00:00", "2026-06-08T09:00:00"),   // 7 days, late
                RowFactory.create("SHP-00003", "ORD-1", "WH-01", "CRR-01", "T3", "IN_TRANSIT", "80", "1.5",
                        "2026-06-05", "2026-06-01T10:00:00", null)                    // not delivered yet
        ), schema);

        Dataset<Row> typed = FulfillmentStarSchemaMapper.toTypedShipments(valid);
        Dataset<Row> withKeys = typed
                .withColumn("customer_key", lit(1L))
                .withColumn("warehouse_key", lit(2L))
                .withColumn("carrier_key", lit(3L));
        Map<String, Row> facts = FulfillmentStarSchemaMapper.toFactShipment(withKeys).collectAsList().stream()
                .collect(Collectors.toMap(row -> row.getString(row.fieldIndex("shipment_id")), row -> row));

        Row onTime = facts.get("SHP-00001");
        assertEquals(20260601, onTime.getInt(onTime.fieldIndex("shipped_date_key")));
        assertEquals(20260604, onTime.getInt(onTime.fieldIndex("delivered_date_key")));
        assertEquals(3, onTime.getInt(onTime.fieldIndex("delivery_days")));
        assertTrue(onTime.getBoolean(onTime.fieldIndex("is_on_time")));

        Row late = facts.get("SHP-00002");
        assertEquals(7, late.getInt(late.fieldIndex("delivery_days")));
        assertFalse(late.getBoolean(late.fieldIndex("is_on_time")));

        Row inTransit = facts.get("SHP-00003");
        assertNull(inTransit.get(inTransit.fieldIndex("delivered_date_key")));
        assertNull(inTransit.get(inTransit.fieldIndex("delivery_days")));
        assertNull(inTransit.get(inTransit.fieldIndex("is_on_time")));

        // shipped + delivered dates go to dim_date together (nulls are filtered by DateDimension)
        assertEquals(6, FulfillmentStarSchemaMapper.allShipmentDates(typed).count());
    }
}
