package com.demo.etl.pipelines.ecommerce.fulfillment;

import com.demo.etl.framework.common.SparkTypes;
import com.demo.etl.framework.target.DateDimension;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.datediff;
import static org.apache.spark.sql.functions.to_date;

/**
 * TRANSFORM step: MongoDB documents -> star-schema rows (snake_case, real types, derived measures).
 */
final class FulfillmentStarSchemaMapper {

    /** Column name used to collect every date of a shipment for silver.dim_date. */
    static final String ANY_DATE_COLUMN = "any_date";

    private FulfillmentStarSchemaMapper() {
    }

    static Dataset<Row> toDimWarehouse(Dataset<Row> validWarehouses) {
        return validWarehouses.select(
                col("warehouseId").as("warehouse_id"),
                col("warehouseName").as("warehouse_name"),
                col("city"),
                col("state"),
                col("pincode"),
                col("capacityUnits").cast(SparkTypes.INT).as("capacity_units"),
                col("status").as("warehouse_status"));
    }

    static Dataset<Row> toDimCarrier(Dataset<Row> validCarriers) {
        return validCarriers.select(
                col("carrierId").as("carrier_id"),
                col("carrierName").as("carrier_name"),
                col("serviceType").as("service_type"),
                col("contactEmail").as("contact_email"),
                col("status").as("carrier_status"));
    }

    static Dataset<Row> toTypedShipments(Dataset<Row> validShipments) {
        return validShipments.select(
                col("shipmentId").as("shipment_id"),
                col("orderId").as("order_id"),
                col("warehouseId").as("warehouse_id"),
                col("carrierId").as("carrier_id"),
                col("trackingNumber").as("tracking_number"),
                col("shipmentStatus").as("shipment_status"),
                col("shippingCost").cast(SparkTypes.MONEY).as("shipping_cost"),
                col("weightKg").cast(SparkTypes.RATE).as("weight_kg"),
                col("promisedDate").cast(SparkTypes.DATE).as("promised_date"),
                col("shippedDate").cast(SparkTypes.TIMESTAMP).as("shipped_ts"),
                col("deliveredDate").cast(SparkTypes.TIMESTAMP).as("delivered_ts"));
    }

    /** Shipped AND delivered dates in one column, so dim_date is loaded in one step for both roles. */
    static Dataset<Row> allShipmentDates(Dataset<Row> typedShipments) {
        return typedShipments.select(col("shipped_ts").as(ANY_DATE_COLUMN))
                .union(typedShipments.select(col("delivered_ts").as(ANY_DATE_COLUMN)));
    }

    /**
     * A shipment document has no customer: it is found through the order.
     * {@code orders} = silver.fact_order (order_id, customer_key), loaded by ecommerce-order-pipeline.
     */
    static Dataset<Row> withOrderCustomerKey(Dataset<Row> shipments, Dataset<Row> orders) {
        return shipments
                .join(orders, shipments.col("order_id").equalTo(orders.col("order_id")), "inner")
                .drop(orders.col("order_id"));
    }

    /** Expects customer_key, warehouse_key and carrier_key to be added already. */
    static Dataset<Row> toFactShipment(Dataset<Row> shipmentsWithKeys) {
        Column deliveredDate = to_date(col("delivered_ts"));
        return shipmentsWithKeys.select(
                col("shipment_id"),
                col("order_id"),
                col("customer_key"),
                col("warehouse_key"),
                col("carrier_key"),
                // ROLE-PLAYING dimension: both keys point to the same silver.dim_date table
                DateDimension.dateKey(col("shipped_ts")).as("shipped_date_key"),
                DateDimension.dateKey(col("delivered_ts")).as("delivered_date_key"),
                col("tracking_number"),
                col("shipment_status"),
                col("shipping_cost"),
                col("weight_kg"),
                col("promised_date"),
                col("shipped_ts"),
                col("delivered_ts"),
                // derived measures (NULL while the parcel is not delivered)
                datediff(deliveredDate, to_date(col("shipped_ts"))).as("delivery_days"),
                deliveredDate.leq(col("promised_date")).as("is_on_time"));
    }
}
