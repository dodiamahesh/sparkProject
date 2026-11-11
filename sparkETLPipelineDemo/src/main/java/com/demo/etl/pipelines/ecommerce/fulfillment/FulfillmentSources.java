package com.demo.etl.pipelines.ecommerce.fulfillment;

import com.demo.etl.framework.common.SparkTypes;
import com.demo.etl.framework.quality.DataQualityRules;
import com.demo.etl.framework.quality.DataSanitizer;
import com.demo.etl.framework.quality.ErrorType;
import com.demo.etl.framework.source.SourceDefinition;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.col;

/**
 * WHAT we read from MongoDB and HOW we clean + validate it, for the 3 collections of the fulfillment pipeline.
 */
final class FulfillmentSources {

    private FulfillmentSources() {
    }

    // =================================================================== ecom_warehouses
    static final SourceDefinition WAREHOUSES = SourceDefinition.builder("ecom_warehouses")
            .fields("warehouseId", "warehouseName", "city", "state", "pincode", "capacityUnits", "status", "updatedAt")
            .businessKey("warehouseId")
            .latestRecordColumn("updatedAt")
            .excludeTestRecords()
            .maxRejectPercent(30)
            .expectedRows(0, 1_000)
            .description("Fulfilment centres and hubs that ship orders")
            .owner("logistics-team@demo.com")
            .sla("on change")
            .build();

    static DataSanitizer warehouseSanitizer() {
        return DataSanitizer.create()
                .trimAllAndBlankToNull()
                .toUpperCase("warehouseId", "status")
                .toProperCase("city", "state")
                .removeThousandSeparators("capacityUnits");
    }

    static DataQualityRules warehouseRules() {
        return DataQualityRules.create()
                .mustBeInteger("capacityUnits")
                .required("warehouseId", "warehouseName", "city", "status")
                .mustMatch("warehouseId", "^WH-[0-9]{2}$", "must look like WH-01")
                .mustMatch("pincode", "^[1-9][0-9]{5}$", "must be a 6 digit Indian pincode")
                .mustBePositive("capacityUnits")
                .mustBeOneOf("status", "ACTIVE", "INACTIVE")
                .warnings(w -> w
                        .mustBeOneOf("status", "ACTIVE"));
    }

    // =================================================================== ecom_carriers
    static final SourceDefinition CARRIERS = SourceDefinition.builder("ecom_carriers")
            .fields("carrierId", "carrierName", "serviceType", "contactEmail", "status", "updatedAt")
            .businessKey("carrierId")
            .latestRecordColumn("updatedAt")
            .excludeTestRecords()
            .maxRejectPercent(30)
            .expectedRows(0, 1_000)
            .description("Courier companies that deliver the parcels")
            .owner("logistics-team@demo.com")
            .sla("on change")
            .build();

    static DataSanitizer carrierSanitizer() {
        return DataSanitizer.create()
                .trimAllAndBlankToNull()
                .toUpperCase("carrierId", "serviceType", "status")
                .toLowerCase("contactEmail");
    }

    static DataQualityRules carrierRules() {
        return DataQualityRules.create()
                .required("carrierId", "carrierName", "serviceType", "status")
                .mustMatch("carrierId", "^CRR-[0-9]{2}$", "must look like CRR-01")
                .mustBeOneOf("serviceType", "STANDARD", "EXPRESS", "SAME_DAY")
                .mustBeValidEmail("contactEmail")
                .mustBeOneOf("status", "ACTIVE", "INACTIVE")
                .warnings(w -> w
                        .required("contactEmail"));
    }

    // =================================================================== ecom_shipments
    static final SourceDefinition SHIPMENTS = SourceDefinition.builder("ecom_shipments")
            .fields("shipmentId", "orderId", "warehouseId", "carrierId", "trackingNumber", "shippedDate",
                    "promisedDate", "deliveredDate", "shipmentStatus", "shippingCost", "weightKg", "updatedAt")
            .businessKey("shipmentId")
            .latestRecordColumn("updatedAt")
            .excludeTestRecords()
            .maxRejectPercent(30)
            .expectedRows(1, 500_000)
            .description("Parcel shipments with tracking status, one per shipped order")
            .owner("logistics-team@demo.com")
            .sla("every 30 minutes (status updates from the carriers)")
            .build();

    static DataSanitizer shipmentSanitizer() {
        return DataSanitizer.create()
                .trimAllAndBlankToNull()
                .toUpperCase("shipmentId", "orderId", "warehouseId", "carrierId", "trackingNumber", "shipmentStatus")
                .removeThousandSeparators("shippingCost", "weightKg");
    }

    /**
     * @param orderIds     every order_id in silver.fact_order -> filled by ecommerce-order-pipeline (DEPENDENCY)
     * @param warehouseIds every warehouse_id in silver.dim_warehouse
     * @param carrierIds   every carrier_id in silver.dim_carrier
     */
    static DataQualityRules shipmentRules(Dataset<Row> orderIds, Dataset<Row> warehouseIds, Dataset<Row> carrierIds) {
        return DataQualityRules.create()
                .mustBeTimestamp("shippedDate")
                .mustBeTimestamp("deliveredDate")
                .mustBeDate("promisedDate")
                .mustBeDecimal("shippingCost")
                .mustBeDecimal("weightKg")
                .required("shipmentId", "orderId", "warehouseId", "carrierId", "shippedDate", "promisedDate",
                        "shipmentStatus", "shippingCost")
                .mustMatch("shipmentId", "^SHP-[0-9]{5}$", "must look like SHP-00001")
                .mustNotBeInFuture("shippedDate")
                .mustNotBeInFuture("deliveredDate")
                .mustNotBeNegative("shippingCost")
                .mustBePositive("weightKg")
                .mustBeOneOf("shipmentStatus", "IN_TRANSIT", "DELIVERED", "RETURNED_TO_ORIGIN", "LOST")
                .custom(ErrorType.VALIDATION, "deliveredDate is required when shipmentStatus is DELIVERED",
                        col("shipmentStatus").equalTo("DELIVERED").and(col("deliveredDate").isNull()))
                .custom(ErrorType.VALIDATION, "deliveredDate cannot be before shippedDate",
                        col("deliveredDate").cast(SparkTypes.TIMESTAMP).lt(col("shippedDate").cast(SparkTypes.TIMESTAMP)))
                .mustExistIn("orderId", orderIds, "order_id", "fact_order")
                .mustExistIn("warehouseId", warehouseIds, "warehouse_id", "dim_warehouse")
                .mustExistIn("carrierId", carrierIds, "carrier_id", "dim_carrier")
                .warnings(w -> w
                        .mustNotBeMoreThan("weightKg", 10));
    }
}
