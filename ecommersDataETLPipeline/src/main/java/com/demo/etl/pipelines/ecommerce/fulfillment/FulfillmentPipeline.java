package com.demo.etl.pipelines.ecommerce.fulfillment;

import com.demo.etl.framework.pipeline.AbstractEtlPipeline;
import com.demo.etl.framework.pipeline.PipelineContext;
import com.demo.etl.framework.pipeline.PipelineToolkit;
import com.demo.etl.framework.source.SourceBatch;
import com.demo.etl.framework.source.SourceDefinition;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.stereotype.Component;

import java.util.List;

import static com.demo.etl.pipelines.ecommerce.fulfillment.FulfillmentStarSchemaMapper.ANY_DATE_COLUMN;
import static com.demo.etl.pipelines.ecommerce.fulfillment.FulfillmentStarSchemaMapper.allShipmentDates;
import static com.demo.etl.pipelines.ecommerce.fulfillment.FulfillmentStarSchemaMapper.toDimCarrier;
import static com.demo.etl.pipelines.ecommerce.fulfillment.FulfillmentStarSchemaMapper.toDimWarehouse;
import static com.demo.etl.pipelines.ecommerce.fulfillment.FulfillmentStarSchemaMapper.toFactShipment;
import static com.demo.etl.pipelines.ecommerce.fulfillment.FulfillmentStarSchemaMapper.toTypedShipments;
import static com.demo.etl.pipelines.ecommerce.fulfillment.FulfillmentStarSchemaMapper.withOrderCustomerKey;

/**
 * EXAMPLE 4 - THREE collections -> STAR schema (e-commerce delivery), DEPENDS ON ecommerce-order-pipeline.
 *
 *   MongoDB: ecom_warehouses ---> dim_warehouse
 *            ecom_carriers   ---> dim_carrier
 *            ecom_shipments  ---> fact_shipment  (+ dim_date for shipped AND delivered date)
 *                                 every shipment must belong to an order in silver.fact_order,
 *                                 its customer_key is taken from that order (conformed dim_ecom_customer)
 *
 * Run ecommerce-order-pipeline FIRST. If you do not, shipments are rejected as ORPHAN
 * ("orderId not found in fact_order"); run the order pipeline, then this one with loadType=RETRY_REJECTED.
 */
@Component
public class FulfillmentPipeline extends AbstractEtlPipeline {

    public FulfillmentPipeline(PipelineToolkit toolkit) {
        super(toolkit);
    }

    @Override
    public String name() {
        return "ecommerce-fulfillment-pipeline";
    }

    @Override
    public String description() {
        return "Star example (depends on ecommerce-order-pipeline): ecom_warehouses + ecom_carriers + ecom_shipments -> "
                + "dim_warehouse, dim_carrier, dim_date, dim_ecom_customer (shared), fact_shipment";
    }

    @Override
    public List<SourceDefinition> sources() {
        return List.of(FulfillmentSources.WAREHOUSES, FulfillmentSources.CARRIERS, FulfillmentSources.SHIPMENTS);
    }

    @Override
    public List<String> targetTables() {
        return List.of("silver.dim_warehouse", "silver.dim_carrier", "silver.dim_date", "silver.fact_shipment");
    }

    @Override
    public List<String> fullLoadTruncateTables() {
        return List.of("silver.fact_shipment");
    }

    @Override
    public void run(PipelineContext context) {
        // ---------------- 1. DIMENSIONS ----------------
        SourceBatch warehouses = extractAndValidate(context, FulfillmentSources.WAREHOUSES,
                FulfillmentSources.warehouseSanitizer(), FulfillmentSources.warehouseRules());
        load(context, toDimWarehouse(warehouses.valid()), "silver.dim_warehouse", "warehouse_id");

        SourceBatch carriers = extractAndValidate(context, FulfillmentSources.CARRIERS,
                FulfillmentSources.carrierSanitizer(), FulfillmentSources.carrierRules());
        load(context, toDimCarrier(carriers.valid()), "silver.dim_carrier", "carrier_id");

        // ---------------- 2. FACT (ORPHAN checks, one of them against ANOTHER pipeline's table) ----------------
        Dataset<Row> orderIds = readTable("silver.fact_order", "order_id");          // from ecommerce-order-pipeline
        Dataset<Row> warehouseIds = readTable("silver.dim_warehouse", "warehouse_id");
        Dataset<Row> carrierIds = readTable("silver.dim_carrier", "carrier_id");

        SourceBatch shipments = extractAndValidate(context, FulfillmentSources.SHIPMENTS,
                FulfillmentSources.shipmentSanitizer(),
                FulfillmentSources.shipmentRules(orderIds, warehouseIds, carrierIds));

        Dataset<Row> typedShipments = toTypedShipments(shipments.valid());
        loadDateDimension(context, allShipmentDates(typedShipments), ANY_DATE_COLUMN);

        Dataset<Row> withKeys = addSurrogateKey(typedShipments, "silver.dim_warehouse", "warehouse_key", "warehouse_id");
        withKeys = addSurrogateKey(withKeys, "silver.dim_carrier", "carrier_key", "carrier_id");
        withKeys = withOrderCustomerKey(withKeys, readTable("silver.fact_order", "order_id", "customer_key"));
        load(context, toFactShipment(withKeys), "silver.fact_shipment", "shipment_id");
    }
}
