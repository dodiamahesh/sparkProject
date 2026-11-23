package com.tcs.dmj.dwh.pipeline.order;

import com.tcs.dmj.dwh.connector.source.SourceBatch;
import com.tcs.dmj.dwh.connector.source.SourceDefinition;
import com.tcs.dmj.dwh.pipeline.AbstractEtlPipeline;
import com.tcs.dmj.dwh.pipeline.PipelineContext;
import com.tcs.dmj.dwh.pipeline.PipelineToolkit;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.stereotype.Component;

import java.util.List;

import static com.tcs.dmj.dwh.pipeline.order.EcommerceOrderSchemaMapper.toDimCategory;
import static com.tcs.dmj.dwh.pipeline.order.EcommerceOrderSchemaMapper.toDimCustomer;
import static com.tcs.dmj.dwh.pipeline.order.EcommerceOrderSchemaMapper.toDimProduct;
import static com.tcs.dmj.dwh.pipeline.order.EcommerceOrderSchemaMapper.toFactOrder;
import static com.tcs.dmj.dwh.pipeline.order.EcommerceOrderSchemaMapper.toFactOrderItem;
import static com.tcs.dmj.dwh.pipeline.order.EcommerceOrderSchemaMapper.toTypedOrderItems;
import static com.tcs.dmj.dwh.pipeline.order.EcommerceOrderSchemaMapper.toTypedOrders;
import static com.tcs.dmj.dwh.pipeline.order.EcommerceOrderSchemaMapper.toTypedProducts;
import static com.tcs.dmj.dwh.pipeline.order.EcommerceOrderSchemaMapper.withOrderHeaderKeys;

/**
 * EXAMPLE 3 - FIVE collections -> SNOWFLAKE schema (e-commerce sales).
 *
 *   MongoDB: ecom_categories  ---> dim_category
 *            ecom_products    ---> dim_product      (product.categoryId must exist in dim_category,
 *                                                     dim_product stores category_key = SNOWFLAKE)
 *            ecom_customers   ---> dim_ecom_customer
 *            ecom_orders      ---> fact_order       (one row per order, customer must exist)  + dim_date
 *            ecom_order_items ---> fact_order_item  (one row per order line, order + product must exist)
 *
 * Order matters: parent tables first (category -> product, customer -> order -> order line).
 */
@Component
public class EcommerceOrderPipeline extends AbstractEtlPipeline {

    public EcommerceOrderPipeline(PipelineToolkit toolkit) {
        super(toolkit);
    }

    @Override
    public String name() {
        return "ecommerce-order-pipeline";
    }

    @Override
    public String description() {
        return "Order snowflake: categories, products, customers, orders, order items -> fact_order, fact_order_item";
    }

    @Override
    public List<SourceDefinition> sources() {
        return List.of(EcommerceOrderSources.CATEGORIES, EcommerceOrderSources.PRODUCTS,
                EcommerceOrderSources.CUSTOMERS, EcommerceOrderSources.ORDERS, EcommerceOrderSources.ORDER_ITEMS);
    }

    @Override
    public List<String> targetTables() {
        return List.of("silver.dim_category", "silver.dim_product", "silver.dim_ecom_customer",
                "silver.dim_date", "silver.fact_order", "silver.fact_order_item");
    }

    @Override
    public void run(PipelineContext context) {
        // ---------------- 1. SNOWFLAKE branch: category -> product ----------------
        SourceBatch categories = extractAndValidate(context, EcommerceOrderSources.CATEGORIES,
                EcommerceOrderSources.categorySanitizer(), EcommerceOrderSources.categoryRules());
        load(context, toDimCategory(categories.valid()), "silver.dim_category", "category_id");

        Dataset<Row> categoryIds = readTable("silver.dim_category", "category_id");
        SourceBatch products = extractAndValidate(context, EcommerceOrderSources.PRODUCTS,
                EcommerceOrderSources.productSanitizer(), EcommerceOrderSources.productRules(categoryIds));

        // The dimension gets a key of ANOTHER dimension: this is what makes it a snowflake
        Dataset<Row> productsWithCategoryKey = addSurrogateKey(toTypedProducts(products.valid()),
                "silver.dim_category", "category_key", "category_id");
        load(context, toDimProduct(productsWithCategoryKey), "silver.dim_product", "product_id");

        // ---------------- 2. customer dimension ----------------
        SourceBatch customers = extractAndValidate(context, EcommerceOrderSources.CUSTOMERS,
                EcommerceOrderSources.customerSanitizer(), EcommerceOrderSources.customerRules());
        load(context, toDimCustomer(customers.valid()), "silver.dim_ecom_customer", "customer_id");

        // ---------------- 3. FACT 1: order header (grain = one order) ----------------
        Dataset<Row> customerIds = readTable("silver.dim_ecom_customer", "customer_id");
        SourceBatch orders = extractAndValidate(context, EcommerceOrderSources.ORDERS,
                EcommerceOrderSources.orderSanitizer(), EcommerceOrderSources.orderRules(customerIds));

        Dataset<Row> typedOrders = toTypedOrders(orders.valid());
        loadDateDimension(context, typedOrders, "order_ts");
        Dataset<Row> ordersWithKeys = addSurrogateKey(typedOrders, "silver.dim_ecom_customer", "customer_key", "customer_id");
        load(context, toFactOrder(ordersWithKeys), "silver.fact_order", "order_id");

        // ---------------- 4. FACT 2: order lines (grain = one product in one order) ----------------
        Dataset<Row> orderIds = readTable("silver.fact_order", "order_id");
        Dataset<Row> productIds = readTable("silver.dim_product", "product_id");
        SourceBatch items = extractAndValidate(context, EcommerceOrderSources.ORDER_ITEMS,
                EcommerceOrderSources.orderItemSanitizer(), EcommerceOrderSources.orderItemRules(orderIds, productIds));

        Dataset<Row> itemsWithKeys = addSurrogateKey(toTypedOrderItems(items.valid()),
                "silver.dim_product", "product_key", "product_id");
        itemsWithKeys = withOrderHeaderKeys(itemsWithKeys,
                readTable("silver.fact_order", "order_id", "customer_key", "date_key"));
        load(context, toFactOrderItem(itemsWithKeys), "silver.fact_order_item", "order_item_id");

        // ---------------- 5. SOFT DELETES (rows are marked, never removed) ----------------
        applyDeletes(context, categories, "silver.dim_category", "category_id");
        applyDeletes(context, products, "silver.dim_product", "product_id");
        applyDeletes(context, customers, "silver.dim_ecom_customer", "customer_id");
        applyDeletes(context, orders, "silver.fact_order", "order_id");
        applyDeletes(context, orders, "silver.fact_order_item", "order_id");          // cascade: lines of a deleted order
        applyDeletes(context, items, "silver.fact_order_item", "order_item_id");
    }
}
