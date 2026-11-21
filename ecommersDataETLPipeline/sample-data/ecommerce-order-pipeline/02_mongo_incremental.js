// =============================================================================
//  ecommerce-order-pipeline  |  02_mongo_incremental.js  |  changes AFTER the full load: UPDATE, INSERT, DELETE
//
//  Run (after 01 + the FULL load):
//      mongosh "mongodb://localhost:27017" sample-data/ecommerce-order-pipeline/02_mongo_incremental.js
//      POST http://localhost:8080/api/etl/pipelines/ecommerce-order-pipeline/run?loadType=INCREMENTAL
//
//  INCREMENTAL reads processedFlag = 'N' (or missing). A changed document gets 'N' again (this script does it).
//  DELETE is not pushed to PostgreSQL (the job only reads existing documents). We delete records that
//  were never loaded (rejected ones); their rejected-table row stays as history.
//
//  EXPECTED INCREMENTAL RUN:
//      ecom_categories   read 1 (CAT-07 new)
//      ecom_products     read 2 (PRD-0001 price change, PRD-0011 new in CAT-07)
//      ecom_customers    read 3 (ECU-0002 moved, ECU-0008 fixed -> resolved, ECU-0009 new)
//      ecom_orders       read 2 (ORD-00002 SHIPPED -> DELIVERED, ORD-00011 new)
//      ecom_order_items  read 3 (OIT-000016/17 new, OIT-000018 VALIDATION: discount > quantity * unitPrice)
// =============================================================================
const DB_NAME = "shopdb";
db = db.getSiblingDB(DB_NAME);

const NOW = "2026-11-20T10:00:00";

function update(coll, filter, changes) {
  const r = db.getCollection(coll).updateOne(filter,
      { $set: Object.assign({}, changes, { updatedAt: changes.updatedAt || NOW, processedFlag: "N" }) });
  print(`  UPDATE ${coll} ${JSON.stringify(filter)} matched=${r.matchedCount}`);
}
function insert(coll, filter, doc) {
  db.getCollection(coll).updateOne(filter, { $set: Object.assign({}, doc, { processedFlag: "N" }) }, { upsert: true });
  print(`  INSERT ${coll} ${JSON.stringify(filter)}`);
}
function remove(coll, filter) {
  const r = db.getCollection(coll).deleteMany(filter);
  print(`  DELETE ${coll} ${JSON.stringify(filter)} deleted=${r.deletedCount}`);
}

print("\n--- ecommerce-order-pipeline incremental changes ---");

// ---------------- categories ----------------
insert("ecom_categories", { categoryId: "CAT-07" },
       { categoryId: "CAT-07", categoryName: "Smart Watches", department: "ELECTRONICS", updatedAt: "2026-11-20T08:00:00" });
remove("ecom_categories", { categoryId: "CAT-06" });                       // rejected TOYS category removed

// ---------------- products ----------------
update("ecom_products", { productId: "PRD-0001" }, { listPrice: 69999, updatedAt: "2026-11-20T08:05:00" });   // price drop
insert("ecom_products", { productId: "PRD-0011" }, {
  productId: "PRD-0011", sku: "SAM-0011", productName: "Galaxy Watch 6", categoryId: "CAT-07", brand: "Samsung",
  listPrice: 24999, costPrice: 16000, status: "ACTIVE", launchDate: "2024-08-10", updatedAt: "2026-11-20T08:06:00" });
remove("ecom_products", { productId: "PRD-0009" });                        // its rejected (ORPHAN) product

// ---------------- customers ----------------
update("ecom_customers", { customerId: "ECU-0002" }, { city: "Pune", pincode: "411001", updatedAt: "2026-11-20T08:10:00" });
update("ecom_customers", { customerId: "ECU-0008" }, { segment: "VIP", updatedAt: "2026-11-20T08:11:00" });     // fix -> resolved
insert("ecom_customers", { customerId: "ECU-0009" }, {
  customerId: "ECU-0009", firstName: "Nisha", lastName: "Kapoor", email: "nisha.kapoor@gmail.com", phone: "9811223344",
  city: "Chandigarh", state: "Punjab", pincode: "160017", segment: "PRIME", signupDate: "2026-11-19",
  updatedAt: "2026-11-20T08:12:00" });

// ---------------- orders ----------------
update("ecom_orders", { orderId: "ORD-00002" }, { orderStatus: "DELIVERED", updatedAt: "2026-11-20T08:20:00" });
insert("ecom_orders", { orderId: "ORD-00011" }, {
  orderId: "ORD-00011", customerId: "ECU-0009", orderDate: "2026-11-20T08:30:00", orderStatus: "PLACED",
  paymentMode: "UPI", channel: "ANDROID", couponCode: "FIRST50", shippingFee: 0, shippingCity: "Chandigarh",
  shippingState: "Punjab", updatedAt: "2026-11-20T08:30:00" });
remove("ecom_orders", { orderId: "ORD-00010" });                           // corrupt order removed at source

// ---------------- order items ----------------
const item = (id, product, qty, price, discount) => ({ orderItemId: id, orderId: "ORD-00011", productId: product,
  quantity: qty, unitPrice: price, discountAmount: discount, updatedAt: "2026-11-20T08:31:00" });
insert("ecom_order_items", { orderItemId: "OIT-000016" }, item("OIT-000016", "PRD-0011", 1, 24999, 2500));
insert("ecom_order_items", { orderItemId: "OIT-000017" }, item("OIT-000017", "PRD-0001", 1, 69999, 0));
insert("ecom_order_items", { orderItemId: "OIT-000018" }, item("OIT-000018", "PRD-0006", 1, 299, 500));   // VALIDATION
remove("ecom_order_items", { orderItemId: "OIT-000014" });                 // line of the removed order

print("");
["ecom_categories", "ecom_products", "ecom_customers", "ecom_orders", "ecom_order_items"].forEach(c => print(
  `✔ ${c.padEnd(17)} to read: ${db.getCollection(c).countDocuments({ processedFlag: { $in: ["N", null] }, isTest: { $ne: true } })}`));
print("  expected: categories 1, products 2, customers 3, orders 2, order_items 3");
print("  Next: POST /api/etl/pipelines/ecommerce-order-pipeline/run?loadType=INCREMENTAL");
