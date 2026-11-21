// =============================================================================
//  ecommerce-order-pipeline  |  02_mongo_incremental.js  |  changes AFTER the first run: UPDATE, INSERT, DELETE
//
//  Run (after 01 + the first run):
//      mongosh "mongodb://localhost:27017" sample-data/ecommerce-order-pipeline/02_mongo_incremental.js
//      POST http://localhost:8080/api/etl/pipelines/ecommerce-order-pipeline/run
//
//  INCREMENTAL by timestamp: the next run reads documents with updatedAt > last watermark (= start of the
//  previous successful run). Every changed / new document below gets updatedAt = NOW (the moment this
//  script runs), exactly like a source application stamps its changes. MongoDB is never written by the ETL.
//  DELETE is not pushed to PostgreSQL (the job only reads existing documents). We delete records that
//  were never loaded (rejected ones); their rejected-table row stays as history.
//
//  EXPECTED INCREMENTAL RUN:
//      ecom_categories   read 1 (CAT-07 new)
//      ecom_products     read 2 (PRD-0001 price change, PRD-0011 new in CAT-07)
//      ecom_customers    read 3 (ECU-0002 moved, ECU-0008 fixed -> resolved, ECU-0009 new)
//      ecom_orders       read 2 (ORD-00002 SHIPPED -> DELIVERED, ORD-00011 new)
//      ecom_order_items  read 3 (OIT-000016/17 new, OIT-000018 VALIDATION: discount > quantity * unitPrice)
//      status: SUCCESS_WITH_FAILED (OIT-000018 rejected)
// =============================================================================
const DB_NAME = "shopdb";
db = db.getSiblingDB(DB_NAME);

const NOW = new Date();   // every change is stamped with the current time -> newer than the last watermark

function update(coll, filter, changes) {
  const r = db.getCollection(coll).updateOne(filter,
      { $set: Object.assign({}, changes, { updatedAt: NOW }) });
  print(`  UPDATE ${coll} ${JSON.stringify(filter)} matched=${r.matchedCount}`);
}
function insert(coll, filter, doc) {
  db.getCollection(coll).updateOne(filter, { $set: Object.assign({}, doc, { updatedAt: NOW }) }, { upsert: true });
  print(`  INSERT ${coll} ${JSON.stringify(filter)}`);
}
function remove(coll, filter) {
  const r = db.getCollection(coll).deleteMany(filter);
  print(`  DELETE ${coll} ${JSON.stringify(filter)} deleted=${r.deletedCount}`);
}

print("\n--- ecommerce-order-pipeline incremental changes ---");

// ---------------- categories ----------------
insert("ecom_categories", { categoryId: "CAT-07" },
       { categoryId: "CAT-07", categoryName: "Smart Watches", department: "ELECTRONICS" });
remove("ecom_categories", { categoryId: "CAT-06" });                       // rejected TOYS category removed

// ---------------- products ----------------
update("ecom_products", { productId: "PRD-0001" }, { listPrice: 69999 });   // price drop
insert("ecom_products", { productId: "PRD-0011" }, {
  productId: "PRD-0011", sku: "SAM-0011", productName: "Galaxy Watch 6", categoryId: "CAT-07", brand: "Samsung",
  listPrice: 24999, costPrice: 16000, status: "ACTIVE", launchDate: "2024-08-10" });
remove("ecom_products", { productId: "PRD-0009" });                        // its rejected (ORPHAN) product

// ---------------- customers ----------------
update("ecom_customers", { customerId: "ECU-0002" }, { city: "Pune", pincode: "411001" });
update("ecom_customers", { customerId: "ECU-0008" }, { segment: "VIP" });     // fix -> resolved
insert("ecom_customers", { customerId: "ECU-0009" }, {
  customerId: "ECU-0009", firstName: "Nisha", lastName: "Kapoor", email: "nisha.kapoor@gmail.com", phone: "9811223344",
  city: "Chandigarh", state: "Punjab", pincode: "160017", segment: "PRIME", signupDate: "2026-11-19" });

// ---------------- orders ----------------
update("ecom_orders", { orderId: "ORD-00002" }, { orderStatus: "DELIVERED" });
insert("ecom_orders", { orderId: "ORD-00011" }, {
  orderId: "ORD-00011", customerId: "ECU-0009", orderDate: "2026-11-20T08:30:00", orderStatus: "PLACED",
  paymentMode: "UPI", channel: "ANDROID", couponCode: "FIRST50", shippingFee: 0, shippingCity: "Chandigarh",
  shippingState: "Punjab" });
remove("ecom_orders", { orderId: "ORD-00010" });                           // corrupt order removed at source

// ---------------- order items ----------------
const item = (id, product, qty, price, discount) => ({ orderItemId: id, orderId: "ORD-00011", productId: product,
  quantity: qty, unitPrice: price, discountAmount: discount });
insert("ecom_order_items", { orderItemId: "OIT-000016" }, item("OIT-000016", "PRD-0011", 1, 24999, 2500));
insert("ecom_order_items", { orderItemId: "OIT-000017" }, item("OIT-000017", "PRD-0001", 1, 69999, 0));
insert("ecom_order_items", { orderItemId: "OIT-000018" }, item("OIT-000018", "PRD-0006", 1, 299, 500));   // VALIDATION
remove("ecom_order_items", { orderItemId: "OIT-000014" });                 // line of the removed order

print("");
["ecom_categories", "ecom_products", "ecom_customers", "ecom_orders", "ecom_order_items"].forEach(c => print(
  `✔ ${c.padEnd(17)} to read: ${db.getCollection(c).countDocuments({ updatedAt: { $gte: NOW }, isTest: { $ne: true } })}`));
print("  expected: categories 1, products 2, customers 3, orders 2, order_items 3");
print("  Next: POST /api/etl/pipelines/ecommerce-order-pipeline/run");
