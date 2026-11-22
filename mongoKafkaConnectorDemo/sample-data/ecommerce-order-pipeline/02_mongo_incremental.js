// =============================================================================
//  ecommerce-order-pipeline  |  02_mongo_incremental.js  |  changes AFTER the first run: UPDATE, INSERT, DELETE
//
//  Run (after 01, when the subscriber LAG = 0):
//      mongosh "mongodb://localhost:27017/?replicaSet=rs0" sample-data/ecommerce-order-pipeline/02_mongo_incremental.js
//      mongosh "mongodb://localhost:27017/?replicaSet=rs0" sample-data/99_verify_bronze.js
//
//  sourceMongo_db is the TRANSACTION database: a removed record is HARD deleted (the document is gone).
//  The subscriber keeps it in bronze_db with its last values: _deleted: true, _op: "delete".
//  Every changed / new document still gets updatedAt = NOW, like a source application stamps its changes.
//
//  HARD DELETES in this script (expected in bronze_db with _deleted: true):
//      ecom_categories   CAT-06 (rejected TOYS category)
//      ecom_products     PRD-0009 (rejected ORPHAN product)
//      ecom_orders       ORD-00010 (rejected CORRUPT order), ORD-00004 (LOADED order, cancelled)
//      ecom_order_items  OIT-000014 (line of ORD-00010), OIT-000005 + OIT-000006 (the 2 lines of ORD-00004)
//      ORD-00004 and its 2 lines are deleted in ONE multi-document TRANSACTION (all or nothing), like an
//      order service does it. The change stream sends the 3 delete events after the commit.
//
//  EXPECTED after this script (fresh start: 01 ran once):
//                         source   bronze total = live + _deleted
//      ecom_categories       6           7      =   6  +  1
//      ecom_products        11          12      =  11  +  1     (PRD-0001 listPrice 69999)
//      ecom_customers        9           9      =   9  +  0     (ECU-0002, ECU-0008 _op "update")
//      ecom_orders          10          12      =  10  +  2     (ORD-00002 DELIVERED)
//      ecom_order_items     15          18      =  15  +  3
// =============================================================================
const DB_NAME = "sourceMongo_db";   // TRANSACTION (source) database, captured by the CDC connector
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
/** HARD delete at the source (document is gone). Bronze keeps it with _deleted: true. */
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
update("ecom_customers", { customerId: "ECU-0008" }, { segment: "VIP" });     // fix -> loaded
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

// HARD DELETE of a LOADED order + its lines, in ONE transaction (order ORD-00004 was cancelled at the source).
// bronze_db: ORD-00004, OIT-000005 and OIT-000006 stay with their last values and _deleted: true.
// Transactions need a replica set (rs0); the change stream delivers the events only after the commit.
const session = db.getMongo().startSession();
const txDb = session.getDatabase(DB_NAME);
session.startTransaction();
try {
  const lines = txDb.ecom_order_items.deleteMany({ orderId: "ORD-00004" });
  const order = txDb.ecom_orders.deleteMany({ orderId: "ORD-00004" });
  session.commitTransaction();
  print(`  DELETE (transaction) ecom_orders ORD-00004 deleted=${order.deletedCount}, ` +
        `ecom_order_items of ORD-00004 deleted=${lines.deletedCount}`);
} catch (e) {
  session.abortTransaction();
  throw e;
} finally {
  session.endSession();
}

print("");
["ecom_categories", "ecom_products", "ecom_customers", "ecom_orders", "ecom_order_items"].forEach(c => print(
  `✔ ${DB_NAME}.${c.padEnd(17)} ${db.getCollection(c).countDocuments()} documents`));
print("  expected source: categories 6, products 11, customers 9, orders 10, order_items 15");
print("  expected bronze_db _deleted: categories 1, products 1, orders 2, order_items 3");
print("  Next: wait until the subscriber LAG = 0, then run sample-data/99_verify_bronze.js");
