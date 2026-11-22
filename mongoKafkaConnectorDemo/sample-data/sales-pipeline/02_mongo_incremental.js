// =============================================================================
//  sales-pipeline (Job-2, J2_SALES)  |  02_mongo_incremental.js  |  changes AFTER the first run: UPDATE, INSERT, DELETE
//
//  Run (after 01, when the subscriber LAG = 0):
//      mongosh "mongodb://localhost:27017/?replicaSet=rs0" sample-data/sales-pipeline/02_mongo_incremental.js
//      mongosh "mongodb://localhost:27017/?replicaSet=rs0" sample-data/99_verify_bronze.js
//
//  Changes between 2026-10-21 00:00 and 04:00 IST (design document, incremental run) taken from
//  "../sparkJavaMongoConnectorAirflowLinageTheory/sample_data/02_mongo_incremental_0400_data.js".
//  Difference: every changed / new document gets updated_at = NOW (the moment this script runs).
//
//  sourceMongo_db is the TRANSACTION database: a removed record is HARD deleted (the document is gone).
//  The subscriber keeps it in bronze_db with its last values: _deleted: true, _op: "delete".
//
//  HARD DELETES in this script (expected in bronze_db with _deleted: true):
//      customers  C020  (LOADED customer, closed her account)
//      orders     O1023 (rejected: no items), O1019 (LOADED order with 2 items, cancelled; payment PAY19 stays)
//      payments   PAY22 (rejected: corrupt amount)
//
//  EXPECTED after this script (fresh start: 01 ran once):
//                    source   bronze total = live + _deleted
//      products        25          25      =  25  +  0     (P003 latest version price 1800, P024 new)
//      customers       23          24      =  23  +  1     (C003 Pune/Platinum, C002 phone, C023 new)
//      orders          23          25      =  23  +  2     (O1024 new, O1015 DELIVERED, O1010 SHIPPED)
//      payments        23          24      =  23  +  1     (PAY10 SUCCESS, PAY23 new)
// =============================================================================
const DB_NAME = "sourceMongo_db";   // TRANSACTION (source) database, captured by the CDC connector
db = db.getSiblingDB(DB_NAME);

const ist = (s) => new Date(s.replace(" ", "T") + ":00+05:30");
const NOW = new Date();   // every change is stamped with the current time -> newer than the last watermark

// Same business key twice (e.g. P003 duplicate)? Only the LATEST version is changed.
function updateLatest(coll, keyFilter, setFields, sortField = "updated_at", arrayFilters) {
  const latest = db.getCollection(coll).find(keyFilter).sort({ [sortField]: -1 }).limit(1).toArray()[0];
  if (!latest) { print(`✘ ${coll} ${JSON.stringify(keyFilter)} not found`); return; }
  const opts = arrayFilters ? { arrayFilters } : {};
  db.getCollection(coll).updateOne({ _id: latest._id }, { $set: Object.assign({}, setFields, { updated_at: NOW }) }, opts);
  print(`  UPDATE  ${coll.padEnd(10)} ${JSON.stringify(keyFilter)}`);
}

// New document: upsert on the business key (running the script twice creates no duplicates)
function insertNew(coll, keyFilter, doc) {
  db.getCollection(coll).updateOne(keyFilter, { $set: Object.assign({}, doc, { updated_at: NOW }) }, { upsert: true });
  print(`  INSERT  ${coll.padEnd(10)} ${JSON.stringify(keyFilter)}`);
}

// HARD delete at the source (document is gone). Bronze keeps it with _deleted: true.
function remove(coll, filter) {
  const r = db.getCollection(coll).deleteMany(filter);
  print(`  DELETE  ${coll.padEnd(10)} ${JSON.stringify(filter)} deleted=${r.deletedCount}`);
}

print("\n--- sales-pipeline changes 2026-10-21 00:00 -> 04:00 ---");

// =============================================================================
// products  → dim_product (this job)
// =============================================================================
// P003: price 2000 → 1800, WH-AHM stock 100 → 95   (01:10)
//   P003 na 2 docs che (10-05 vado duplicate). Latest (10-20) ne j update karvano.
updateLatest("products", { product_code: "P003" },
  { price: 1800, "stock.$[wh].qty": 95 },
  "updated_at", [{ "wh.warehouse": "WH-AHM" }]);

// P024: navo product (00:45)
insertNew("products", { product_code: "P024" }, {
  product_code: "P024",
  name: "Wireless Mouse",
  brand: { code: "B03", name: "TechBook" },
  category: { code: "C-ELE-LAP", name: "Laptops", parent: { code: "C-ELE", name: "Electronics" } },
  supplier: { code: "SUP-01", name: "Shree Electronics Pvt Ltd", city: "Ahmedabad" },
  price: 800,
  stock: [{ warehouse: "WH-AHM", qty: 50 }],
  is_active: true,
});

// =============================================================================
// customers  → this job
// =============================================================================
// C003: Mumbai → Pune, Gold → Platinum  (01:20)  → SCD2 (navi version)
updateLatest("customers", { customer_code: "C003" }, {
  "address.city": "Pune", "address.state": "Maharashtra", "address.pincode": "411001",
  tier: "Platinum"
});

// C002: phone badlayo  (02:15)  → SCD1 (overwrite)
updateLatest("customers", { customer_code: "C002" }, {
  phone: "9876511122"
});

// C023: navo customer  (00:30)
insertNew("customers", { customer_code: "C023" }, {
  customer_code: "C023", name: "Jay Parmar", email: "jay.parmar@mail.com", phone: "9876500023",
  address: { city: "Gandhinagar", state: "Gujarat", pincode: "382010" },
  tier: "Bronze", signup_date: "2026-10-21"
});

// =============================================================================
// orders  → fact_sales
// =============================================================================
// O1024: navo order, C023, P024 + P003 (navi price 1800) = 2600  (01:30)
insertNew("orders", { order_no: "O1024" }, {
  order_no: "O1024", customer_code: "C023", order_ts: ist("2026-10-21 01:30"),
  status: "PLACED", payment_method: "UPI",
  items: [{ product_code: "P024", qty: 1, unit_price: 800 },
          { product_code: "P003", qty: 1, unit_price: 1800 }],
  shipping: { city: "Gandhinagar", state: "Gujarat", pincode: "382010" },
  order_total: 2600
});

// O1015: SHIPPED → DELIVERED  (02:05)
updateLatest("orders", { order_no: "O1015" }, { status: "DELIVERED" });

// O1010: PLACED → SHIPPED  (02:40)
updateLatest("orders", { order_no: "O1010" }, { status: "SHIPPED" });

// =============================================================================
// payments  → this job
// =============================================================================
// PAY10: PENDING → SUCCESS (paid 10-21)  (01:00)
updateLatest("payments", { payment_id: "PAY10" }, {
  status: "SUCCESS", paid_at: ist("2026-10-21 01:00")
});

// PAY23: navo payment O1024  (01:31)
insertNew("payments", { payment_id: "PAY23" }, {
  payment_id: "PAY23", order_no: "O1024", method: "UPI", amount: 2600, status: "SUCCESS",
  paid_at: ist("2026-10-21 01:31")
});


// =============================================================================
// HARD deletes: records that were rejected in the first run and are removed at the source
// =============================================================================
remove("orders", { order_no: "O1023" });      // empty order (no items) cancelled
remove("payments", { payment_id: "PAY22" });  // corrupt payment ("abc") removed by the gateway team

// =============================================================================
// HARD deletes: LOADED records (bronze_db keeps the last version with _deleted: true)
// =============================================================================
// C020 closed her account. Her old order O1020 still points to her (bronze keeps the customer).
remove("customers", { customer_code: "C020" });
// O1019 was cancelled: the whole order document (with its 2 items) is removed. Payment PAY19 is a separate record.
remove("orders", { order_no: "O1019" });

print("");
["products", "customers", "orders", "payments"].forEach(c => print(
  `✔ ${DB_NAME}.${c.padEnd(10)} ${db.getCollection(c).countDocuments()} documents`));
print("  expected source: products 25, customers 23, orders 23, payments 23");
print("  expected bronze_db _deleted: customers 1 (C020), orders 2 (O1019, O1023), payments 1 (PAY22)");
print("  Next: wait until the subscriber LAG = 0, then run sample-data/99_verify_bronze.js");
