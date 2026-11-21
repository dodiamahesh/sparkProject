// =============================================================================
//  sales-pipeline (Job-2, J2_SALES)  |  02_mongo_incremental.js  |  changes AFTER the full load: UPDATE, INSERT, DELETE
//
//  Run (after 01 + the FULL load):
//      mongosh "mongodb://localhost:27017" sample-data/sales-pipeline/02_mongo_incremental.js
//      POST http://localhost:8080/api/etl/pipelines/sales-pipeline/run?loadType=INCREMENTAL
//
//  Changes between 2026-10-21 00:00 and 04:00 IST (design document, incremental run) taken from
//  "../sparkJavaMongoConnectorAirflowLinageTheory/sample_data/02_mongo_incremental_0400_data.js".
//  Difference: every changed / new document gets processedFlag = 'N', because this framework's
//  INCREMENTAL reads processedFlag = 'N' (or missing), not a timestamp window.
//
//  DELETE is not pushed to PostgreSQL (the job only reads existing documents). We delete documents that
//  were never loaded (rejected ones); their rejected-table row stays as history.
//
//  EXPECTED INCREMENTAL RUN:
//      products   read 2 (P003 price 2000 -> 1800, P024 new)
//      customers  read 3 (C003 Mumbai/Gold -> Pune/Platinum = SCD2 new version, C002 phone = SCD1, C023 new)
//      orders     read 3 (O1024 new with 2 lines, O1015 SHIPPED -> DELIVERED, O1010 PLACED -> SHIPPED)
//      payments   read 2 (PAY10 PENDING -> SUCCESS, PAY23 new)
//      -> dim_customer 22 rows (C003 has 2 versions), fact_sales 32, fact_payment 21. Nothing rejected.
//      Old order O1003 keeps the Mumbai version of C003 (point-in-time lookup).
// =============================================================================
const DB_NAME = "shopdb";
db = db.getSiblingDB(DB_NAME);

const ist = (s) => new Date(s.replace(" ", "T") + ":00+05:30");

// Same business key twice (e.g. P003 duplicate)? Only the LATEST version is changed.
function updateLatest(coll, keyFilter, setFields, sortField = "updated_at", arrayFilters) {
  const latest = db.getCollection(coll).find(keyFilter).sort({ [sortField]: -1 }).limit(1).toArray()[0];
  if (!latest) { print(`✘ ${coll} ${JSON.stringify(keyFilter)} not found`); return; }
  const opts = arrayFilters ? { arrayFilters } : {};
  db.getCollection(coll).updateOne({ _id: latest._id }, { $set: Object.assign({}, setFields, { processedFlag: "N" }) }, opts);
  print(`  UPDATE  ${coll.padEnd(10)} ${JSON.stringify(keyFilter)}`);
}

// New document: upsert on the business key (running the script twice creates no duplicates)
function insertNew(coll, keyFilter, doc) {
  db.getCollection(coll).updateOne(keyFilter, { $set: Object.assign({}, doc, { processedFlag: "N" }) }, { upsert: true });
  print(`  INSERT  ${coll.padEnd(10)} ${JSON.stringify(keyFilter)}`);
}

// Document removed at the source
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
  { price: 1800, "stock.$[wh].qty": 95, updated_at: ist("2026-10-21 01:10") },
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
  updated_at: ist("2026-10-21 00:45")
});

// =============================================================================
// customers  → this job
// =============================================================================
// C003: Mumbai → Pune, Gold → Platinum  (01:20)  → SCD2 (navi version)
updateLatest("customers", { customer_code: "C003" }, {
  "address.city": "Pune", "address.state": "Maharashtra", "address.pincode": "411001",
  tier: "Platinum", updated_at: ist("2026-10-21 01:20")
});

// C002: phone badlayo  (02:15)  → SCD1 (overwrite)
updateLatest("customers", { customer_code: "C002" }, {
  phone: "9876511122", updated_at: ist("2026-10-21 02:15")
});

// C023: navo customer  (00:30)
insertNew("customers", { customer_code: "C023" }, {
  customer_code: "C023", name: "Jay Parmar", email: "jay.parmar@mail.com", phone: "9876500023",
  address: { city: "Gandhinagar", state: "Gujarat", pincode: "382010" },
  tier: "Bronze", signup_date: "2026-10-21", updated_at: ist("2026-10-21 00:30")
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
  order_total: 2600, updated_at: ist("2026-10-21 01:30")
});

// O1015: SHIPPED → DELIVERED  (02:05)
updateLatest("orders", { order_no: "O1015" }, { status: "DELIVERED", updated_at: ist("2026-10-21 02:05") });

// O1010: PLACED → SHIPPED  (02:40)
updateLatest("orders", { order_no: "O1010" }, { status: "SHIPPED", updated_at: ist("2026-10-21 02:40") });

// =============================================================================
// payments  → this job
// =============================================================================
// PAY10: PENDING → SUCCESS (paid 10-21)  (01:00)
updateLatest("payments", { payment_id: "PAY10" }, {
  status: "SUCCESS", paid_at: ist("2026-10-21 01:00"), updated_at: ist("2026-10-21 01:00")
});

// PAY23: navo payment O1024  (01:31)
insertNew("payments", { payment_id: "PAY23" }, {
  payment_id: "PAY23", order_no: "O1024", method: "UPI", amount: 2600, status: "SUCCESS",
  paid_at: ist("2026-10-21 01:31"), updated_at: ist("2026-10-21 01:31")
});


// =============================================================================
// deletes: records that were rejected in the full load and are removed at the source
// =============================================================================
remove("orders", { order_no: "O1023" });      // empty order (no items) cancelled
remove("payments", { payment_id: "PAY22" });  // corrupt payment ("abc") removed by the gateway team

print("");
["products", "customers", "orders", "payments"].forEach(c => print(
  `✔ ${c.padEnd(10)} to read: ${db.getCollection(c).countDocuments({ processedFlag: { $in: ["N", null] } })}`));
print("  expected: products 2, customers 3, orders 3, payments 2");
print("  Next: POST /api/etl/pipelines/sales-pipeline/run?loadType=INCREMENTAL");
