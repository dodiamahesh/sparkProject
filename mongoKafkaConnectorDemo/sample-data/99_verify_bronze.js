// =============================================================================
//  99_verify_bronze.js  |  CDC check: sourceMongo_db (transaction DB) vs bronze_db (written by the subscriber)
//
//  Run (after any 01 / 02 script, when the subscriber LAG = 0):
//      mongosh "mongodb://localhost:27017/?replicaSet=rs0" sample-data/99_verify_bronze.js
//
//  For every collection, compared by _id (the subscriber writes the same _id as the source):
//      source         documents in sourceMongo_db
//      bronze         all documents in bronze_db  = live + _deleted (+ other)
//      live           _deleted: false  -> must be exactly the source documents
//      _deleted       _deleted: true   -> documents HARD deleted in the source (soft delete in bronze)
//      other          documents without _op: NOT written by the subscriber (for example loaded directly by an
//                     old ETL sample script). Remove them before testing, they make the counts wrong.
//  Problems:
//      not arrived    in the source, not live in bronze, and not a duplicate (subscriber still busy? LAG?
//                     DLQ: no mapping / destination collection missing?)
//  Not a problem:
//      duplicates     in the source, not in bronze, because a live bronze document with another _id has the same
//                     rule values (DUP_RULES below = meta.cdc_duplicate_rule): the subscriber stored them in
//                     PostgreSQL meta.meta_duplicate_record. So: source = live + duplicates.
//      not deleted    live in bronze, gone from the source   (delete event lost?)
//      wrong delete   _deleted in bronze, but still in the source
//
//  Then every soft-deleted document is listed with its business key, _op and the delete time (_source_ts).
//  "(stub)" = bronze never had the document before the delete, so only { _id, metadata } was written.
// =============================================================================
const src = db.getSiblingDB("sourceMongo_db");
const brz = db.getSiblingDB("bronze_db");

// business key of every sample collection (only used to print WHICH records are soft-deleted)
const KEY = {
  bank_payments: "paymentId", bank_customers: "customerId", bank_accounts: "accountId",
  bank_merchants: "merchantId", bank_transactions: "transactionId",
  ecom_categories: "categoryId", ecom_products: "productId", ecom_customers: "customerId",
  ecom_orders: "orderId", ecom_order_items: "orderItemId",
  ecom_warehouses: "warehouseId", ecom_carriers: "carrierId", ecom_shipments: "shipmentId",
  products: "product_code", customers: "customer_code", orders: "order_no", payments: "payment_id",
  vat_return_coll: "ackNo"
};

// source collection -> bronze_db destination collection.
// Sources = the list in "pipeline" / "startup.mode.copy.existing.namespace.regex" (kafka-connect/mongo-cdc-db-source.json),
// destinations = the active NOSQL (MongoDB source) rows of meta.cdc_collection_mapping (PostgreSQL). Keep all three the same.
const SYNCED = { customers: "customers", products: "products" };

// Duplicate rules: SAME fields as meta.cdc_duplicate_rule (sample-data/00_cdc_duplicate_rule.sql)
const DUP_RULES = { customers: ["customer_code"], products: ["product_code"] };

// value at a dot path ("address.pincode"); numbers compare by value (5 == 5.0), like the subscriber
const valueAt = (doc, path) => path.split(".").reduce((o, k) => (o === null || o === undefined) ? undefined : o[k], doc);
const ruleKey = (doc, fields) => {
  const values = fields.map(f => valueAt(doc, f));
  return values.some(v => v === undefined) ? null : JSON.stringify(values.map(v => (typeof v === "object" && v !== null && "toNumber" in v) ? v.toNumber() : v));
};

const isUserCollection = (n) => !n.startsWith("system.");
const collections = Object.keys(SYNCED).sort();
const skipped = src.getCollectionNames().filter(n => isUserCollection(n) && !(n in SYNCED)).sort();
const missingDestinations = collections.map(n => SYNCED[n]).filter(d => !brz.getCollectionNames().includes(d));

const toTime = (ts) => (ts === undefined || ts === null) ? "-" : new Date(Number(ts)).toISOString();
let problems = 0;
const deletedRows = [];

print("");
print("source -> bronze_db                 source  bronze =  live + _deleted + other  duplicates  result");
print("----------------------------------  ------  ------    ----   --------   -----  ----------  ------");
collections.forEach(name => {
  const key = KEY[name];
  const sourceIds = new Set(src.getCollection(name).find({}, { _id: 1 }).toArray().map(d => String(d._id)));

  const projection = { _id: 1, _op: 1, _deleted: 1, _source_ts: 1 };
  if (key) projection[key] = 1;
  const bronzeDocs = brz.getCollection(SYNCED[name]).find({}, projection).toArray();

  let live = 0, deleted = 0, other = 0, notDeleted = 0, wrongDelete = 0;
  const liveIds = new Set();
  bronzeDocs.forEach(d => {
    const id = String(d._id);
    if (d._op === undefined) { other++; return; }
    if (d._deleted === true) {
      deleted++;
      if (sourceIds.has(id)) wrongDelete++;
      deletedRows.push({ name, key: key && d[key] !== undefined ? d[key] : "(stub) _id " + id, op: d._op, ts: d._source_ts });
    } else {
      live++;
      liveIds.add(id);
      if (!sourceIds.has(id)) notDeleted++;
    }
  });
  // source documents not live in bronze: a duplicate (same rule values as a live bronze document) or not arrived
  const missingIds = [...sourceIds].filter(id => !liveIds.has(id));
  const fields = DUP_RULES[name];
  let duplicates = 0;
  if (fields && missingIds.length) {
    const liveKeys = new Set();
    brz.getCollection(SYNCED[name]).find({ _deleted: false }).forEach(d => { const k = ruleKey(d, fields); if (k) liveKeys.add(k); });
    src.getCollection(name).find().forEach(d => {
      if (!liveIds.has(String(d._id)) && liveKeys.has(ruleKey(d, fields))) duplicates++;
    });
  }
  const notArrived = missingIds.length - duplicates;

  const issues = [];
  if (notArrived) issues.push(`${notArrived} not arrived`);
  if (notDeleted) issues.push(`${notDeleted} not deleted`);
  if (wrongDelete) issues.push(`${wrongDelete} wrong delete`);
  if (other) issues.push(`${other} other`);
  if (issues.length) problems++;

  print(`${(name + " -> " + SYNCED[name]).padEnd(34)}  ${String(sourceIds.size).padStart(6)}  ${String(bronzeDocs.length).padStart(6)} = ` +
        `${String(live).padStart(5)} + ${String(deleted).padStart(8)} + ${String(other).padStart(5)}  ` +
        `${String(duplicates).padStart(10)}  ` +
        (issues.length ? "✘ " + issues.join(", ") : "✔"));
});

print("");
if (missingDestinations.length) {
  print(`✘ Destination collection(s) missing in bronze_db: ${missingDestinations.join(", ")}. The subscriber does NOT`);
  print("  create collections: their messages go to the DLQ. Run sample-data/00_create_bronze_collections.js.");
  print("");
  problems++;
}
if (skipped.length) {
  print(`Not synced by the connector (not checked): ${skipped.join(", ")}`);
  print("");
}
if (deletedRows.length === 0) {
  print("No soft-deleted documents in bronze_db yet (run a 02 script: it HARD deletes records in sourceMongo_db).");
} else {
  print(`Soft-deleted in bronze_db (_deleted: true) = HARD deleted in sourceMongo_db: ${deletedRows.length}`);
  deletedRows.forEach(r => print(`  ${r.name.padEnd(18)} ${String(r.key).padEnd(14)} _op=${r.op}  deleted at ${toTime(r.ts)}`));
}

print("");
print(problems === 0
  ? "✔ bronze_db matches sourceMongo_db: every live document exists in the source, every hard delete is a soft delete"
  : `✘ ${problems} collection(s) do not match. Subscriber running? LAG = 0? Anything in the DLQ topic cdc-dlq?` +
    (collections.length ? "" : " (no collections found: run a 01 script first)"));
