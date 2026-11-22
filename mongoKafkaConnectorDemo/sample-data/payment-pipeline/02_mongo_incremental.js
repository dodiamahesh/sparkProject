// =============================================================================
//  payment-pipeline  |  02_mongo_incremental.js  |  changes AFTER the first run: UPDATE, INSERT, DELETE
//
//  Run (after 01, when the subscriber LAG = 0):
//      mongosh "mongodb://localhost:27017/?replicaSet=rs0" sample-data/payment-pipeline/02_mongo_incremental.js
//      mongosh "mongodb://localhost:27017/?replicaSet=rs0" sample-data/99_verify_bronze.js
//
//  sourceMongo_db is the TRANSACTION database: a record removed by the application is HARD deleted
//  (deleteOne / deleteMany, the document is gone). The connector sends a "delete" event, and the subscriber
//  keeps the document in bronze_db with its last values and marks it: _deleted: true, _op: "delete".
//  Every changed / new document still gets updatedAt = NOW, like a source application stamps its changes.
//
//  HARD DELETES in this script (expected in bronze_db.bank_payments with _deleted: true):
//      PAY-0005  a LOADED (valid) payment, cancelled: first UPDATE status -> CANCELLED, then DELETE right after.
//                Tests the subscriber path "update event without fullDocument" (the document is already gone
//                when the connector looks it up). Bronze must keep status "CANCELLED".
//      PAY-0016  rejected record (CHEQUE), removed at the source
//      PAY-0018  QA test document, removed at the source
//
//  EXPECTED after this script (fresh start: 01 ran once):
//      sourceMongo_db.bank_payments 21   (21 + 3 new - 3 deleted)
//      bronze_db.bank_payments      24 = 21 live (_deleted false) + 3 soft-deleted (_deleted true)
//      updates in bronze: PAY-0007 status SUCCESS, PAY-0013 amount 1200 (_op "update")
// =============================================================================
const DB_NAME = "sourceMongo_db";   // TRANSACTION (source) database, captured by the CDC connector
db = db.getSiblingDB(DB_NAME);

const NOW = new Date();   // every change is stamped with the current time -> newer than the last watermark

/** Changed document: new values + updatedAt = NOW, so the next run (updatedAt > watermark) picks it up. */
function update(coll, filter, changes) {
  const r = db.getCollection(coll).updateOne(filter,
      { $set: Object.assign({}, changes, { updatedAt: NOW }) });
  print(`  UPDATE ${coll} ${JSON.stringify(filter)} matched=${r.matchedCount}`);
}
/** New document (upsert on the business key, so running this script twice creates no duplicates). */
function insert(coll, filter, doc) {
  db.getCollection(coll).updateOne(filter,
      { $set: Object.assign({}, doc, { updatedAt: NOW }) }, { upsert: true });
  print(`  INSERT ${coll} ${JSON.stringify(filter)}`);
}
/** HARD delete at the source (document is gone). Bronze keeps it with _deleted: true. */
function remove(coll, filter) {
  const r = db.getCollection(coll).deleteMany(filter);
  print(`  DELETE ${coll} ${JSON.stringify(filter)} deleted=${r.deletedCount}`);
}

print("\n--- payment-pipeline incremental changes ---");

// UPDATE: pending payment is now confirmed by the provider
update("bank_payments", { paymentId: "PAY-0007" }, { status: "SUCCESS" });

// UPDATE (fix): the corrupt amount "12abc" was corrected at the source -> loaded now, removed from rejected table
update("bank_payments", { paymentId: "PAY-0013" }, { amount: 1200 });

// INSERT: two new payments + one bad one (currency EUR is not allowed)
const base = { payerName: "Mehul Shah", payerEmail: "mehul.shah@gmail.com", payerPhone: "9876500011",
               payerCity: "Ahmedabad", referenceNumber: null };
insert("bank_payments", { paymentId: "PAY-0021" }, Object.assign({}, base, {
  paymentId: "PAY-0021", payerId: "PYR-011", paymentMethod: "UPI", paymentProvider: "GPAY",
  billerName: "Torrent Power", billerCategory: "ELECTRICITY", amount: 1875, currency: "INR",
  paymentDate: "2026-11-20T08:00:00", status: "SUCCESS", referenceNumber: "REF1000021" }));
insert("bank_payments", { paymentId: "PAY-0022" }, Object.assign({}, base, {
  paymentId: "PAY-0022", payerId: "PYR-011", paymentMethod: "WALLET", paymentProvider: "AMAZONPAY",
  billerName: "Adani Gas", billerCategory: "GAS", amount: 720, currency: "INR",
  paymentDate: "2026-11-20T08:30:00", status: "SUCCESS", referenceNumber: "REF1000022" }));
insert("bank_payments", { paymentId: "PAY-0023" }, Object.assign({}, base, {
  paymentId: "PAY-0023", payerId: "PYR-011", paymentMethod: "CARD", paymentProvider: "VISA",
  billerName: "Airtel Postpaid", billerCategory: "MOBILE", amount: 499, currency: "EUR",
  paymentDate: "2026-11-20T08:45:00", status: "SUCCESS", referenceNumber: "REF1000023" }));   // VALIDATION currency

// HARD DELETE: the CHEQUE payment (rejected, never loaded) and the QA test document were removed at the source
remove("bank_payments", { paymentId: "PAY-0016" });
remove("bank_payments", { paymentId: "PAY-0018", isTest: true });

// HARD DELETE of a loaded payment: the application first marks it CANCELLED, then removes it.
// bronze_db: PAY-0005 stays with status "CANCELLED", _op "delete", _deleted true.
update("bank_payments", { paymentId: "PAY-0005" }, { status: "CANCELLED" });
remove("bank_payments", { paymentId: "PAY-0005" });

print(`\n✔ ${DB_NAME}.bank_payments: ${db.bank_payments.countDocuments()} documents (expected 21)`);
print("  expected in bronze_db.bank_payments: 24 = 21 live + 3 _deleted (PAY-0005, PAY-0016, PAY-0018)");
print("  Next: wait until the subscriber LAG = 0, then run sample-data/99_verify_bronze.js");
