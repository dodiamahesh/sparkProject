// =============================================================================
//  payment-pipeline  |  02_mongo_incremental.js  |  changes AFTER the first run: UPDATE, INSERT, DELETE
//
//  Run (after 01 + the first run):
//      mongosh "mongodb://localhost:27017" sample-data/payment-pipeline/02_mongo_incremental.js
//      POST http://localhost:8080/api/etl/pipelines/payment-pipeline/run
//
//  INCREMENTAL by timestamp: the next run reads documents with updatedAt > last watermark (= start of the
//  previous successful run). Every changed / new document below gets updatedAt = NOW (the moment this
//  script runs), exactly like a source application stamps its changes. MongoDB is never written by the ETL.
//
//  DELETE: the job reads only existing documents, so a hard delete is NOT pushed to PostgreSQL.
//  Here we delete only records that were never loaded (bad / test records); they simply disappear
//  from MongoDB, and the rejected-table row stays as history.
//
//  EXPECTED INCREMENTAL RUN:
//      read 6 | valid 4 | rejected 1 (VALIDATION) | deleted 1 (PAY-0005 soft-deleted at the source)
//      (PAY-0013 was rejected, now fixed -> loaded)
//      fact_payment 13 -> 16 rows (PAY-0013 fixed, PAY-0021 + PAY-0022 new; PAY-0007 updated in place),
//      of which 1 is_deleted = TRUE (PAY-0005)
//      status: SUCCESS_WITH_FAILED (1 rejected)
// =============================================================================
const DB_NAME = "bankdb";
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
/** Document removed at the source. */
function remove(coll, filter) {
  const r = db.getCollection(coll).deleteMany(filter);
  print(`  DELETE ${coll} ${JSON.stringify(filter)} deleted=${r.deletedCount}`);
}

print("\n--- payment-pipeline incremental changes ---");

// UPDATE: pending payment is now confirmed by the provider
update("payments", { paymentId: "PAY-0007" }, { status: "SUCCESS" });

// UPDATE (fix): the corrupt amount "12abc" was corrected at the source -> loaded now, removed from rejected table
update("payments", { paymentId: "PAY-0013" }, { amount: 1200 });

// INSERT: two new payments + one bad one (currency EUR is not allowed)
const base = { payerName: "Mehul Shah", payerEmail: "mehul.shah@gmail.com", payerPhone: "9876500011",
               payerCity: "Ahmedabad", referenceNumber: null };
insert("payments", { paymentId: "PAY-0021" }, Object.assign({}, base, {
  paymentId: "PAY-0021", payerId: "PYR-011", paymentMethod: "UPI", paymentProvider: "GPAY",
  billerName: "Torrent Power", billerCategory: "ELECTRICITY", amount: 1875, currency: "INR",
  paymentDate: "2026-11-20T08:00:00", status: "SUCCESS", referenceNumber: "REF1000021" }));
insert("payments", { paymentId: "PAY-0022" }, Object.assign({}, base, {
  paymentId: "PAY-0022", payerId: "PYR-011", paymentMethod: "WALLET", paymentProvider: "AMAZONPAY",
  billerName: "Adani Gas", billerCategory: "GAS", amount: 720, currency: "INR",
  paymentDate: "2026-11-20T08:30:00", status: "SUCCESS", referenceNumber: "REF1000022" }));
insert("payments", { paymentId: "PAY-0023" }, Object.assign({}, base, {
  paymentId: "PAY-0023", payerId: "PYR-011", paymentMethod: "CARD", paymentProvider: "VISA",
  billerName: "Airtel Postpaid", billerCategory: "MOBILE", amount: 499, currency: "EUR",
  paymentDate: "2026-11-20T08:45:00", status: "SUCCESS", referenceNumber: "REF1000023" }));   // VALIDATION currency

// DELETE: the CHEQUE payment (rejected, never loaded) and the QA test document were removed at the source
remove("payments", { paymentId: "PAY-0016" });
remove("payments", { paymentId: "PAY-0018", isTest: true });

// SOFT DELETE: the payment was cancelled at the source -> isDeleted = true + new updatedAt.
// Not validated: fact_payment row PAY-0005 gets is_deleted = TRUE (the row stays, reports filter it out).
update("payments", { paymentId: "PAY-0005" }, { isDeleted: true });

const pending = db.payments.countDocuments({ updatedAt: { $gte: NOW }, isTest: { $ne: true } });
print(`\n✔ Documents the INCREMENTAL run will read: ${pending} (expected 6)`);
print("  Next: POST /api/etl/pipelines/payment-pipeline/run");
