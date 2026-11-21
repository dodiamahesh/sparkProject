// =============================================================================
//  payment-pipeline  |  02_mongo_incremental.js  |  changes AFTER the full load: UPDATE, INSERT, DELETE
//
//  Run (after 01 + the FULL load):
//      mongosh "mongodb://localhost:27017" sample-data/payment-pipeline/02_mongo_incremental.js
//      POST http://localhost:8080/api/etl/pipelines/payment-pipeline/run?loadType=INCREMENTAL
//
//  How INCREMENTAL works in this framework: the job reads documents with processedFlag = 'N' (or no flag).
//  After a successful run it writes Y (loaded), E (rejected) or D (duplicate). A source system that
//  changes a document must set processedFlag back to 'N'. This script does exactly that.
//
//  DELETE: the job reads only existing documents, so a hard delete is NOT pushed to PostgreSQL.
//  Here we delete only records that were never loaded (bad / test records); they simply disappear
//  from MongoDB, and the rejected-table row stays as history.
//
//  EXPECTED INCREMENTAL RUN:
//      read 5 | valid 4 | rejected 1 (VALIDATION) | resolved 1 (PAY-0013 was rejected, now fixed)
//      fact_payment 13 -> 16 (PAY-0013 fixed, PAY-0021 + PAY-0022 new; PAY-0007 updated in place)
// =============================================================================
const DB_NAME = "bankdb";
db = db.getSiblingDB(DB_NAME);

const NOW = "2026-11-20T10:00:00";

/** Changed document: new values + newer updatedAt + processedFlag 'N' so the next INCREMENTAL picks it up. */
function update(coll, filter, changes) {
  const r = db.getCollection(coll).updateOne(filter,
      { $set: Object.assign({}, changes, { updatedAt: changes.updatedAt || NOW, processedFlag: "N" }) });
  print(`  UPDATE ${coll} ${JSON.stringify(filter)} matched=${r.matchedCount}`);
}
/** New document (upsert on the business key, so running this script twice creates no duplicates). */
function insert(coll, filter, doc) {
  db.getCollection(coll).updateOne(filter,
      { $set: Object.assign({}, doc, { processedFlag: "N" }) }, { upsert: true });
  print(`  INSERT ${coll} ${JSON.stringify(filter)}`);
}
/** Document removed at the source. */
function remove(coll, filter) {
  const r = db.getCollection(coll).deleteMany(filter);
  print(`  DELETE ${coll} ${JSON.stringify(filter)} deleted=${r.deletedCount}`);
}

print("\n--- payment-pipeline incremental changes ---");

// UPDATE: pending payment is now confirmed by the provider
update("payments", { paymentId: "PAY-0007" }, { status: "SUCCESS", updatedAt: "2026-11-20T09:00:00" });

// UPDATE (fix): the corrupt amount "12abc" was corrected at the source -> loaded now, removed from rejected table
update("payments", { paymentId: "PAY-0013" }, { amount: 1200, updatedAt: "2026-11-20T09:10:00" });

// INSERT: two new payments + one bad one (currency EUR is not allowed)
const base = { payerName: "Mehul Shah", payerEmail: "mehul.shah@gmail.com", payerPhone: "9876500011",
               payerCity: "Ahmedabad", referenceNumber: null };
insert("payments", { paymentId: "PAY-0021" }, Object.assign({}, base, {
  paymentId: "PAY-0021", payerId: "PYR-011", paymentMethod: "UPI", paymentProvider: "GPAY",
  billerName: "Torrent Power", billerCategory: "ELECTRICITY", amount: 1875, currency: "INR",
  paymentDate: "2026-11-20T08:00:00", status: "SUCCESS", referenceNumber: "REF1000021", updatedAt: "2026-11-20T08:01:00" }));
insert("payments", { paymentId: "PAY-0022" }, Object.assign({}, base, {
  paymentId: "PAY-0022", payerId: "PYR-011", paymentMethod: "WALLET", paymentProvider: "AMAZONPAY",
  billerName: "Adani Gas", billerCategory: "GAS", amount: 720, currency: "INR",
  paymentDate: "2026-11-20T08:30:00", status: "SUCCESS", referenceNumber: "REF1000022", updatedAt: "2026-11-20T08:31:00" }));
insert("payments", { paymentId: "PAY-0023" }, Object.assign({}, base, {
  paymentId: "PAY-0023", payerId: "PYR-011", paymentMethod: "CARD", paymentProvider: "VISA",
  billerName: "Airtel Postpaid", billerCategory: "MOBILE", amount: 499, currency: "EUR",
  paymentDate: "2026-11-20T08:45:00", status: "SUCCESS", referenceNumber: "REF1000023", updatedAt: "2026-11-20T08:46:00" }));   // VALIDATION currency

// DELETE: the CHEQUE payment (rejected, never loaded) and the QA test document were removed at the source
remove("payments", { paymentId: "PAY-0016" });
remove("payments", { paymentId: "PAY-0018", isTest: true });

const pending = db.payments.countDocuments({ processedFlag: { $in: ["N", null] }, isTest: { $ne: true } });
print(`\n✔ Documents the INCREMENTAL run will read: ${pending} (expected 5)`);
print("  Next: POST /api/etl/pipelines/payment-pipeline/run?loadType=INCREMENTAL");
