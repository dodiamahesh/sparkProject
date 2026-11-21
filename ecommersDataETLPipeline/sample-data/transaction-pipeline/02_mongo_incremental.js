// =============================================================================
//  transaction-pipeline  |  02_mongo_incremental.js  |  changes AFTER the first run: UPDATE, INSERT, DELETE
//
//  Run (after 01 + the first run):
//      mongosh "mongodb://localhost:27017" sample-data/transaction-pipeline/02_mongo_incremental.js
//      POST http://localhost:8080/api/etl/pipelines/transaction-pipeline/run
//
//  INCREMENTAL by timestamp: the next run reads documents with updatedAt > last watermark (= start of the
//  previous successful run). Every changed / new document below gets updatedAt = NOW (the moment this
//  script runs), exactly like a source application stamps its changes. MongoDB is never written by the ETL.
//  DELETE is not pushed to PostgreSQL (the job only reads existing documents). We delete records that
//  were never loaded (rejected ones); their rejected-table row stays as history.
//
//  Scenario "fix the parent, retry the child":
//      CUST-0009 had a bad email -> fixed now (new updatedAt). ACC-0009 was rejected as ORPHAN because of it,
//      so the source re-sends ACC-0009 (touches its updatedAt) and it loads in the same run.
//
//  EXPECTED INCREMENTAL RUN:
//      customers    read 3 (CUST-0002 update, CUST-0009 fixed, CUST-0012 new)  -> all valid, 1 resolved
//      accounts     read 3 (ACC-0003 update, ACC-0009 retry, ACC-0012 new)     -> all valid, 1 resolved
//      merchants    read 1 (MER-0009 new)
//      transactions read 4 (TXN-000003 update, TXN-000017/18 new, TXN-000019 bad) -> 3 valid, 1 CORRUPT
//      -> dim_customer 10, dim_account 10, dim_merchant 8, fact_transaction 14; resolved 2 (CUST-0009, ACC-0009)
//      status: SUCCESS_WITH_FAILED (TXN-000019 rejected)
// =============================================================================
const DB_NAME = "bankdb";
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

print("\n--- transaction-pipeline incremental changes ---");

// ---------------- customers ----------------
update("customers", { customerId: "CUST-0002" }, { city: "Thiruvananthapuram" });   // moved city
update("customers", { customerId: "CUST-0009" }, { email: "test.person@gmail.com" }); // fix
insert("customers", { customerId: "CUST-0012" }, {
  customerId: "CUST-0012", firstName: "Meera", lastName: "Pillai", email: "meera.pillai@gmail.com", phone: "9567812340",
  dateOfBirth: "1994-07-12", gender: "F", city: "Kochi", state: "Kerala", country: "India", kycStatus: "VERIFIED",
  createdAt: "2026-11-20T08:10:00" });

// ---------------- accounts ----------------
update("accounts", { accountId: "ACC-0003" }, { balance: 298000.00 });
update("accounts", { accountId: "ACC-0009" }, {});            // re-sent: parent CUST-0009 is fixed now
insert("accounts", { accountId: "ACC-0012" }, {
  accountId: "ACC-0012", customerId: "CUST-0012", accountType: "SAVINGS", branchCode: "BR-012", ifscCode: "FDRL0001234",
  balance: 25000, currency: "INR", status: "ACTIVE", openedDate: "2026-11-20" });

// ---------------- merchants ----------------
insert("merchants", { merchantId: "MER-0009" }, {
  merchantId: "MER-0009", merchantName: "Lulu Hypermarket Kochi", category: "GROCERY", mccCode: "5411", city: "Kochi",
  country: "India", status: "ACTIVE", onboardedDate: "2026-11-01" });

// ---------------- transactions ----------------
update("transactions", { transactionId: "TXN-000003" }, { status: "REVERSED" });
const t = (id, acc, cust, mer, amount, date) => ({
  transactionId: id, accountId: acc, customerId: cust, merchantId: mer, transactionType: "DEBIT", channel: "UPI",
  amount, currency: "INR", transactionDate: date, status: "SUCCESS", description: "Payment " + id });
insert("transactions", { transactionId: "TXN-000017" }, t("TXN-000017", "ACC-0012", "CUST-0012", "MER-0009", 2150, "2026-11-20T09:00:00"));
insert("transactions", { transactionId: "TXN-000018" }, t("TXN-000018", "ACC-0009", "CUST-0009", "MER-0003", 640, "2026-11-20T09:05:00"));
insert("transactions", { transactionId: "TXN-000019" }, t("TXN-000019", "ACC-0001", "CUST-0001", "MER-0001", "x100", "2026-11-20T09:10:00"));  // CORRUPT amount

// ---------------- deletes (records that were never loaded) ----------------
remove("merchants", { merchantId: "MER-0007" });          // casino merchant off-boarded
remove("transactions", { transactionId: "TXN-000013" });  // its rejected (ORPHAN) transaction

print("");
["customers", "accounts", "merchants", "transactions"].forEach(c => print(
  `✔ ${c.padEnd(13)} to read: ${db.getCollection(c).countDocuments({ updatedAt: { $gte: NOW }, isTest: { $ne: true } })}`));
print("  expected: customers 3, accounts 3, merchants 1, transactions 4");
print("  Next: POST /api/etl/pipelines/transaction-pipeline/run");
