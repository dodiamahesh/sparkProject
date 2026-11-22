// =============================================================================
//  transaction-pipeline  |  02_mongo_incremental.js  |  changes AFTER the first run: UPDATE, INSERT, DELETE
//
//  Run (after 01, when the subscriber LAG = 0):
//      mongosh "mongodb://localhost:27017/?replicaSet=rs0" sample-data/transaction-pipeline/02_mongo_incremental.js
//      mongosh "mongodb://localhost:27017/?replicaSet=rs0" sample-data/99_verify_bronze.js
//
//  sourceMongo_db is the TRANSACTION database: a removed record is HARD deleted (the document is gone).
//  The subscriber keeps it in bronze_db with its last values: _deleted: true, _op: "delete".
//  Every changed / new document still gets updatedAt = NOW, like a source application stamps its changes.
//
//  Scenario "fix the parent, retry the child":
//      CUST-0009 had a bad email -> fixed now (new updatedAt). ACC-0009 was rejected as ORPHAN because of it,
//      so the source re-sends ACC-0009 (touches its updatedAt).
//
//  HARD DELETES in this script (expected in bronze_db with _deleted: true):
//      bank_merchants     MER-0007 (rejected casino merchant), MER-0006 (LOADED merchant, off-boarded)
//      bank_transactions  TXN-000013 (rejected ORPHAN), TXN-000006 (LOADED transaction, withdrawn)
//
//  EXPECTED after this script (fresh start: 01 ran once):
//                         source   bronze total = live + _deleted
//      bank_customers       13          13      =  13  +  0     (CUST-0002, CUST-0009 _op "update", CUST-0012 new)
//      bank_accounts        12          12      =  12  +  0     (ACC-0003, ACC-0009 _op "update", ACC-0012 new)
//      bank_merchants        7           9      =   7  +  2
//      bank_transactions    18          20      =  18  +  2     (TXN-000003 status REVERSED)
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
function remove(coll, filter) {
  const r = db.getCollection(coll).deleteMany(filter);
  print(`  DELETE ${coll} ${JSON.stringify(filter)} deleted=${r.deletedCount}`);
}

print("\n--- transaction-pipeline incremental changes ---");

// ---------------- customers ----------------
update("bank_customers", { customerId: "CUST-0002" }, { city: "Thiruvananthapuram" });   // moved city
update("bank_customers", { customerId: "CUST-0009" }, { email: "test.person@gmail.com" }); // fix
insert("bank_customers", { customerId: "CUST-0012" }, {
  customerId: "CUST-0012", firstName: "Meera", lastName: "Pillai", email: "meera.pillai@gmail.com", phone: "9567812340",
  dateOfBirth: "1994-07-12", gender: "F", city: "Kochi", state: "Kerala", country: "India", kycStatus: "VERIFIED",
  createdAt: "2026-11-20T08:10:00" });

// ---------------- accounts ----------------
update("bank_accounts", { accountId: "ACC-0003" }, { balance: 298000.00 });
update("bank_accounts", { accountId: "ACC-0009" }, {});            // re-sent: parent CUST-0009 is fixed now
insert("bank_accounts", { accountId: "ACC-0012" }, {
  accountId: "ACC-0012", customerId: "CUST-0012", accountType: "SAVINGS", branchCode: "BR-012", ifscCode: "FDRL0001234",
  balance: 25000, currency: "INR", status: "ACTIVE", openedDate: "2026-11-20" });

// ---------------- merchants ----------------
insert("bank_merchants", { merchantId: "MER-0009" }, {
  merchantId: "MER-0009", merchantName: "Lulu Hypermarket Kochi", category: "GROCERY", mccCode: "5411", city: "Kochi",
  country: "India", status: "ACTIVE", onboardedDate: "2026-11-01" });

// ---------------- transactions ----------------
update("bank_transactions", { transactionId: "TXN-000003" }, { status: "REVERSED" });
const t = (id, acc, cust, mer, amount, date) => ({
  transactionId: id, accountId: acc, customerId: cust, merchantId: mer, transactionType: "DEBIT", channel: "UPI",
  amount, currency: "INR", transactionDate: date, status: "SUCCESS", description: "Payment " + id });
insert("bank_transactions", { transactionId: "TXN-000017" }, t("TXN-000017", "ACC-0012", "CUST-0012", "MER-0009", 2150, "2026-11-20T09:00:00"));
insert("bank_transactions", { transactionId: "TXN-000018" }, t("TXN-000018", "ACC-0009", "CUST-0009", "MER-0003", 640, "2026-11-20T09:05:00"));
insert("bank_transactions", { transactionId: "TXN-000019" }, t("TXN-000019", "ACC-0001", "CUST-0001", "MER-0001", "x100", "2026-11-20T09:10:00"));  // CORRUPT amount

// ---------------- HARD deletes: records that were never loaded (rejected ones) ----------------
remove("bank_merchants", { merchantId: "MER-0007" });          // casino merchant off-boarded
remove("bank_transactions", { transactionId: "TXN-000013" });  // its rejected (ORPHAN) transaction

// ---------------- HARD deletes: LOADED records ----------------
// The source application removes them for good. bronze_db keeps the last version with _deleted: true.
// MER-0006 is a parent row: its old transaction TXN-000010 still points to it (bronze keeps the merchant).
remove("bank_merchants", { merchantId: "MER-0006" });
remove("bank_transactions", { transactionId: "TXN-000006" });

print("");
["bank_customers", "bank_accounts", "bank_merchants", "bank_transactions"].forEach(c => print(
  `✔ ${DB_NAME}.${c.padEnd(17)} ${db.getCollection(c).countDocuments()} documents`));
print("  expected source: customers 13, accounts 12, merchants 7, transactions 18");
print("  expected bronze_db _deleted: merchants 2 (MER-0006, MER-0007), transactions 2 (TXN-000006, TXN-000013)");
print("  Next: wait until the subscriber LAG = 0, then run sample-data/99_verify_bronze.js");
