// =============================================================================
//  transaction-pipeline  |  01_mongo_full_load.js  |  MongoDB sample data for the FIRST run
//
//  Run:   mongosh "mongodb://localhost:27017/?replicaSet=rs0" sample-data/transaction-pipeline/01_mongo_full_load.js
//  Check: mongosh "mongodb://localhost:27017/?replicaSet=rs0" sample-data/99_verify_bronze.js
//
//  CDC flow: sourceMongo_db (this script writes here) -> connector "mongo-cdc-sourcemongo-source"
//            -> Kafka topic "sourceMongo_db" -> mongo-cdc-subscriber (subscriber-sourcemongo.properties) -> bronze_db
//
//  Collections get the prefix "bank_" (were customers / accounts / merchants / transactions in bankdb): the sales
//  job also has a "customers" collection with a different shape, and now both live in ONE database.
//      bank_customers -> dim_customer,  bank_accounts -> dim_account (customer must exist),
//      bank_merchants -> dim_merchant,  bank_transactions -> fact_transaction (customer, account, merchant must exist)
//
//  EXPECTED in bronze_db: customers 12, accounts 11, merchants 8, transactions 17; all _op "insert", _deleted false
//
//  ETL later (reads bronze_db), EXPECTED FIRST RUN:
//      customers     read 12 | valid 8  | rejected 4 (DUPLICATE CUST-0004, VALIDATION CUST-0009 / CUST-0010, CORRUPT CUST-0011)
//      accounts      read 11 | valid 8  | rejected 3 (ORPHAN ACC-0009, CORRUPT ACC-0010, VALIDATION ACC-0011)
//      merchants     read 8  | valid 7  | rejected 1 (VALIDATION MER-0007)
//      transactions  read 17 | valid 12 | rejected 5 (DUPLICATE, ORPHAN x2, VALIDATION, CORRUPT)
//      status: SUCCESS_WITH_FAILED (valid rows loaded, the rejected ones are in meta.meta_rejected_record)
// =============================================================================
const DB_NAME = "sourceMongo_db";   // TRANSACTION (source) database, captured by the CDC connector
db = db.getSiblingDB(DB_NAME);

// updatedAt = the INCREMENTAL WATERMARK field: it must be a BSON Date (IST time written here, MongoDB stores UTC)
const ist = (s) => new Date(s + "+05:30");   // "2026-11-01T09:16:00" (IST) -> Date


const COLLECTIONS = ["bank_customers", "bank_accounts", "bank_merchants", "bank_transactions"];
// EMPTY, not drop: deleteMany sends one CDC delete event per old document (bronze: _deleted = true).
// drop() would send only a "drop" event, which the subscriber ignores (apply.drop=false), so old documents would stay live in bronze.
COLLECTIONS.forEach(c => db.getCollection(c).deleteMany({}));

// ---------------------------------------------------------------- customers (11)
function cust(id, first, last, email, phone, dob, gender, city, state, kyc, updatedAt) {
  return { customerId: id, firstName: first, lastName: last, email, phone, dateOfBirth: dob, gender,
           city, state, country: "India", kycStatus: kyc, createdAt: "2025-01-10T10:00:00", updatedAt: ist(updatedAt) };
}
db.bank_customers.insertMany([
  cust("CUST-0001", "Nikhil",  "Agarwal", "nikhil.agarwal@gmail.com",   "8164452831",      "1985-01-06", "M", "Rajkot",    "Gujarat",     "VERIFIED", "2026-10-01T10:00:00"),
  cust("CUST-0002", "deepak",  "nair",    " Deepak.Nair@Gmail.com ",    "+91 69259 86413", "1975-08-19", "m", "kochi",     "kerala",      "verified", "2026-10-01T10:00:00"),   // sanitize: case, email, phone
  cust("CUST-0003", "Anjali",  "Rao",     "anjali.rao@yahoo.in",        "9123456780",      "1992-03-14", "F", "Hyderabad", "Telangana",   "VERIFIED", "2026-10-02T10:00:00"),
  cust("CUST-0004", "Suresh",  "Kumar",   "suresh.kumar@gmail.com",     "9988776655",      "1980-11-30", "M", "Chennai",   "Tamil Nadu",  "VERIFIED", "2026-10-05T10:00:00"),   // latest version
  cust("CUST-0005", "Fatima",  "Shaikh",  "fatima.shaikh@outlook.com",  "7894561230",      "1995-06-21", "F", "Mumbai",    "Maharashtra", "VERIFIED", "2026-10-03T10:00:00"),
  cust("CUST-0006", "Rohit",   "Verma",   "rohit.verma@gmail.com",      "8877665544",      "1988-09-09", "M", "Lucknow",   "Uttar Pradesh","PENDING", "2026-10-03T10:00:00"),
  cust("CUST-0007", "Kavita",  "Joshi",   "kavita.joshi@gmail.com",     "9001122334",      "1990-12-01", "F", "Indore",    "Madhya Pradesh","VERIFIED","2026-10-04T10:00:00"),
  cust("CUST-0008", "Imran",   "Khan",    "imran.khan@gmail.com",       "9334455667",      "1983-04-17", "O", "Bhopal",    "Madhya Pradesh","VERIFIED","2026-10-04T10:00:00"),
  cust("CUST-0004", "Suresh",  "Kumar",   "suresh.k@gmail.com",         "9988776655",      "1980-11-30", "M", "Chennai",   "Tamil Nadu",  "PENDING",  "2026-09-01T10:00:00"),   // DUPLICATE (older)
  cust("CUST-0009", "Test",    "Person",  "test.person@@gmail",         "9876543210",      "1991-01-01", "M", "Delhi",     "Delhi",       "VERIFIED", "2026-10-06T10:00:00"),   // VALIDATION email (fixed in incremental)
  cust("CUST-0010", "Mohan",   "Lal",     "mohan.lal@gmail.com",        "12345",           "1970-05-05", "M", "Patna",     "Bihar",       "VERIFIED", "2026-10-06T10:00:00"),   // VALIDATION phone
  cust("CUST-0011", "Rekha",   "Iyer",    "rekha.iyer@gmail.com",       "9445566778",      "1999-02-30", "F", "Madurai",   "Tamil Nadu",  "VERIFIED", "2026-10-06T10:00:00")    // CORRUPT dateOfBirth (30 Feb)
]);

// ---------------------------------------------------------------- accounts (11)
function acc(id, customerId, type, balance, status, openedDate, updatedAt, ifsc) {
  return { accountId: id, customerId, accountType: type, branchCode: "BR-0" + id.slice(-2), ifscCode: ifsc || "HDFC0001234",
           balance, currency: "INR", status, openedDate, updatedAt: ist(updatedAt) };
}
db.bank_accounts.insertMany([
  acc("ACC-0001", "CUST-0001", "SAVINGS", 84904.69,     "ACTIVE",  "2018-11-14", "2026-10-01T11:00:00"),
  acc("ACC-0002", "CUST-0002", "savings", "1,25,000.50","active",  "2019-06-27", "2026-10-01T11:00:00", "icic0432848"),   // sanitize: case, separators
  acc("ACC-0003", "CUST-0003", "CURRENT", 321815.39,    "ACTIVE",  "2020-02-11", "2026-10-02T11:00:00", "UTIB0718560"),
  acc("ACC-0004", "CUST-0004", "SALARY",  56000,        "ACTIVE",  "2021-08-01", "2026-10-02T11:00:00"),
  acc("ACC-0005", "CUST-0005", "SAVINGS", 12000,        "ACTIVE",  "2022-01-15", "2026-10-03T11:00:00"),
  acc("ACC-0006", "CUST-0006", "NRI",     780000,       "ACTIVE",  "2017-03-03", "2026-10-03T11:00:00"),
  acc("ACC-0007", "CUST-0007", "SAVINGS", 150,          "DORMANT", "2015-07-07", "2026-10-04T11:00:00"),
  acc("ACC-0008", "CUST-0008", "CURRENT", 990000,       "ACTIVE",  "2023-05-20", "2026-10-04T11:00:00"),
  acc("ACC-0009", "CUST-0009", "SAVINGS", 5000,         "ACTIVE",  "2024-01-01", "2026-10-06T11:00:00"),   // ORPHAN: CUST-0009 rejected (retried in incremental)
  acc("ACC-0010", "CUST-0001", "SAVINGS", "abc",        "ACTIVE",  "2024-02-02", "2026-10-06T11:00:00"),   // CORRUPT balance
  acc("ACC-0011", "CUST-0002", "CURRENT", 1000,         "ACTIVE",  "2024-03-03", "2026-10-06T11:00:00", "HDFC123")   // VALIDATION ifsc
]);

// ---------------------------------------------------------------- merchants (8)
function mer(id, name, category, mcc, city, onboarded, updatedAt) {
  return { merchantId: id, merchantName: name, category, mccCode: mcc, city, country: "India",
           status: "ACTIVE", onboardedDate: onboarded, updatedAt: ist(updatedAt) };
}
db.bank_merchants.insertMany([
  mer("MER-0001", "Apollo Pharmacy Indore",  "HEALTHCARE",  "5912", "Indore",    "2020-06-14", "2026-10-01T12:00:00"),
  mer("MER-0002", "Bharat Petroleum Vadodara","fuel",       "5541", "Vadodara",  "2021-03-19", "2026-10-01T12:00:00"),   // sanitize: category
  mer("MER-0003", "Reliance Fresh Pune",     "GROCERY",     "5411", "Pune",      "2019-09-09", "2026-10-02T12:00:00"),
  mer("MER-0004", "Croma Mumbai",            "ELECTRONICS", "5732", "Mumbai",    "2018-04-04", "2026-10-02T12:00:00"),
  mer("MER-0005", "IRCTC",                   "TRAVEL",      "4112", "Delhi",     "2017-01-01", "2026-10-03T12:00:00"),
  mer("MER-0006", "Barbeque Nation Chennai", "DINING",      "5812", "Chennai",   "2022-11-11", "2026-10-03T12:00:00"),
  mer("MER-0007", "Lucky Star Casino",       "CASINO",      "7995", "Goa",       "2023-02-02", "2026-10-04T12:00:00"),   // VALIDATION category (deleted in incremental)
  mer("MER-0008", "PVR Cinemas Surat",       "ENTERTAINMENT", null,  "Surat",    "2024-06-06", "2026-10-04T12:00:00")
]);

// ---------------------------------------------------------------- transactions (17)
function txn(id, acc, cust, mer, type, channel, amount, date, status, updatedAt) {
  return { transactionId: id, accountId: acc, customerId: cust, merchantId: mer, transactionType: type,
           channel, amount, currency: "INR", transactionDate: date, status, description: "Payment " + id, updatedAt: ist(updatedAt) };
}
db.bank_transactions.insertMany([
  txn("TXN-000001", "ACC-0001", "CUST-0001", "MER-0001", "DEBIT",  "POS",        1250.75,    "2026-11-01T10:15:00", "SUCCESS", "2026-11-01T10:16:00"),
  txn("TXN-000002", "ACC-0002", "CUST-0002", "MER-0002", "debit",  "upi",        "2,300.00", "2026-11-01T18:40:00", "success", "2026-11-01T18:41:00"),   // sanitize
  txn("TXN-000003", "ACC-0003", "CUST-0003", "MER-0003", "DEBIT",  "UPI",        845,        "2026-11-02T09:05:00", "SUCCESS", "2026-11-02T09:06:00"),   // incremental: REVERSED
  txn("TXN-000004", "ACC-0004", "CUST-0004", "MER-0004", "DEBIT",  "ONLINE",     45999,      "2026-11-03T20:10:00", "SUCCESS", "2026-11-03T20:11:00"),
  txn("TXN-000005", "ACC-0005", "CUST-0005", "MER-0005", "DEBIT",  "NETBANKING", 3200,       "2026-11-04T11:00:00", "SUCCESS", "2026-11-04T11:30:00"),   // latest version
  txn("TXN-000006", "ACC-0006", "CUST-0006", "MER-0006", "DEBIT",  "POS",        2750,       "2026-11-05T21:30:00", "SUCCESS", "2026-11-05T21:31:00"),
  txn("TXN-000007", "ACC-0001", "CUST-0001", "MER-0003", "CREDIT", "UPI",        500,        "2026-11-06T08:00:00", "SUCCESS", "2026-11-06T08:01:00"),
  txn("TXN-000008", "ACC-0007", "CUST-0007", "MER-0001", "DEBIT",  "ATM",        2000,       "2026-11-07T13:00:00", "FAILED",  "2026-11-07T13:01:00"),
  txn("TXN-000009", "ACC-0008", "CUST-0008", "MER-0004", "DEBIT",  "ONLINE",     18999,      "2026-11-08T16:45:00", "PENDING", "2026-11-08T16:46:00"),
  txn("TXN-000010", "ACC-0002", "CUST-0002", "MER-0006", "DEBIT",  "POS",        1640,       "2026-11-09T20:00:00", "SUCCESS", "2026-11-09T20:01:00"),
  txn("TXN-000011", "ACC-0003", "CUST-0003", "MER-0008", "DEBIT",  "ONLINE",     900,        "2026-11-10T19:00:00", "SUCCESS", "2026-11-10T19:01:00"),
  txn("TXN-000012", "ACC-0008", "CUST-0008", "MER-0004", "DEBIT",  "ONLINE",     75000,      "2026-11-11T12:00:00", "SUCCESS", "2026-11-11T12:01:00"),
  txn("TXN-000005", "ACC-0005", "CUST-0005", "MER-0005", "DEBIT",  "NETBANKING", 3200,       "2026-11-04T11:00:00", "PENDING", "2026-11-04T11:01:00"),   // DUPLICATE (older)
  txn("TXN-000013", "ACC-0001", "CUST-0001", "MER-0007", "DEBIT",  "ONLINE",     5000,       "2026-11-12T22:00:00", "SUCCESS", "2026-11-12T22:01:00"),   // ORPHAN merchant (MER-0007 rejected; deleted in incremental)
  txn("TXN-000014", "ACC-0004", "CUST-0004", "MER-0003", "DEBIT",  "UPI",        0,          "2026-11-12T10:00:00", "SUCCESS", "2026-11-12T10:01:00"),   // VALIDATION amount 0
  txn("TXN-000015", "ACC-0005", "CUST-0005", "MER-0002", "DEBIT",  "POS",        700,        "2026-11-35T10:00:00", "SUCCESS", "2026-11-12T11:01:00"),   // CORRUPT date
  txn("TXN-000016", "ACC-0099", "CUST-0003", "MER-0001", "DEBIT",  "POS",        300,        "2026-11-12T12:00:00", "SUCCESS", "2026-11-12T12:01:00")    // ORPHAN account
]);

// Indexes: business key + updatedAt (the watermark field: fast incremental $match on the time window)
db.bank_customers.createIndex({ customerId: 1 });       db.bank_customers.createIndex({ updatedAt: 1 });
db.bank_accounts.createIndex({ accountId: 1 });         db.bank_accounts.createIndex({ updatedAt: 1 });
db.bank_merchants.createIndex({ merchantId: 1 });       db.bank_merchants.createIndex({ updatedAt: 1 });
db.bank_transactions.createIndex({ transactionId: 1 }); db.bank_transactions.createIndex({ updatedAt: 1 });

print("");
COLLECTIONS.forEach(c => print(`✔ ${DB_NAME}.${c.padEnd(17)} ${db.getCollection(c).countDocuments()} documents`));
print("  expected: customers 12, accounts 11, merchants 8, transactions 17");
print("  Next: wait until the subscriber LAG = 0, then run sample-data/99_verify_bronze.js");
