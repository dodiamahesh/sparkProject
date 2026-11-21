// =============================================================================
//  payment-pipeline  |  01_mongo_full_load.js  |  MongoDB sample data for the FULL load
//
//  Run:   mongosh "mongodb://localhost:27017" sample-data/payment-pipeline/01_mongo_full_load.js
//  Then:  POST http://localhost:8080/api/etl/pipelines/payment-pipeline/run?loadType=FULL
//
//  Database: bankdb (banking examples). The collection "payments" also exists in shopdb with the
//  SALES shape, so the banking jobs use their own database. Start the app for this job with
//      $env:MONGO_DATABASE="bankdb"
//
//  Re-run safe: the collection is dropped and loaded again (all processedFlag values gone = new).
//  Dates are strings "yyyy-MM-ddTHH:mm:ss" (IST); every field is read by Spark as String.
//
//  EXPECTED FULL LOAD (test document is never read):
//      read 20 | valid 13 | rejected 7 (1 DUPLICATE, 2 CORRUPT, 4 VALIDATION) | warnings 1
//      fact_payment 13, dim_payer 9, dim_payment_method 7, dim_biller 9
// =============================================================================
const DB_NAME = "bankdb";
db = db.getSiblingDB(DB_NAME);

const COLLECTIONS = ["payments"];
COLLECTIONS.forEach(c => db.getCollection(c).drop());

function pay(id, payerId, payerName, email, phone, city, method, provider, biller, category,
             amount, currency, paymentDate, status, ref, updatedAt, extra) {
  return Object.assign({
    paymentId: id, payerId, payerName, payerEmail: email, payerPhone: phone, payerCity: city,
    paymentMethod: method, paymentProvider: provider, billerName: biller, billerCategory: category,
    amount, currency, paymentDate, status, referenceNumber: ref, updatedAt
  }, extra || {});
}

db.payments.insertMany([
  // ---------------- good records (some need SANITIZE) ----------------
  pay("PAY-0001", "PYR-001", "Rahul Patel",  "rahul.patel@gmail.com",     "9876500001",       "Ahmedabad", "UPI",        "GPAY",    "Torrent Power",  "ELECTRICITY", 1450.50,     "INR", "2026-11-01T09:15:00", "SUCCESS", "REF1000001", "2026-11-01T09:16:00"),
  pay("PAY-0002", "PYR-002", "priya shah",   "  PRIYA.SHAH@GMAIL.COM ",   "+91 98765-00002",  "surat",     "upi",        "phonepe", "Jio Fiber",      "broadband",   999,         "inr", "2026-11-01T11:40:00", "success", "REF1000002", "2026-11-01T11:41:00"),   // sanitize: name, email, phone, case
  pay("PAY-0003", "PYR-003", "Amit Mehta",   "amit.mehta@yahoo.in",       "9876500003",       "Mumbai",    "CARD",       "VISA",    "Airtel Postpaid","MOBILE",      "1,250.00",  "INR", "2026-11-02T08:05:00", "SUCCESS", "REF1000003", "2026-11-02T08:30:00"),   // sanitize: "1,250.00"
  pay("PAY-0004", "PYR-004", "Neha Joshi",   "neha.joshi@outlook.com",    "09876500004",      "Pune",      "NETBANKING", "HDFC",    "MSEDCL",         "ELECTRICITY", 2310,        "INR", "2026-11-02T19:20:00", "SUCCESS", "REF1000004", "2026-11-02T19:21:00"),   // sanitize: phone with 0
  pay("PAY-0005", "PYR-005", "Karan Desai",  "karan.desai@gmail.com",     "9876500005",       "Vadodara",  "WALLET",     "PAYTM",   "Tata Play",      "DTH",         450,         "INR", "2026-11-03T10:00:00", "FAILED",  "REF1000005", "2026-11-03T10:01:00"),
  pay("PAY-0006", "PYR-001", "Rahul Patel",  "rahul.patel@gmail.com",     "9876500001",       "Ahmedabad", "UPI",        "GPAY",    "Jio Fiber",      "BROADBAND",   999,         "INR", "2026-11-04T07:30:00", "SUCCESS", "REF1000006", "2026-11-04T07:31:00"),
  pay("PAY-0007", "PYR-006", "Sneha Iyer",   "sneha.iyer@gmail.com",      "9876500006",       "Bengaluru", "CARD",       "MASTERCARD","BESCOM",       "ELECTRICITY", 3120.75,     "INR", "2026-11-05T21:10:00", "PENDING", "REF1000007", "2026-11-05T21:11:00"),   // incremental: PENDING -> SUCCESS
  pay("PAY-0008", "PYR-007", "Vikram Singh", "vikram.singh@gmail.com",    "9876500007",       "Jaipur",    "UPI",        "PHONEPE", "Airtel Postpaid","MOBILE",      599,         "INR", "2026-11-06T12:00:00", "SUCCESS", "REF1000008", "2026-11-06T12:01:00"),
  pay("PAY-0009", "PYR-008", "Pooja Trivedi","pooja.trivedi@gmail.com",   "9876500008",       "Rajkot",    "NETBANKING", "SBI",     "LIC Premium",    "INSURANCE",   8500,        "INR", "2026-11-07T15:45:00", "SUCCESS", "REF1000009", "2026-11-07T15:46:00"),
  pay("PAY-0010", "PYR-009", "Arjun Nair",   "arjun.nair@gmail.com",      "9876500009",       "Kochi",     "CARD",       "VISA",    "KSEB",           "ELECTRICITY", 1780,        "INR", "2026-11-08T09:00:00", "SUCCESS", "REF1000010", "2026-11-08T09:01:00"),
  pay("PAY-0011", "PYR-002", "Priya Shah",   "priya.shah@gmail.com",      "9876500002",       "Surat",     "UPI",        "PHONEPE", "Gujarat Gas",    "GAS",         640,         "INR", "2026-11-09T18:30:00", "SUCCESS", "REF1000011", "2026-11-09T18:31:00"),
  pay("PAY-0012", "PYR-003", "Amit Mehta",   "amit.mehta@yahoo.in",       null,               "Mumbai",    "UPI",        "GPAY",    "Tata Play",      "DTH",         350,         "USD", "2026-11-10T08:15:00", "SUCCESS", null,         "2026-11-10T08:16:00"),   // optional fields missing: still valid
  pay("PAY-0019", "PYR-008", "Pooja Trivedi","pooja.trivedi@gmail.com",   "9876500008",       "Rajkot",    "NETBANKING", "SBI",     "LIC Premium",    "INSURANCE",   15000,       "INR", "2026-11-11T10:00:00", "SUCCESS", "REF1000019", "2026-11-11T10:01:00"),   // WARNING: amount > 12,000 (loaded)

  // ---------------- DUPLICATE: same paymentId, older updatedAt -> rejected, latest kept ----------------
  pay("PAY-0003", "PYR-003", "Amit Mehta",   "amit.mehta@yahoo.in",       "9876500003",       "Mumbai",    "CARD",       "VISA",    "Airtel Postpaid","MOBILE",      1250,        "INR", "2026-11-02T08:05:00", "PENDING", "REF1000003", "2026-11-02T08:06:00"),

  // ---------------- bad records ----------------
  pay("PAY-0013", "PYR-004", "Neha Joshi",   "neha.joshi@outlook.com",    "9876500004",       "Pune",      "UPI",        "GPAY",    "MSEDCL",         "ELECTRICITY", "12abc",     "INR", "2026-11-12T09:00:00", "SUCCESS", "REF1000013", "2026-11-12T09:01:00"),   // CORRUPT amount (fixed in incremental)
  pay("PAY-0014", "PYR-005", "Karan Desai",  "karan.desai@gmail.com",     "9876500005",       "Vadodara",  "CARD",       "VISA",    "Tata Play",      "DTH",         450,         "INR", "2026-02-30T10:00:00", "SUCCESS", "REF1000014", "2026-11-12T10:01:00"),   // CORRUPT date (30 Feb)
  pay("PAY-0015", "PYR-006", "Sneha Iyer",   "sneha.iyer@gmail.com",      "9876500006",       "Bengaluru", "UPI",        "GPAY",    "BESCOM",         "ELECTRICITY", -500,        "INR", "2026-11-12T11:00:00", "SUCCESS", "REF1000015", "2026-11-12T11:01:00"),   // VALIDATION amount <= 0
  pay("PAY-0016", "PYR-007", "Vikram Singh", "vikram.singh@gmail.com",    "9876500007",       "Jaipur",    "CHEQUE",     "SBI",     "LIC Premium",    "INSURANCE",   2000,        "INR", "2026-11-12T12:00:00", "SUCCESS", "REF1000016", "2026-11-12T12:01:00"),   // VALIDATION method (deleted in incremental)
  pay("PAY-0017", "PYR-009", "Arjun Nair",   "arjun@@mail",               "9876500009",       "Kochi",     "UPI",        "GPAY",    "KSEB",           "ELECTRICITY", 900,         "INR", "2026-11-12T13:00:00", "SUCCESS", "REF1000017", "2026-11-12T13:01:00"),   // VALIDATION email
  pay("PAY-0020", "PYR-010", null,           "unknown@gmail.com",         "9876500010",       "Delhi",     "UPI",        "GPAY",    "BSES",           "ELECTRICITY", 1100,        "INR", "2026-11-12T14:00:00", "SUCCESS", "REF1000020", "2026-11-12T14:01:00"),   // VALIDATION payerName missing

  // ---------------- test document: skipped by the source filter (isTest = true), never read ----------------
  pay("PAY-0018", "PYR-999", "QA Tester",    "qa@demo.com",               "9999999999",       "Test",      "UPI",        "GPAY",    "Test Biller",    "TEST",        1,           "INR", "2026-11-12T15:00:00", "SUCCESS", "REF-TEST",   "2026-11-12T15:01:00", { isTest: true })
]);

// Indexes: business key + flag (INCREMENTAL reads {processedFlag: "N"/missing} -> index makes the $match fast)
db.payments.createIndex({ paymentId: 1 });
db.payments.createIndex({ processedFlag: 1 });

print(`\n✔ ${DB_NAME}.payments: ${db.payments.countDocuments()} documents (expected 21, of which 1 test document)`);
print("  Next: POST /api/etl/pipelines/payment-pipeline/run?loadType=FULL");
