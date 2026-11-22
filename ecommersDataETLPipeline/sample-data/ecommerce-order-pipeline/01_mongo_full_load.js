// =============================================================================
//  ecommerce-order-pipeline  |  01_mongo_full_load.js  |  MongoDB sample data for the FIRST run
//
//  Run:   mongosh "mongodb://localhost:27017" sample-data/ecommerce-order-pipeline/01_mongo_full_load.js
//  Then:  POST http://localhost:8080/api/etl/pipelines/ecommerce-order-pipeline/run   (first run: no watermark yet, reads everything)
//
//  Database: bronze_db (default MONGO_DATABASE). Collections ecom_* do not clash with the sales job.
//
//  SNOWFLAKE: ecom_categories -> dim_category <- ecom_products -> dim_product (category must exist)
//             ecom_customers  -> dim_ecom_customer
//             ecom_orders     -> fact_order       (customer must exist)
//             ecom_order_items-> fact_order_item  (order + product must exist)
//
//  EXPECTED FIRST RUN:
//      ecom_categories   read 6  | valid 5 | rejected 1 (VALIDATION CAT-06 department TOYS)
//      ecom_products     read 11 | valid 8 | rejected 3 (DUPLICATE PRD-0002, ORPHAN PRD-0009, VALIDATION PRD-0010)
//      ecom_customers    read 8  | valid 6 | rejected 2 (VALIDATION ECU-0007 email, ECU-0008 segment)
//      ecom_orders       read 11 | valid 8 | rejected 3 (DUPLICATE ORD-00003, ORPHAN ORD-00009, CORRUPT ORD-00010)
//      ecom_order_items  read 15 | valid 12| rejected 3 (ORPHAN OIT-000013 product, ORPHAN OIT-000014 order, CORRUPT OIT-000015 qty)
//      status: SUCCESS_WITH_FAILED (valid rows loaded, the rejected ones are in meta.meta_rejected_record)
// =============================================================================
const DB_NAME = "bronze_db";
db = db.getSiblingDB(DB_NAME);

// updatedAt = the INCREMENTAL WATERMARK field: it must be a BSON Date (IST time written here, MongoDB stores UTC)
const ist = (s) => new Date(s + "+05:30");   // "2026-11-01T09:16:00" (IST) -> Date


const COLLECTIONS = ["ecom_categories", "ecom_products", "ecom_customers", "ecom_orders", "ecom_order_items"];
COLLECTIONS.forEach(c => db.getCollection(c).drop());

// ---------------------------------------------------------------- ecom_categories (6)
const cat = (id, name, dept) => ({ categoryId: id, categoryName: name, department: dept, updatedAt: ist("2026-10-01T09:00:00") });
db.ecom_categories.insertMany([
  cat("CAT-01", " mobile phones ", "electronics"),   // sanitize: trim, Proper Case, UPPER
  cat("CAT-02", "Laptops",         "ELECTRONICS"),
  cat("CAT-03", "Men's Clothing",  "FASHION"),
  cat("CAT-04", "Kitchen",         "HOME"),
  cat("CAT-05", "Fiction Books",   "BOOKS"),
  cat("CAT-06", "Soft Toys",       "TOYS")           // VALIDATION department not allowed (deleted in incremental)
]);

// ---------------------------------------------------------------- ecom_products (11)
function prd(id, sku, name, catId, brand, list, cost, status, launch, updatedAt) {
  return { productId: id, sku, productName: name, categoryId: catId, brand, listPrice: list, costPrice: cost,
           status, launchDate: launch, updatedAt: ist(updatedAt || "2026-10-02T09:00:00") };
}
db.ecom_products.insertMany([
  prd("PRD-0001", "SAM-0001", "Galaxy S24",          "CAT-01", "Samsung", 74999,       44853.88, "ACTIVE",       "2024-01-24"),
  prd("PRD-0002", "APP-0002", "iPhone 15",           "CAT-01", "Apple",   "79,900.00", 58737.68, "ACTIVE",       "2023-09-22", "2026-10-03T09:00:00"),   // sanitize "79,900.00"; latest
  prd("PRD-0003", "DEL-0003", "Inspiron 14",         "CAT-02", "Dell",    56990,       41000,    "active",       "2024-03-10"),   // sanitize status
  prd("PRD-0004", "LEV-0004", "Slim Fit Jeans",      "CAT-03", "Levis",   2999,        1200,     "ACTIVE",       "2023-05-01"),
  prd("PRD-0005", "PRS-0005", "Pressure Cooker 5L",  "CAT-04", "Prestige",1899,        950,      "OUT_OF_STOCK", "2022-08-15"),
  prd("PRD-0006", "PEN-0006", "The Alchemist",       "CAT-05", "Penguin", 299,         120,      "ACTIVE",       "2020-01-01"),
  prd("PRD-0007", "HP-0007",  "Pavilion 15",         "CAT-02", "HP",      64990,       50000,    "DISCONTINUED", "2022-02-02"),
  prd("PRD-0008", null,       "Cotton Kurta",        "CAT-03", null,      1499,        600,      "ACTIVE",       "2024-06-06"),
  prd("PRD-0002", "APP-0002", "iPhone 15",           "CAT-01", "Apple",   82900,       58737.68, "ACTIVE",       "2023-09-22", "2026-09-01T09:00:00"),   // DUPLICATE (older)
  prd("PRD-0009", "TOY-0009", "Teddy Bear",          "CAT-06", "Archies", 799,         300,      "ACTIVE",       "2024-01-01"),   // ORPHAN: CAT-06 was rejected
  prd("PRD-0010", "MIX-0010", "Mixer Grinder",       "CAT-04", "Bajaj",   2499,        3100,     "ACTIVE",       "2023-03-03")    // VALIDATION cost > list
]);

// ---------------------------------------------------------------- ecom_customers (8)
function ecu(id, first, last, email, phone, city, state, pincode, segment, signup) {
  return { customerId: id, firstName: first, lastName: last, email, phone, city, state, pincode, segment,
           signupDate: signup, updatedAt: ist("2026-10-04T09:00:00") };
}
db.ecom_customers.insertMany([
  ecu("ECU-0001", "Meera",  "Iyer",   "meera.iyer@yahoo.in",      "7889079928",     "Surat",     "Gujarat",     "395007", "REGULAR", "2023-06-18"),
  ecu("ECU-0002", "karan",  "verma",  "Karan.Verma@Outlook.com",  "+91 60397 51913","mumbai",    "maharashtra", "400053", "prime",   "2023-07-05"),   // sanitize
  ecu("ECU-0003", "Aditi",  "Sharma", "aditi.sharma@gmail.com",   "9812345670",     "Delhi",     "Delhi",       "110001", "VIP",     "2022-01-11"),
  ecu("ECU-0004", "Rohan",  "Das",    "rohan.das@gmail.com",      "9733001122",     "Kolkata",   "West Bengal", "700001", "REGULAR", "2024-02-20"),
  ecu("ECU-0005", "Sana",   "Ali",    "sana.ali@gmail.com",       "9900112233",     "Bengaluru", "Karnataka",   "560001", "PRIME",   "2023-11-30"),
  ecu("ECU-0006", "Varun",  "Reddy",  "varun.reddy@gmail.com",    "9848022338",     "Hyderabad", "Telangana",   null,     "REGULAR", "2024-05-05"),
  ecu("ECU-0007", "Bad",    "Email",  "bad.email",                "9876500007",     "Pune",      "Maharashtra", "411001", "REGULAR", "2024-06-01"),   // VALIDATION email
  ecu("ECU-0008", "Gold",   "Member", "gold.member@gmail.com",    "9876500008",     "Jaipur",    "Rajasthan",   "302001", "GOLD",    "2024-06-02")    // VALIDATION segment
]);

// ---------------------------------------------------------------- ecom_orders (11)
function ord(id, cust, date, status, mode, channel, coupon, fee, city, state, updatedAt) {
  return { orderId: id, customerId: cust, orderDate: date, orderStatus: status, paymentMode: mode, channel,
           couponCode: coupon, shippingFee: fee, shippingCity: city, shippingState: state, updatedAt: ist(updatedAt || date) };
}
db.ecom_orders.insertMany([
  ord("ORD-00001", "ECU-0001", "2026-11-01T10:00:00", "DELIVERED", "UPI",        "WEB",     "DIWALI10", 0,    "Surat",     "Gujarat"),
  ord("ORD-00002", "ECU-0002", "2026-11-02T11:30:00", "SHIPPED",   "card",       "android", null,       49,   "mumbai",    "maharashtra"),   // sanitize; incremental -> DELIVERED
  ord("ORD-00003", "ECU-0003", "2026-11-03T12:00:00", "DELIVERED", "NETBANKING", "IOS",     "FIRST50",  "99", "Delhi",     "Delhi", "2026-11-04T08:00:00"),   // latest
  ord("ORD-00004", "ECU-0004", "2026-11-04T09:15:00", "PLACED",    "COD",        "WEB",     null,       49,   "Kolkata",   "West Bengal"),
  ord("ORD-00005", "ECU-0005", "2026-11-05T19:45:00", "CONFIRMED", "WALLET",     "ANDROID", "FREESHIP", 0,    "Bengaluru", "Karnataka"),
  ord("ORD-00006", "ECU-0006", "2026-11-06T14:20:00", "CANCELLED", "UPI",        "WEB",     null,       49,   "Hyderabad", "Telangana"),
  ord("ORD-00007", "ECU-0001", "2026-11-07T16:00:00", "DELIVERED", "CARD",       "IOS",     null,       0,    "Surat",     "Gujarat"),
  ord("ORD-00008", "ECU-0003", "2026-11-08T20:10:00", "RETURNED",  "UPI",        "WEB",     "BOGUS99",  49,   "Delhi",     "Delhi"),
  ord("ORD-00003", "ECU-0003", "2026-11-03T12:00:00", "PLACED",    "NETBANKING", "IOS",     "FIRST50",  "99", "Delhi",     "Delhi", "2026-11-03T12:01:00"),   // DUPLICATE (older)
  ord("ORD-00009", "ECU-0007", "2026-11-09T10:00:00", "PLACED",    "UPI",        "WEB",     null,       49,   "Pune",      "Maharashtra"),   // ORPHAN: ECU-0007 was rejected
  ord("ORD-00010", "ECU-0004", "2026-13-01T10:00:00", "PLACED",    "UPI",        "WEB",     null,       49,   "Kolkata",   "West Bengal", "2026-11-10T10:00:00")   // CORRUPT orderDate (deleted in incremental)
]);

// ---------------------------------------------------------------- ecom_order_items (15)
const oit = (id, order, product, qty, price, discount, updatedAt) => ({ orderItemId: id, orderId: order, productId: product,
  quantity: qty, unitPrice: price, discountAmount: discount, updatedAt: ist(updatedAt || "2026-11-10T12:00:00") });
db.ecom_order_items.insertMany([
  oit("OIT-000001", "ORD-00001", "PRD-0001", 1, 74999,      7499.90),
  oit("OIT-000002", "ORD-00001", "PRD-0006", 2, 299,        0),
  oit("OIT-000003", "ORD-00002", "PRD-0003", 1, "56,990",   null),      // sanitize: separator, missing discount -> 0
  oit("OIT-000004", "ORD-00003", "PRD-0002", 1, 79900,      5000),
  oit("OIT-000005", "ORD-00004", "PRD-0005", 1, 1899,       0),
  oit("OIT-000006", "ORD-00004", "PRD-0004", 2, 2999,       300),
  oit("OIT-000007", "ORD-00005", "PRD-0007", 1, 64990,      0),
  oit("OIT-000008", "ORD-00006", "PRD-0008", 3, 1499,       0),
  oit("OIT-000009", "ORD-00007", "PRD-0004", 1, 2999,       0),
  oit("OIT-000010", "ORD-00007", "PRD-0006", 5, 299,        100),
  oit("OIT-000011", "ORD-00008", "PRD-0001", 1, 74999,      0),
  oit("OIT-000012", "ORD-00008", "PRD-0005", 1, 1899,       800),
  oit("OIT-000013", "ORD-00002", "PRD-0009", 1, 799,        0),         // ORPHAN: PRD-0009 rejected
  oit("OIT-000014", "ORD-00010", "PRD-0004", 1, 2999,       0),         // ORPHAN: ORD-00010 rejected (deleted in incremental)
  oit("OIT-000015", "ORD-00005", "PRD-0006", "2.5", 299,    0)          // CORRUPT quantity
]);

COLLECTIONS.forEach(c => db.getCollection(c).createIndex({ updatedAt: 1 }));   // watermark field
db.ecom_categories.createIndex({ categoryId: 1 });  db.ecom_products.createIndex({ productId: 1 });
db.ecom_customers.createIndex({ customerId: 1 });   db.ecom_orders.createIndex({ orderId: 1 });
db.ecom_order_items.createIndex({ orderItemId: 1 }); db.ecom_order_items.createIndex({ orderId: 1 });

print("");
COLLECTIONS.forEach(c => print(`✔ ${DB_NAME}.${c.padEnd(17)} ${db.getCollection(c).countDocuments()} documents`));
print("  expected: categories 6, products 11, customers 8, orders 11, order_items 15");
print("  Next: POST /api/etl/pipelines/ecommerce-order-pipeline/run");
