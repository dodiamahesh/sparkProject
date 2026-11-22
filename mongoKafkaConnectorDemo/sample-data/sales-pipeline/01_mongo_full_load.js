// =============================================================================
//  sales-pipeline (Job-2, J2_SALES)  |  01_mongo_full_load.js  |  MongoDB sample data for the FIRST run
//
//  Run:   mongosh "mongodb://localhost:27017/?replicaSet=rs0" sample-data/sales-pipeline/01_mongo_full_load.js
//  Check: mongosh "mongodb://localhost:27017/?replicaSet=rs0" sample-data/99_verify_bronze.js
//
//  CDC flow: sourceMongo_db (this script writes here) -> connector "mongo-cdc-sourcemongo-source"
//            -> Kafka topic "sourceMongo_db" -> mongo-cdc-subscriber (subscriber-sourcemongo.properties) -> bronze_db
//
//  Data = design document "Job-2: Multiple Collections", taken from
//  "../sparkJavaMongoConnectorAirflowLinageTheory/sample_data/01_mongo_full_load_data.js" (only the 4
//  collections of this job). Timestamps are BSON Dates written in IST (+05:30), MongoDB stores UTC.
//  The banking collections in the same database have the prefix "bank_", so "customers" / "payments" are this job's.
//
//  EXPECTED in bronze_db: products 24, customers 23, orders 24, payments 23; all _op "insert", _deleted false
//
//  ETL later (reads bronze_db), EXPECTED FIRST RUN:
//      products   read 24 | valid 20 | rejected 4 (DUPLICATE P003, VALIDATION P021 / P023, CORRUPT P022)
//      customers  read 23 | valid 20 | rejected 3 (DUPLICATE C005, VALIDATION C021 email / C022 name)
//      orders     read 24 | valid 20 | rejected 4 (DUPLICATE O1005, ORPHAN O1021, CORRUPT O1022, VALIDATION O1023)
//      order lines 32     | valid 30 | rejected 2 (O1008/line-2 qty 0, O1012/line-2 product P022)
//      payments   read 23 | valid 20 | rejected 3 (DUPLICATE PAY05, ORPHAN PAY21, CORRUPT PAY22)
//      dim_customer 20, fact_sales 30 (total 249,500), fact_payment 20
//      status: SUCCESS_WITH_FAILED (valid rows loaded, the rejected ones are in meta.meta_rejected_record)
// =============================================================================
const DB_NAME = "sourceMongo_db";   // TRANSACTION (source) database, captured by the CDC connector
db = db.getSiblingDB(DB_NAME);

// ---------- helpers ----------
const ist = (s) => new Date(s.replace(" ", "T") + ":00+05:30");   // "2026-10-01 09:00" (IST)
const addMin = (d, m) => new Date(d.getTime() + m * 60000);

const COLLECTIONS = ["products", "customers", "orders", "payments"];
// EMPTY, not drop: deleteMany sends one CDC delete event per old document (bronze: _deleted = true).
// drop() would send only a "drop" event, which the subscriber ignores (apply.drop=false), so old documents would stay live in bronze.
COLLECTIONS.forEach(c => db.getCollection(c).deleteMany({}));

// =============================================================================
// 1) products (24 docs)  → dim_product (built by this job)
//    Document shape:
//    { product_code, name, brand{code,name}, category{code,name,parent{code,name}},
//      supplier{code,name,city}, price, stock[{warehouse,qty}], is_active, updated_at }
// =============================================================================
const BRAND = { B01: "Volt", B02: "SoundMax", B03: "TechBook", B04: "UrbanWear", B05: "Ethnika",
                B06: "StepUp", B07: "KitchenPro", B08: "CookEasy", B09: "FitLife", B10: "PlayStrong" };
const PARENT = { "C-ELE": "Electronics", "C-FAS": "Fashion", "C-HOM": "Home & Kitchen", "C-SPO": "Sports" };
const CATEGORY = {
  "C-ELE-MOB": "Mobiles", "C-ELE-AUD": "Audio", "C-ELE-LAP": "Laptops", "C-ELE-WEA": "Wearables",
  "C-FAS-MEN": "Men Clothing", "C-FAS-WOM": "Women Clothing", "C-FAS-FOO": "Footwear",
  "C-HOM-KIT": "Kitchen Appliances", "C-HOM-COO": "Cookware",
  "C-SPO-FIT": "Fitness", "C-SPO-CRI": "Cricket" };
const SUPPLIER = {
  "SUP-01": ["Shree Electronics Pvt Ltd", "Ahmedabad"], "SUP-02": ["Fashion Hub Traders", "Surat"],
  "SUP-03": ["HomeNeeds Distributors", "Mumbai"],      "SUP-04": ["SportZone Wholesale", "Jalandhar"] };

const w = (warehouse, qty) => ({ warehouse, qty });
function product(code, name, brandCode, catCode, supCode, price, stock, updatedAt, brandNameRaw) {
  const parentCode = catCode ? catCode.substring(0, 5) : null;
  return {
    product_code: code,
    name: name,
    brand: { code: brandCode, name: brandNameRaw !== undefined ? brandNameRaw : BRAND[brandCode] },
    category: catCode
      ? { code: catCode, name: CATEGORY[catCode], parent: { code: parentCode, name: PARENT[parentCode] } }
      : null,
    supplier: { code: supCode, name: SUPPLIER[supCode][0], city: SUPPLIER[supCode][1] },
    price: price,
    stock: stock,
    is_active: true,
    updated_at: updatedAt
  };
}

db.products.insertMany([
  product("P001", "Smartphone Pro 5G",        "B01", "C-ELE-MOB", "SUP-01", 25000, [w("WH-AHM", 40), w("WH-MUM", 25)], ist("2026-10-01 09:00")),
  product("P002", "Smartphone Lite",          "B01", "C-ELE-MOB", "SUP-01", 12000, [w("WH-AHM", 60), w("WH-DEL", 30)], ist("2026-10-01 09:00")),
  product("P003", "Wireless Earbuds",         "B02", "C-ELE-AUD", "SUP-01", 2000,  [w("WH-AHM", 100), w("WH-MUM", 80), w("WH-DEL", 50)], ist("2026-10-20 09:00")),  // latest version
  product("P004", "Bluetooth Speaker",        "B02", "C-ELE-AUD", "SUP-01", 3500,  [w("WH-MUM", 45)], ist("2026-10-02 09:00")),
  product("P005", "Laptop 14 inch",           "B03", "C-ELE-LAP", "SUP-01", 55000, [w("WH-AHM", 10), w("WH-DEL", 8)], ist("2026-10-02 09:00")),
  product("P006", "Cotton T-Shirt",           "B04", "C-FAS-MEN", "SUP-02", 500,   [w("WH-AHM", 200), w("WH-MUM", 150)], ist("2026-10-03 09:00")),
  product("P007", "Denim Jeans",              "B04", "C-FAS-MEN", "SUP-02", 1500,  [w("WH-AHM", 90)], ist("2026-10-03 09:00"), "urbanwear"),  // 🧹 brand name lowercase
  product("P008", "Printed Kurti",            "B05", "C-FAS-WOM", "SUP-02", 800,   [w("WH-AHM", 120), w("WH-DEL", 60)], ist("2026-10-04 09:00")),
  product("P009", "Silk Saree",               "B05", "C-FAS-WOM", "SUP-02", 4500,  [w("WH-MUM", 20)], ist("2026-10-04 09:00")),
  product("P010", "Running Shoes",            "B06", "C-FAS-FOO", "SUP-02", 2800,  [w("wh-ahm ", 35), w("WH-MUM", 40), w("WH-DEL", 25)], ist("2026-10-05 09:00")),  // 🧹 warehouse code
  product("P011", "Mixer Grinder",            "B07", "C-HOM-KIT", "SUP-03", 3500,  [w("WH-MUM", 30)], ist("2026-10-05 09:00")),
  product("P012", "Electric Kettle",          "B07", "C-HOM-KIT", "SUP-03", "₹1,200", [w("WH-AHM", 50), w("WH-MUM", 50)], ist("2026-10-06 09:00")),  // 🧹 price string
  product("P013", "  non-stick tawa ",        "B08", "C-HOM-COO", "SUP-03", 700,   [w("WH-AHM", 80)], ist("2026-10-06 09:00")),  // 🧹 name spaces + lowercase
  product("P014", "Pressure Cooker 5L",       "B08", "C-HOM-COO", "SUP-03", 1800,  [w("WH-DEL", 40)], ist("2026-10-07 09:00")),
  product("P015", "Yoga Mat",                 "B09", "C-SPO-FIT", "SUP-04", 600,   [w("WH-AHM", 70), w("WH-DEL", 70)], ist("2026-10-07 09:00")),
  product("P016", "Dumbbell Set 10kg",        "B09", "C-SPO-FIT", "SUP-04", 2200,  [w("WH-AHM", 15), w("WH-MUM", -5)], ist("2026-10-08 09:00")),  // ⚠️ line qty -5
  product("P017", "Cricket Bat",              "B10", "C-SPO-CRI", "SUP-04", 1800,  [w("WH-AHM", 25), w("WH-DEL", 30)], ist("2026-10-08 09:00")),
  product("P018", "Cricket Ball (Pack of 6)", "B10", "C-SPO-CRI", "SUP-04", 900,   [w("WH-AHM", 60)], ist("2026-10-09 09:00")),
  product("P019", "Smart Watch",              "B01", "C-ELE-WEA", "SUP-01", 4000,  [w("WH-AHM", 30), w("WH-DEL", 20)], ist("2026-10-09 09:00")),
  product("P020", "Sports Water Bottle",      "B09", "C-SPO-FIT", "SUP-04", 350,   [w("WH-DEL", 90)], ist("2026-10-10 09:00")),
  product("P003", "Wireless Earbuds",         "B02", "C-ELE-AUD", "SUP-01", 1900,  [w("WH-AHM", 120)], ist("2026-10-05 09:00")),  // ⚠️ DUPLICATE (juno version)
  product("P021", "Gym Gloves",               "B09", null,        "SUP-04", 450,   [w("WH-AHM", 40)], ist("2026-10-11 09:00")),  // ⚠️ category missing
  product("P022", "Steel Lunch Box",          "B08", "C-HOM-COO", "SUP-03", "abc", [w("WH-AHM", 30)], ist("2026-10-11 09:00")),  // ⚠️ corrupt price
  product("P023", "Travel Bag",               "B04", "C-FAS-MEN", "SUP-02", -150,  [w("WH-MUM", 10)], ist("2026-10-12 09:00"))   // ⚠️ negative price
]);

// =============================================================================
// 2) customers (23 docs)  → this job
//    { customer_code, name, email, phone, address{city,state,pincode}, tier, signup_date, updated_at }
// =============================================================================
function cust(code, name, email, phone, city, state, pincode, tier, signup, updatedAt) {
  return { customer_code: code, name, email, phone, address: { city, state, pincode },
           tier, signup_date: signup, updated_at: updatedAt };
}

db.customers.insertMany([
  cust("C001", "Rahul Patel",   "rahul.patel@mail.com",       "9876500001",      "Ahmedabad", "Gujarat",        "380001", "Gold",   "2025-01-10", ist("2026-10-01 10:00")),
  cust("C002", "Priya Shah",    "  PRIYA.SHAH@MAIL.COM ",     "9876500002",      "Surat",     "Gujarat",        "395003", "Silver", "2025-02-14", ist("2026-10-01 10:00")),  // 🧹 email
  cust("C003", "Amit Mehta",    "amit.mehta@mail.com",        "9876500003",      "Mumbai",    "Maharashtra",    "400001", "Gold",   "2025-03-05", ist("2026-10-01 10:00")),
  cust("C004", "Neha Joshi",    "neha.joshi@mail.com",        "+91-98765 00004", "Pune",      "Maharashtra",    "411001", "Bronze", "2025-04-20", ist("2026-10-01 10:00")),  // 🧹 phone
  cust("C005", "Karan Desai",   "karan.desai@mail.com",       "9876500005",      "Vadodara",  "Gujarat",        "390001", "Silver", "2025-05-11", ist("2026-10-02 10:00")),  // latest
  cust("C006", "Sneha Iyer",    "sneha.iyer@mail.com",        "9876500006",      "Bengaluru", "Karnataka",      "560001", "Gold",   "2025-06-01", ist("2026-10-02 10:00")),
  cust("C007", "Vikram Singh",  "vikram.singh@mail.com",      "9876500007",      "Jaipur",    "RJ",             "302001", "Bronze", "2025-07-19", ist("2026-10-02 10:00")),  // 🧹 state code
  cust("C008", "Pooja Trivedi", "pooja.trivedi@mail.com",     "9876500008",      "Rajkot",    "Gujarat",        "360001", "Silver", "2025-08-08", ist("2026-10-03 10:00")),
  cust("C009", "Arjun Nair",    "arjun.nair@mail.com",        "9876500009",      "Kochi",     "Kerala",         "682001", "Bronze", "2025-09-25", ist("2026-10-03 10:00")),  // koi order nathi
  cust("C010", "Riya Kapoor",   "riya.kapoor@mail.com",       "9876500010",      "Delhi",     "Delhi",          "110001", "Gold",   "2025-10-30", ist("2026-10-03 10:00")),
  cust("C011", "Mehul Shah",    "mehul.shah@mail.com",        "9876500011",      "Ahmedabad", "Gujarat",        "380001", null,     "2025-11-12", ist("2026-10-04 10:00")),  // 🧹 tier null
  cust("C012", "Kavya Reddy",   "kavya.reddy@mail.com",       "9876500012",      "Hyderabad", "Telangana",      "500001", "Silver", "2025-12-03", ist("2026-10-04 10:00")),
  cust("C013", "Rohan Gupta",   "rohan.gupta@mail.com",       "9876500013",      "lucknow ",  "Uttar Pradesh",  "226001", "Bronze", "2026-01-15", ist("2026-10-04 10:00")),  // 🧹 city
  cust("C014", "Isha Bhatt",    "isha.bhatt@mail.com",        "9876500014",      "Bhavnagar", "Gujarat",        "364001", "Gold",   "2026-02-08", ist("2026-10-05 10:00")),
  cust("C015", "Siddharth Rao", "siddharth.rao@mail.com",     "9876500015",      "Chennai",   "Tamil Nadu",     "600001", "Silver", "2026-03-21", ist("2026-10-05 10:00")),
  cust("C016", "Ananya Das",    "ananya.das@mail.com",        "9876500016",      "Kolkata",   "West Bengal",    "700001", "Bronze", "2026-04-02", ist("2026-10-05 10:00")),
  cust("C017", "Harsh Vora",    "harsh.vora@mail.com",        "9876500017",      "Surat",     "Gujarat",        "395003", "Silver", "2026-05-17", ist("2026-10-06 10:00")),
  cust("C018", "Nidhi Agarwal", "nidhi.agarwal@mail.com",     "9876500018",      "Indore",    "Madhya Pradesh", "452001", "Bronze", "2026-06-09", ist("2026-10-06 10:00")),
  cust("C019", "Yash Thakkar",  "yash.thakkar@mail.com",      "9876500019",      "Ahmedabad", "Gujarat",        "380001", "Gold",   "2026-07-23", ist("2026-10-06 10:00")),
  cust("C020", "Diya Menon",    "diya.menon@mail.com",        "9876500020",      "Mumbai",    "Maharashtra",    "400001", "Silver", "2026-08-30", ist("2026-10-07 10:00")),
  cust("C005", "Karan Desai",   "karan.desai@mail.com",       "9876500005",      "Vadodara",  "Gujarat",        "390001", "Bronze", "2025-05-11", ist("2026-09-01 10:00")),  // ⚠️ DUPLICATE (juno)
  cust("C021", "Test User",     "test@@mail",                 "9876500021",      "Surat",     "Gujarat",        "395003", "Bronze", "2026-09-10", ist("2026-10-08 10:00")),  // ⚠️ invalid email
  cust("C022", null,            "no.name@mail.com",           "9876500022",      "Pune",      "Maharashtra",    "411001", "Bronze", "2026-09-15", ist("2026-10-08 10:00"))   // ⚠️ name missing
]);

// =============================================================================
// 3) orders (24 docs)  → fact_sales (items[] = order lines)
//    { order_no, customer_code, order_ts, status, payment_method, items[{product_code,qty,unit_price}],
//      shipping{city,state,pincode}, order_total, updated_at }
// =============================================================================
const SHIP = {
  C001: ["Ahmedabad", "Gujarat", "380001"], C002: ["Surat", "Gujarat", "395003"], C003: ["Mumbai", "Maharashtra", "400001"],
  C004: ["Pune", "Maharashtra", "411001"],  C005: ["Vadodara", "Gujarat", "390001"], C006: ["Bengaluru", "Karnataka", "560001"],
  C007: ["Jaipur", "Rajasthan", "302001"],  C008: ["Rajkot", "Gujarat", "360001"], C009: ["Kochi", "Kerala", "682001"],
  C010: ["Delhi", "Delhi", "110001"],       C011: ["Ahmedabad", "Gujarat", "380001"], C012: ["Hyderabad", "Telangana", "500001"],
  C013: ["Lucknow", "Uttar Pradesh", "226001"], C014: ["Bhavnagar", "Gujarat", "364001"], C015: ["Chennai", "Tamil Nadu", "600001"],
  C016: ["Kolkata", "West Bengal", "700001"], C017: ["Surat", "Gujarat", "395003"], C018: ["Indore", "Madhya Pradesh", "452001"],
  C019: ["Ahmedabad", "Gujarat", "380001"], C020: ["Mumbai", "Maharashtra", "400001"], C099: ["Mumbai", "Maharashtra", "400001"] };

const it = (product_code, qty, unit_price) => ({ product_code, qty, unit_price });
function order(no, custCode, orderTs, status, paymentMethod, items, updatedAt) {
  const s = SHIP[custCode];
  return { order_no: no, customer_code: custCode, order_ts: orderTs, status, payment_method: paymentMethod, items,
           shipping: { city: s[0], state: s[1], pincode: s[2] },
           order_total: items.reduce((t, i) => t + i.qty * i.unit_price, 0),
           updated_at: updatedAt };
}

db.orders.insertMany([
  order("O1001", "C001", ist("2026-10-01 10:15"), "DELIVERED", "UPI",              [it("P001", 1, 25000), it("P003", 2, 2000)], ist("2026-10-03 14:00")),
  order("O1002", "C002", ist("2026-10-01 12:40"), "delivered", "COD",              [it("P008", 3, 800)],                        ist("2026-10-04 22:00")),  // 🧹 status lowercase
  order("O1003", "C003", ist("2026-10-02 09:05"), "DELIVERED", "CARD",             [it("P005", 1, 55000)],                      ist("2026-10-06 22:00")),
  order("O1004", "C001", ist("2026-10-03 18:20"), "CANCELLED", "UPI",              [it("P017", 1, 1800), it("P015", 1, 600)],   ist("2026-10-03 22:00")),
  order("O1005", "C004", ist("2026-10-03 20:10"), "DELIVERED", "UPI",              [it("P006", 4, 500), it("P007", 1, 1500)],   ist("2026-10-06 22:00")),  // latest
  order("O1006", "C005", ist("2026-10-04 11:00"), "DELIVERED", "CARD",             [it("P011", 1, 3500), it("P013", 2, 700)],   ist("2026-10-07 22:00")),
  order("O1007", "C006", ist("2026-10-05 15:30"), "DELIVERED", "upi ",             [it("P003", 1, 2000)],                       ist("2026-10-07 22:00")),  // 🧹 payment
  order("O1008", "C007", ist("2026-10-05 16:45"), "RETURNED",  "COD",              [it("P017", 2, 1800), it("P018", 0, 900)],   ist("2026-10-09 22:00")),  // ⚠️ line-2 qty 0
  order("O1009", "C008", ist("2026-10-06 10:00"), "DELIVERED", "UPI",              [it("P008", 2, 800), it("P006", 2, 500)],    ist("2026-10-08 22:00")),
  order("O1010", "C010", ist("2026-10-07 13:25"), "PLACED",    "CARD",             [it("P001", 1, 25000), it("P005", 1, 55000)], ist("2026-10-07 22:00")),
  order("O1011", "C011", ist("2026-10-08 09:40"), "DELIVERED", "UPI",              [it("P012", 1, 1200), it("P014", 1, 1800)],  ist("2026-10-10 22:00")),
  order("O1012", "C012", ist("2026-10-08 19:15"), "DELIVERED", "Net Banking",      [it("P002", 1, 12000), it("P022", 1, 999)],  ist("2026-10-13 22:00")),  // 🧹 payment / ⚠️ P022 dim ma nathi
  order("O1013", "C013", ist("2026-10-09 14:00"), "DELIVERED", "Cash on Delivery", [it("P010", 1, 2800)],                       ist("2026-10-12 22:00")),  // 🧹 payment
  order("O1014", "C014", ist("2026-10-10 11:30"), "DELIVERED", "CARD",             [it("P009", 1, 4500), it("P008", 1, 800)],   ist("2026-10-16 22:00")),
  order("O1015", "C015", ist("2026-10-11 17:50"), "SHIPPED",   "UPI",              [it("P019", 1, 4000)],                       ist("2026-10-12 22:00")),
  order("O1016", "C016", ist("2026-10-12 08:20"), "DELIVERED", "UPI",              [it("P016", 1, 2200), it("P015", 2, 600)],   ist("2026-10-17 22:00")),
  order("O1017", "C017", ist("2026-10-13 21:05"), "DELIVERED", "NETBANKING",       [it("P004", 1, 3500)],                       ist("2026-10-16 22:00")),
  order("O1018", "C018", ist("2026-10-14 12:10"), "CANCELLED", "COD",              [it("P020", 2, 350)],                        ist("2026-10-14 22:00")),
  order("O1019", "C019", ist("2026-10-15 10:45"), "DELIVERED", "CARD",             [it("P001", 1, 25000), it("P003", 1, 2000)], ist("2026-10-18 22:00")),
  order("O1020", "C020", ist("2026-10-16 16:30"), "DELIVERED", "UPI",              [it("P012", 2, 1200)],                       ist("2026-10-19 22:00")),
  order("O1005", "C004", ist("2026-10-03 20:10"), "PLACED",    "UPI",              [it("P006", 4, 500), it("P007", 1, 1500)],   ist("2026-10-03 22:00")),  // ⚠️ DUPLICATE (juno)
  order("O1021", "C099", ist("2026-10-17 11:00"), "PLACED",    "UPI",              [it("P003", 1, 2000)],                       ist("2026-10-17 22:00")),  // ⚠️ customer exist nathi
  order("O1022", "C009", "2026-13-45",            "PLACED",    "CARD",             [it("P015", 1, 600)],                        ist("2026-10-18 22:00")),  // ⚠️ corrupt date
  order("O1023", "C011", ist("2026-10-19 15:00"), "PLACED",    "UPI",              [],                                          ist("2026-10-19 22:00"))   // ⚠️ items nathi
]);

// =============================================================================
// 4) payments (23 docs)  → this job
//    { payment_id, order_no, method, amount, status, paid_at, updated_at }
// =============================================================================
function pay(id, orderNo, method, amount, status, paidAt, updatedAt) {
  return { payment_id: id, order_no: orderNo, method, amount, status, paid_at: paidAt, updated_at: updatedAt || paidAt };
}

db.payments.insertMany([
  pay("PAY01", "O1001", "UPI",         "₹29,000", "SUCCESS",  ist("2026-10-01 10:16")),                          // 🧹 amount string
  pay("PAY02", "O1002", "COD",         2400,      "SUCCESS",  ist("2026-10-04 14:00")),                          // COD: delivery vakhte
  pay("PAY03", "O1003", "CARD",        55000,     "SUCCESS",  ist("2026-10-02 09:06")),
  pay("PAY04", "O1004", "UPI",         2400,      "REFUNDED", ist("2026-10-03 18:21"), ist("2026-10-03 21:00")),
  pay("PAY05", "O1005", "UPI",         3500,      "SUCCESS",  ist("2026-10-03 20:11"), ist("2026-10-03 20:12")),  // latest
  pay("PAY06", "O1006", "CARD",        4900,      "SUCCESS",  ist("2026-10-04 11:01")),
  pay("PAY07", "O1007", "UPI",         2000,      "success",  ist("2026-10-05 15:31")),                          // 🧹 status lowercase
  pay("PAY08", "O1008", "COD",         3600,      "REFUNDED", ist("2026-10-07 12:00"), ist("2026-10-09 18:00")),
  pay("PAY09", "O1009", "UPI",         2600,      "SUCCESS",  ist("2026-10-06 10:01")),
  pay("PAY10", "O1010", "CARD",        80000,     "PENDING",  ist("2026-10-07 13:26")),
  pay("PAY11", "O1011", "UPI",         3000,      "SUCCESS",  ist("2026-10-08 09:41")),
  pay("PAY12", "O1012", "Net Banking", 12999,     "SUCCESS",  ist("2026-10-08 19:16")),                          // 🧹 method naam
  pay("PAY13", "O1013", "COD",         2800,      "SUCCESS",  ist("2026-10-12 12:00")),
  pay("PAY14", "O1014", "CARD",        5300,      "SUCCESS",  ist("2026-10-10 11:31")),
  pay("PAY15", "O1015", "UPI",         4000,      "SUCCESS",  ist("2026-10-11 17:51")),
  pay("PAY16", "O1016", "UPI",         3400,      "SUCCESS",  ist("2026-10-12 08:21")),
  pay("PAY17", "O1017", "NETBANKING",  3500,      "SUCCESS",  ist("2026-10-13 21:06")),
  pay("PAY18", "O1018", "COD",         700,       "FAILED",   ist("2026-10-14 18:00")),                          // cancelled COD
  pay("PAY19", "O1019", "CARD",        27000,     "SUCCESS",  ist("2026-10-15 10:46")),
  pay("PAY20", "O1020", "UPI",         2400,      "SUCCESS",  ist("2026-10-16 16:31")),
  pay("PAY05", "O1005", "UPI",         3500,      "SUCCESS",  ist("2026-10-03 20:11"), ist("2026-10-03 20:11")),  // ⚠️ DUPLICATE (callback 2 vaar)
  pay("PAY21", "O1021", "UPI",         2000,      "SUCCESS",  ist("2026-10-17 11:01")),                          // ⚠️ order reject thashe
  pay("PAY22", "O1020", "UPI",         "abc",     "FAILED",   ist("2026-10-16 16:35"))                           // ⚠️ corrupt amount
]);

// Indexes: business key + updated_at (the incremental watermark field -> fast $match on the time window)
// Business key index is NOT unique on purpose: the source really contains duplicates (the ETL removes them).
db.products.createIndex({ product_code: 1 });   db.products.createIndex({ updated_at: 1 });
db.customers.createIndex({ customer_code: 1 }); db.customers.createIndex({ updated_at: 1 });
db.orders.createIndex({ order_no: 1 });         db.orders.createIndex({ updated_at: 1 });
db.payments.createIndex({ payment_id: 1 });     db.payments.createIndex({ updated_at: 1 });

print("");
COLLECTIONS.forEach(c => print(`✔ ${DB_NAME}.${c.padEnd(10)} ${db.getCollection(c).countDocuments()} documents`));
print("  expected: products 24, customers 23, orders 24, payments 23");
print("  Next: wait until the subscriber LAG = 0, then run sample-data/99_verify_bronze.js");
