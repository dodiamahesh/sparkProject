// =============================================================================
//  01_mongo_full_load_data.js
//  MongoDB: database `shopdb` – 9 collections + FULL LOAD sample data
//  (ETL_Jobs_Data_Design_Guide.md, Section 2)
//
//  Kevi rite chalavvu:
//      mongosh "mongodb://localhost:27017" 01_mongo_full_load_data.js
//
//  Aa script shu kare che:
//      1. shopdb ma 9 collections DROP kari ne farithi banave che (re-run safe)
//      2. Darek collection ma Section 2 no data insert kare che
//         (saro data + jaanibujhi ne mukelo kharab data)
//      3. Watermark ane business key par index banave che
//      4. Chhelle darek collection no count print kare che
//
//  Pachhi Job-1 thi Job-4 no FULL LOAD run (2026-10-21 00:00) chalavo.
//  Pachhi 02_mongo_incremental_0400_data.js chalavo, ane incremental run (04:00) karo.
//
//  Data type niyam:
//    - Timestamps (updated_at, order_ts, paid_at …) → BSON Date (ISODate).
//      Value IST ma lakhi che (+05:30). Mongo ene UTC ma store kare che.
//    - Date-only fields (signup_date, promised_date, review_date) → "yyyy-MM-dd" string
//    - chat_tickets: startedAt / updatedAt → UTC STRING ("...Z")
//      (biji system, Section 11.4 nu timezone trap)
//    - call_logs: call_time → "dd/MM/yyyy HH:mm" STRING (IST)
//    - Kharab values (price "abc", order_ts "2026-13-45", rating "five" …) STRING
//      j rakhi che. Etle ek j field ma number/Date ane string banne che.
//      Spark ma aava fields ne String tarike vancho, pachhi parse karo (corrupt check).
// =============================================================================

db = db.getSiblingDB("shopdb");

// ---------- helpers ----------
const ist = (s) => new Date(s.replace(" ", "T") + ":00+05:30");   // "2026-10-01 09:00" (IST)
const addMin = (d, m) => new Date(d.getTime() + m * 60000);
const utcStr = (d) => d.toISOString().replace(".000Z", "Z");

const COLLECTIONS = ["products", "customers", "orders", "payments", "shipments",
                     "reviews", "email_tickets", "chat_tickets", "call_logs"];
COLLECTIONS.forEach(c => { db.getCollection(c).drop(); db.createCollection(c); });
print("✔ 9 collections created in shopdb");

// =============================================================================
// 1) products (24 docs)  → Job-1
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
// 2) customers (23 docs)  → Job-2
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
// 3) orders (24 docs)  → Job-2 (driving) ane Job-4 (lookup)
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
// 4) payments (23 docs)  → Job-2
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

// =============================================================================
// 5) shipments (24 docs)  → Job-4
//    { shipment_id, order_no, courier, dispatched_at, promised_date, delivered_at, status, updated_at }
// =============================================================================
function ship(id, orderNo, courier, dispatched, promised, delivered, status, updatedAt) {
  const d = ist(dispatched + " 18:00");
  const dl = delivered ? ist(delivered + " 14:00") : null;
  return { shipment_id: id, order_no: orderNo, courier, dispatched_at: d, promised_date: promised,
           delivered_at: dl, status, updated_at: updatedAt || dl || d };
}

db.shipments.insertMany([
  ship("SH-001A", "O1001", "SpeedEx",    "2026-10-01", "2026-10-04", "2026-10-03", "DELIVERED"),
  ship("SH-001B", "O1001", "SpeedEx",    "2026-10-02", "2026-10-05", "2026-10-04", "DELIVERED"),   // 2nd package
  ship("SH-002",  "O1002", "QuickMove",  "2026-10-02", "2026-10-05", "2026-10-04", "DELIVERED"),
  ship("SH-003",  "O1003", "speedex",    "2026-10-02", "2026-10-05", "2026-10-06", "DELIVERED"),   // 🧹 courier lowercase
  ship("SH-005",  "O1005", "EcoDeliver", "2026-10-04", "2026-10-07", "2026-10-06", "DELIVERED"),   // latest
  ship("SH-006",  "O1006", "QuickMove",  "2026-10-05", "2026-10-08", "2026-10-07", "DELIVERED"),
  ship("SH-007",  "O1007", "SpeedEx",    "2026-10-05", "2026-10-08", "2026-10-07", "DELIVERED"),
  ship("SH-008",  "O1008", "EcoDeliver", "2026-10-06", "2026-10-09", "2026-10-08", "RETURNED", ist("2026-10-09 12:00")),
  ship("SH-009",  "O1009", "QuickMove",  "2026-10-06", "2026-10-09", "2026-10-08", "DELIVERED"),
  ship("SH-011",  "O1011", "SpeedEx",    "2026-10-08", "2026-10-11", "2026-10-10", "DELIVERED"),
  ship("SH-012",  "O1012", "SpeedEx",    "2026-10-09", "2026-10-12", "2026-10-13", "DELIVERED"),   // late
  ship("SH-013",  "O1013", "EcoDeliver", "2026-10-09", "2026-10-12", "2026-10-12", "DELIVERED"),
  ship("SH-014A", "O1014", "QuickMove",  "2026-10-10", "2026-10-13", "2026-10-12", "DELIVERED"),
  ship("SH-014B", "O1014", "QUICKMOVE ", "2026-10-11", "2026-10-14", "2026-10-16", "DELIVERED"),   // 🧹 courier case + space
  ship("SH-015",  "O1015", "SpeedEx",    "2026-10-12", "2026-10-15", null,         "IN_TRANSIT"),  // promise date vitavi gai
  ship("SH-016",  "O1016", "EcoDeliver", "2026-10-12", "2026-10-15", "2026-10-17", "DELIVERED"),   // late
  ship("SH-017",  "O1017", "QuickMove",  "2026-10-14", "2026-10-17", "2026-10-16", "DELIVERED"),
  ship("SH-019A", "O1019", "SpeedEx",    "2026-10-15", "2026-10-18", "2026-10-17", "DELIVERED"),
  ship("SH-019B", "O1019", "SpeedEx",    "2026-10-16", "2026-10-19", "2026-10-18", "DELIVERED"),
  ship("SH-020",  "O1020", "EcoDeliver", "2026-10-17", "2026-10-20", "2026-10-19", "DELIVERED"),
  ship("SH-005",  "O1005", "EcoDeliver", "2026-10-04", "2026-10-07", null,         "IN_TRANSIT"),  // ⚠️ DUPLICATE (juno)
  ship("SH-004",  "O1004", "SpeedEx",    "2026-10-04", "2026-10-07", null,         "IN_TRANSIT"),  // ⚠️ order CANCELLED che
  ship("SH-021",  "O1099", "QuickMove",  "2026-10-18", "2026-10-21", null,         "IN_TRANSIT"),  // ⚠️ order exist nathi
  ship("SH-013B", "O1013", "EcoDeliver", "2026-10-12", "2026-10-15", "2026-10-10", "DELIVERED", ist("2026-10-12 18:00"))  // ⚠️ delivery dispatch pehla!
]);

// =============================================================================
// 6) reviews (24 docs)  → Job-4
//    { review_id, product_code, customer_code, order_no, rating, comment, review_date, updated_at }
// =============================================================================
function rev(id, productCode, custCode, orderNo, rating, comment, reviewDate, updatedAt) {
  return { review_id: id, product_code: productCode, customer_code: custCode, order_no: orderNo,
           rating, comment, review_date: reviewDate, updated_at: updatedAt || ist(reviewDate + " 19:00") };
}

db.reviews.insertMany([
  rev("RV01", "P001", "C001", "O1001", 5,     "Superb phone",                      "2026-10-05"),
  rev("RV02", "P003", "C001", "O1001", 4,     "Good sound",                        "2026-10-05"),
  rev("RV03", "P008", "C002", "O1002", 4,     "  nice fabric!!  ",                 "2026-10-06"),   // 🧹 comment trim
  rev("RV04", "P005", "C003", "O1003", 3,     "Good but heats up",                 "2026-10-08"),
  rev("RV05", "P006", "C004", "O1005", 5,     "Comfortable",                       "2026-10-08"),
  rev("RV06", "P007", "C004", "O1005", 4,     "Good fit",                          "2026-10-08"),
  rev("RV07", "P003", "C006", "O1007", 3,     "Left earbud weak, replaced now ok", "2026-10-09", ist("2026-10-12 19:00")),  // latest (edit karyu)
  rev("RV08", "P008", "C008", "O1009", 5,     "Loved it",                          "2026-10-10"),
  rev("RV09", "P006", "C008", "O1009", 4,     "Value for money",                   "2026-10-10"),
  rev("RV10", "P012", "C011", "O1011", 1,     "Arrived damaged",                   "2026-10-11"),
  rev("RV11", "P014", "C011", "O1011", "4.0", "Cooks fast",                        "2026-10-12"),   // 🧹 rating string
  rev("RV12", "P002", "C012", "O1012", 4,     "Good budget phone",                 "2026-10-14"),
  rev("RV13", "P010", "C013", "O1013", 3,     "Size slightly small",               "2026-10-13"),
  rev("RV14", "P009", "C014", "O1014", 3,     "Colour different",                  "2026-10-17"),
  rev("RV15", "P008", "C014", "O1014", 5,     "Beautiful",                         "2026-10-17"),
  rev("RV16", "P016", "C016", "O1016", 4,     "Solid build",                       "2026-10-18"),
  rev("RV17", "P004", "C017", "O1017", 2,     "Bass is low",                       "2026-10-17"),
  rev("RV18", "P001", "C019", "O1019", 5,     "Fast and smooth",                   "2026-10-18"),
  rev("RV19", "P003", "C019", "O1019", 4,     "Good",                              "2026-10-19"),
  rev("RV20", "P012", "C020", "O1020", 4,     "Boils quickly",                     "2026-10-20"),
  rev("RV07", "P003", "C006", "O1007", 2,     "Left earbud weak",                  "2026-10-09", ist("2026-10-09 19:00")),  // ⚠️ DUPLICATE (juno)
  rev("RV21", "P005", "C005", "O1006", 1,     "Not good",                          "2026-10-12"),   // ⚠️ O1006 ma P005 nathi
  rev("RV22", "P015", "C016", "O1016", 7,     "Great mat",                         "2026-10-18"),   // ⚠️ rating 1–5 bahar
  rev("RV24", "P017", "C007", "O1008", "five", "Nice bat",                         "2026-10-10")    // ⚠️ corrupt rating
]);

// =============================================================================
// 7) email_tickets (21 docs)  → Job-3
//    { ticket_ref, from_email, subject, received_at (IST), status, priority, order_no, updated_at }
// =============================================================================
function em(ref, fromEmail, subject, receivedAt, status, priority, orderNo, updatedAt) {
  return { ticket_ref: ref, from_email: fromEmail, subject, received_at: receivedAt, status, priority,
           order_no: orderNo, updated_at: updatedAt || addMin(receivedAt, 120) };
}

db.email_tickets.insertMany([
  em("EM-001", "rahul.patel@mail.com",        "Order O1001 delivery late",             ist("2026-10-02 09:10"), "closed",  "High",   "O1001", ist("2026-10-02 15:00")),
  em("EM-002", "priya.shah@mail.com",         "Kurti size issue - want exchange",      ist("2026-10-05 11:00"), "closed",  "Medium", "O1002"),
  em("EM-003", "amit.mehta@mail.com",         "Laptop not working after update",       ist("2026-10-06 14:20"), "open",    "High",   "O1003"),
  em("EM-004", "neha.joshi@mail.com",         "Payment debited twice",                 ist("2026-10-04 08:30"), "closed",  "High",   "O1005"),
  em("EM-005", "karan.desai@mail.com",        "When will my order be delivered?",      ist("2026-10-06 10:00"), "pending", "Low",    "O1006"),
  em("EM-006", "sneha.iyer@mail.com",         "Earbuds left side not working",         ist("2026-10-08 16:45"), "open",    "Medium", "O1007"),
  em("EM-007", "vikram.singh@mail.com",       "Refund for returned bat",               ist("2026-10-09 12:00"), "open",    "High",   "O1008"),
  em("EM-008", "pooja.trivedi@mail.com",      "Change delivery address",               ist("2026-10-06 09:00"), "closed",  "Low",    "O1009"),
  em("EM-009", "riya.kapoor@mail.com",        "Payment pending for card order",        ist("2026-10-08 10:30"), "pending", "High",   "O1010"),
  em("EM-010", "mehul.shah@mail.com",         "Kettle damaged in box",                 ist("2026-10-10 18:00"), "closed",  "High",   "O1011"),
  em("EM-011", "kavya.reddy@mail.com",        "Invoice copy needed",                   ist("2026-10-11 11:20"), "closed",  "Low",    "O1012"),
  em("EM-012", "rohan.gupta@mail.com",        "Shoes size exchange",                   ist("2026-10-12 13:00"), "open",    "Medium", "O1013"),
  em("EM-013", "isha.bhatt@mail.com",         "Saree colour different, quality issue", ist("2026-10-13 15:10"), "closed",  "Medium", "O1014"),
  em("EM-014", "unknown.person@mail.com",     "Forgot password",                       ist("2026-10-14 09:30"), "closed",  "Low",    null),     // customer list ma nathi (UNKNOWN)
  em("EM-015", "noreply@spam.xyz",            "WIN PRIZE NOW",                         ist("2026-10-14 03:00"), "open",    "Low",    null),     // ⚠️ spam
  em("EM-016", "ananya.das@mail.com",         "Dumbbell delivery not received",        ist("2026-10-15 10:40"), "open",    "High",   "O1016"),
  em("EM-017", "harsh.vora@mail.com",         "Speaker not working",                   ist("2026-10-16 17:30"), "pending", "Medium", "O1017"),
  em("EM-018", " NIDHI.AGARWAL@MAIL.COM ",    "Refund for cancelled order",            ist("2026-10-15 12:00"), "closed",  "Medium", "O1018"),  // 🧹 email case + space
  em("EM-019", "yash.thakkar@mail.com",       "Delivery late",                         null,                    "open",    "High",   "O1019", ist("2026-10-18 09:00")),  // ⚠️ time missing
  em("EM-020", "diya.menon@mail.com",         "Return kettle",                         ist("2026-10-18 11:15"), "open",    "Low",    "O1020"),
  em("EM-007", "vikram.singh@mail.com",       "Refund for returned bat",               ist("2026-10-09 12:00"), "open",    "High",   "O1008")   // ⚠️ DUPLICATE (email 2 vaar process)
]);

// =============================================================================
// 8) chat_tickets (21 docs)  → Job-3
//    ⚠️ Field naam alag (chatId, customerCode, startedAt) ane time UTC STRING ma
//    { chatId, customerCode, topic, startedAt, state, agent, updatedAt }
// =============================================================================
function ch(id, custCode, topic, startedAtUtc, state, agent, updatedAtUtc) {
  return { chatId: id, customerCode: custCode, topic, startedAt: startedAtUtc, state, agent,
           updatedAt: updatedAtUtc || utcStr(addMin(new Date(startedAtUtc), 25)) };
}

db.chat_tickets.insertMany([
  ch("CH-001", "C001", "upi payment failed but money deducted", "2026-10-03T12:55:00Z", "RESOLVED",    "Meena", "2026-10-03T13:20:00Z"),
  ch("CH-002", "C003", "laptop delivery status",                "2026-10-02T04:30:00Z", "RESOLVED",    "Ravi"),
  ch("CH-003", "C004", "jeans size exchange",                   "2026-10-07T07:00:00Z", "PENDING",     "Meena"),
  ch("CH-004", "C005", "mixer courier late",                    "2026-10-07T13:30:00Z", "TRANSFERRED", "Ravi"),
  ch("CH-005", "C006", "earbuds defective replace",             "2026-10-09T05:40:00Z", "RESOLVED",    "Sonal"),
  ch("CH-006", "C008", "kurti return pickup",                   "2026-10-10T09:15:00Z", "PENDING",     "Sonal"),
  ch("CH-007", "C009", "account login problem",                 "2026-10-11T03:50:00Z", "RESOLVED",    "Ravi"),
  ch("CH-008", "C010", "card payment pending",                  "2026-10-08T08:20:00Z", "TRANSFERRED", "Meena"),
  ch("CH-009", "C011", "pressure cooker quality",               "2026-10-12T10:30:00Z", "RESOLVED",    "Sonal"),
  ch("CH-010", "C012", "phone delivery tracking",               "2026-10-09T05:00:00Z", "RESOLVED",    "Ravi"),
  ch("CH-011", "C013", "shoes size issue",                      "abc",                  "PENDING",     "Meena", "2026-10-13T08:00:00Z"),  // ⚠️ corrupt time
  ch("CH-012", "C014", "saree return",                          "2026-10-14T06:10:00Z", "PENDING",     "Meena"),
  ch("CH-013", "C015", "smart watch delivery late",             "2026-10-14T12:40:00Z", "TRANSFERRED", "Ravi"),
  ch("CH-014", "C016", "yoga mat damaged",                      "2026-10-15T03:25:00Z", "RESOLVED",    "Sonal"),
  ch("CH-015", "C017", "netbanking refund",                     "2026-10-16T14:30:00Z", "PENDING",     "Meena"),
  ch("CH-016", "C018", "order cancelled refund status",         "2026-10-15T08:00:00Z", "RESOLVED",    "Ravi"),
  ch("CH-017", "C019", "thanks for quick service",              "2026-10-17T06:30:00Z", "RESOLVED",    "Sonal"),
  ch("CH-018", "C099", "where is my order",                     "2026-10-18T04:40:00Z", "PENDING",     "Meena"),  // customer list ma nathi (UNKNOWN)
  ch("CH-019", "C020", "kettle return pickup date",             "2026-10-19T04:15:00Z", "PENDING",     "Ravi"),
  ch("CH-020", "C002", "address change",                        "2026-10-19T11:50:00Z", "RESOLVED",    "Sonal"),
  ch("CH-005", "C006", "earbuds defective replace",             "2026-10-09T05:40:00Z", "RESOLVED",    "Sonal")   // ⚠️ DUPLICATE
]);

// =============================================================================
// 9) call_logs (20 docs)  → Job-3
//    ⚠️ call_time "dd/MM/yyyy HH:mm" STRING (IST), customer phone thi olakhay
//    { call_id, phone, reason, call_time, outcome, duration_sec, agent, logged_at }
// =============================================================================
function cl(id, phone, reason, callTime, outcome, durationSec, agent, loggedAt) {
  let logged = loggedAt;
  if (!logged) {                                   // "02/10/2026 11:30" → logged_at = call_time + 5 min
    const [d, t] = callTime.split(" ");
    const [dd, mm, yyyy] = d.split("/");
    logged = addMin(ist(`${yyyy}-${mm}-${dd} ${t}`), 5);
  }
  return { call_id: id, phone, reason, call_time: callTime, outcome, duration_sec: durationSec, agent, logged_at: logged };
}

db.call_logs.insertMany([
  cl("CL-001", "+91 98765 00001", "delivery late",         "02/10/2026 11:30",   "Resolved",  240, "Kiran"),  // 🧹 phone format
  cl("CL-002", "9876500003",      "laptop defective",      "07/10/2026 10:05",   "Escalated", 600, "Asha"),
  cl("CL-003", "09876500004",     "payment debited twice", "04/10/2026 09:15",   "Resolved",  180, "Kiran"),  // 🧹 aagal 0
  cl("CL-004", "9876500005",      "courier not reached",   "08/10/2026 17:40",   "Callback",  120, "Asha"),
  cl("CL-005", "9876500006",      "return earbuds",        "10/10/2026 12:20",   "Resolved",  300, "Kiran"),
  cl("CL-006", "9876500007",      "refund status",         "10/10/2026 15:00",   "Escalated", 420, "Asha"),
  cl("CL-007", "9876500008",      "address change",        "06/10/2026 08:45",   "Resolved",  150, "Kiran"),
  cl("CL-008", "9876500010",      "card payment pending",  "09/10/2026 11:00",   "Callback",  200, "Asha"),
  cl("CL-009", "9876500011",      "kettle damaged",        "11/10/2026 10:00",   "Resolved",  -30, "Kiran"),  // ⚠️ negative duration
  cl("CL-010", "9876500012",      "invoice needed",        "11/10/2026 16:30",   "Resolved",  90,  "Kiran"),
  cl("CL-011", "9876500013",      "shoes size exchange",   "12/10/2026 14:10",   "Resolved",  210, "Asha"),
  cl("CL-012", "9876500014",      "saree colour issue",    "2026/10/12 25:61",   "Resolved",  180, "Kiran", ist("2026-10-12 15:00")),  // ⚠️ corrupt time
  cl("CL-013", "9876500015",      "watch delivery late",   "15/10/2026 10:20",   "Escalated", 360, "Kiran"),
  cl("CL-014", "9876500016",      "dumbbell not received", "15/10/2026 18:05",   "Callback",  270, "Asha"),
  cl("CL-015", "9876500017",      "speaker not working",   "16/10/2026 19:30",   "Resolved",  330, "Kiran"),
  cl("CL-016", null,              null,                    "16/10/2026 20:00",   "Resolved",  45,  "Asha"),   // ⚠️ phone ane reason banne nathi
  cl("CL-017", "9876500019",      "account login issue",   "17/10/2026 09:10",   "Resolved",  120, "Asha"),
  cl("CL-018", "9876500020",      "kettle return pickup",  "19/10/2026 10:50",   "Callback",  160, "Kiran"),
  cl("CL-019", "9999999999",      "product enquiry",       "19/10/2026 13:15",   "Resolved",  60,  "Asha"),   // customer list ma nathi (UNKNOWN)
  cl("CL-020", "9876500002",      "kurti exchange status", "20/10/2026 11:40",   "Resolved",  140, "Kiran")
]);

// =============================================================================
// Indexes
//   - Watermark field par index: incremental read fast thay
//   - Business key par index NON-UNIQUE j rakhvo: data ma jaanibujhi ne duplicates che.
//     (Real system ma pan source par unique na hoy, etle j ETL ma dedupe kariye chhiye.)
// =============================================================================
db.products.createIndex({ updated_at: 1 });      db.products.createIndex({ product_code: 1 });
db.customers.createIndex({ updated_at: 1 });     db.customers.createIndex({ customer_code: 1 });
db.orders.createIndex({ updated_at: 1 });        db.orders.createIndex({ order_no: 1 });   // Job-4 key lookup mate
db.payments.createIndex({ updated_at: 1 });      db.payments.createIndex({ payment_id: 1 });
db.shipments.createIndex({ updated_at: 1 });     db.shipments.createIndex({ shipment_id: 1 });
db.reviews.createIndex({ updated_at: 1 });       db.reviews.createIndex({ review_id: 1 });
db.email_tickets.createIndex({ updated_at: 1 }); db.email_tickets.createIndex({ ticket_ref: 1 });
db.chat_tickets.createIndex({ updatedAt: 1 });   db.chat_tickets.createIndex({ chatId: 1 });
db.call_logs.createIndex({ logged_at: 1 });      db.call_logs.createIndex({ call_id: 1 });

// =============================================================================
// Verify: counts (Section 2.1 sathe match thava joie)
// =============================================================================
const EXPECTED = { products: 24, customers: 23, orders: 24, payments: 23, shipments: 24,
                   reviews: 24, email_tickets: 21, chat_tickets: 21, call_logs: 20 };
print("\n Collection        Docs  Expected");
print(" ----------------  ----  --------");
let total = 0;
COLLECTIONS.forEach(c => {
  const n = db.getCollection(c).countDocuments();
  total += n;
  print(` ${c.padEnd(16)}  ${String(n).padStart(4)}  ${String(EXPECTED[c]).padStart(8)}  ${n === EXPECTED[c] ? "✔" : "✘"}`);
});
print(` ${"TOTAL".padEnd(16)}  ${String(total).padStart(4)}  ${String(204).padStart(8)}`);
print("\n✔ Full load data ready. Have Job-1 → Job-2 → (Job-3, Job-4) → Job-5 no FULL LOAD run karo (2026-10-21 00:00).");
