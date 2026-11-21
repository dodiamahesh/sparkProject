// =============================================================================
//  02_mongo_incremental_0400_data.js
//  MongoDB `shopdb`: 2026-10-21 00:00 thi 04:00 (IST) vacche aavelo NAVO + BADLAYELO data
//  (ETL_Jobs_Data_Design_Guide.md, Section 11.1)
//
//  Kevi rite chalavvu (01 script ane FULL LOAD run pachhi):
//      mongosh "mongodb://localhost:27017" 02_mongo_incremental_0400_data.js
//
//  Aa script ma:
//      - UPDATE: juna documents badlay che, ane emnu updated_at window ma aave che
//      - INSERT: nava documents (business key par upsert. Etle script 2 vaar chale to
//                pan duplicate nahi bane)
//      - Chhelle: window ma ketla docs aave che e print kare che
//        (Job no incremental read aa j count vanchse)
//
//  Window:  updated_at > 2026-10-21 00:00 IST  AND  <= 2026-10-21 04:00 IST
//
//  Expected incremental read:
//      products 2 | customers 3 | orders 3 | payments 2 | shipments 2 | reviews 1
//      email_tickets 2 | chat_tickets 1 | call_logs 1          → TOTAL 17
// =============================================================================

db = db.getSiblingDB("shopdb");

const ist = (s) => new Date(s.replace(" ", "T") + ":00+05:30");

// Same business key na 2 docs hoy (jem ke P003 nu duplicate), to LATEST version ne j update karo
function updateLatest(coll, keyFilter, setFields, sortField = "updated_at", arrayFilters) {
  const latest = db.getCollection(coll).find(keyFilter).sort({ [sortField]: -1 }).limit(1).toArray()[0];
  if (!latest) { print(`✘ ${coll} ${JSON.stringify(keyFilter)} nathi malyu`); return; }
  const opts = arrayFilters ? { arrayFilters } : {};
  db.getCollection(coll).updateOne({ _id: latest._id }, { $set: setFields }, opts);
  print(`  UPDATE  ${coll.padEnd(14)} ${JSON.stringify(keyFilter)}`);
}

// Navo document: business key par upsert (re-run safe)
function insertNew(coll, keyFilter, doc) {
  db.getCollection(coll).updateOne(keyFilter, { $set: doc }, { upsert: true });
  print(`  INSERT  ${coll.padEnd(14)} ${JSON.stringify(keyFilter)}`);
}

print("\n--- 00:00 → 04:00 changes ---");

// =============================================================================
// products  → Job-1
// =============================================================================
// P003: price 2000 → 1800, WH-AHM stock 100 → 95   (01:10)
//   P003 na 2 docs che (10-05 vado duplicate). Latest (10-20) ne j update karvano.
updateLatest("products", { product_code: "P003" },
  { price: 1800, "stock.$[wh].qty": 95, updated_at: ist("2026-10-21 01:10") },
  "updated_at", [{ "wh.warehouse": "WH-AHM" }]);

// P024: navo product (00:45)
insertNew("products", { product_code: "P024" }, {
  product_code: "P024",
  name: "Wireless Mouse",
  brand: { code: "B03", name: "TechBook" },
  category: { code: "C-ELE-LAP", name: "Laptops", parent: { code: "C-ELE", name: "Electronics" } },
  supplier: { code: "SUP-01", name: "Shree Electronics Pvt Ltd", city: "Ahmedabad" },
  price: 800,
  stock: [{ warehouse: "WH-AHM", qty: 50 }],
  is_active: true,
  updated_at: ist("2026-10-21 00:45")
});

// =============================================================================
// customers  → Job-2
// =============================================================================
// C003: Mumbai → Pune, Gold → Platinum  (01:20)  → SCD2 (navi version)
updateLatest("customers", { customer_code: "C003" }, {
  "address.city": "Pune", "address.state": "Maharashtra", "address.pincode": "411001",
  tier: "Platinum", updated_at: ist("2026-10-21 01:20")
});

// C002: phone badlayo  (02:15)  → SCD1 (overwrite)
updateLatest("customers", { customer_code: "C002" }, {
  phone: "9876511122", updated_at: ist("2026-10-21 02:15")
});

// C023: navo customer  (00:30)
insertNew("customers", { customer_code: "C023" }, {
  customer_code: "C023", name: "Jay Parmar", email: "jay.parmar@mail.com", phone: "9876500023",
  address: { city: "Gandhinagar", state: "Gujarat", pincode: "382010" },
  tier: "Bronze", signup_date: "2026-10-21", updated_at: ist("2026-10-21 00:30")
});

// =============================================================================
// orders  → Job-2 (watermark) ane Job-4 (key lookup)
// =============================================================================
// O1024: navo order, C023, P024 + P003 (navi price 1800) = 2600  (01:30)
insertNew("orders", { order_no: "O1024" }, {
  order_no: "O1024", customer_code: "C023", order_ts: ist("2026-10-21 01:30"),
  status: "PLACED", payment_method: "UPI",
  items: [{ product_code: "P024", qty: 1, unit_price: 800 },
          { product_code: "P003", qty: 1, unit_price: 1800 }],
  shipping: { city: "Gandhinagar", state: "Gujarat", pincode: "382010" },
  order_total: 2600, updated_at: ist("2026-10-21 01:30")
});

// O1015: SHIPPED → DELIVERED  (02:05)
updateLatest("orders", { order_no: "O1015" }, { status: "DELIVERED", updated_at: ist("2026-10-21 02:05") });

// O1010: PLACED → SHIPPED  (02:40)
updateLatest("orders", { order_no: "O1010" }, { status: "SHIPPED", updated_at: ist("2026-10-21 02:40") });

// =============================================================================
// payments  → Job-2
// =============================================================================
// PAY10: PENDING → SUCCESS (paid 10-21)  (01:00)
updateLatest("payments", { payment_id: "PAY10" }, {
  status: "SUCCESS", paid_at: ist("2026-10-21 01:00"), updated_at: ist("2026-10-21 01:00")
});

// PAY23: navo payment O1024  (01:31)
insertNew("payments", { payment_id: "PAY23" }, {
  payment_id: "PAY23", order_no: "O1024", method: "UPI", amount: 2600, status: "SUCCESS",
  paid_at: ist("2026-10-21 01:31"), updated_at: ist("2026-10-21 01:31")
});

// =============================================================================
// shipments  → Job-4
// =============================================================================
// SH-015: IN_TRANSIT → DELIVERED (10-21 01:50)  (02:00)  → 9 divas, 6 divas late
updateLatest("shipments", { shipment_id: "SH-015" }, {
  status: "DELIVERED", delivered_at: ist("2026-10-21 01:50"), updated_at: ist("2026-10-21 02:00")
});

// SH-010: navo shipment O1010, SpeedEx, dispatched 10-21, promised 10-24  (02:30)
insertNew("shipments", { shipment_id: "SH-010" }, {
  shipment_id: "SH-010", order_no: "O1010", courier: "SpeedEx",
  dispatched_at: ist("2026-10-21 02:30"), promised_date: "2026-10-24", delivered_at: null,
  status: "IN_TRANSIT", updated_at: ist("2026-10-21 02:30")
});

// =============================================================================
// reviews  → Job-4
// =============================================================================
// RV25: navo review, P019, C015, O1015 (have DELIVERED), rating 4  (03:10)
insertNew("reviews", { review_id: "RV25" }, {
  review_id: "RV25", product_code: "P019", customer_code: "C015", order_no: "O1015",
  rating: 4, comment: "Nice watch, late delivery", review_date: "2026-10-21",
  updated_at: ist("2026-10-21 03:10")
});

// =============================================================================
// email_tickets  → Job-3
// =============================================================================
// EM-003: open → closed  (02:30)
updateLatest("email_tickets", { ticket_ref: "EM-003" }, { status: "closed", updated_at: ist("2026-10-21 02:30") });

// EM-021: navi email  (03:30)
insertNew("email_tickets", { ticket_ref: "EM-021" }, {
  ticket_ref: "EM-021", from_email: "siddharth.rao@mail.com", subject: "Watch finally delivered, thanks",
  received_at: ist("2026-10-21 03:30"), status: "closed", priority: "Low", order_no: "O1015",
  updated_at: ist("2026-10-21 03:30")
});

// =============================================================================
// chat_tickets  → Job-3   ⚠️ TIMEZONE TRAP
//   startedAt / updatedAt UTC string che: 2026-10-20T21:20:00Z = 2026-10-21 02:50 IST
//   Date "10-20" dekhay che, pan IST ma e 10-21 02:50 che, etle aa window ma j aave.
// =============================================================================
insertNew("chat_tickets", { chatId: "CH-021" }, {
  chatId: "CH-021", customerCode: "C023", topic: "first order tracking",
  startedAt: "2026-10-20T21:20:00Z", state: "PENDING", agent: "Meena",
  updatedAt: "2026-10-20T21:20:00Z"
});

// =============================================================================
// call_logs  → Job-3
// =============================================================================
insertNew("call_logs", { call_id: "CL-021" }, {
  call_id: "CL-021", phone: "9876500010", reason: "laptop dispatch confirmation",
  call_time: "21/10/2026 03:00", outcome: "Resolved", duration_sec: 100, agent: "Asha",
  logged_at: ist("2026-10-21 03:00")
});

// =============================================================================
// Verify: incremental window ma ketla docs aave che?
// =============================================================================
const FROM = ist("2026-10-21 00:00"), TO = ist("2026-10-21 04:00");
const CHECKS = [
  ["products", "updated_at", 2], ["customers", "updated_at", 3], ["orders", "updated_at", 3],
  ["payments", "updated_at", 2], ["shipments", "updated_at", 2], ["reviews", "updated_at", 1],
  ["email_tickets", "updated_at", 2], ["call_logs", "logged_at", 1]
];

print("\n Window: updated_at > 2026-10-21 00:00 IST AND <= 04:00 IST");
print(" Collection        Docs  Expected");
print(" ----------------  ----  --------");
let total = 0;
CHECKS.forEach(([c, f, exp]) => {
  const n = db.getCollection(c).countDocuments({ [f]: { $gt: FROM, $lte: TO } });
  total += n;
  print(` ${c.padEnd(16)}  ${String(n).padStart(4)}  ${String(exp).padStart(8)}  ${n === exp ? "✔" : "✘"}`);
});

// chat_tickets: updatedAt UTC STRING che → window ne pan UTC string ma badlo
const chatWrong = db.chat_tickets.countDocuments({ updatedAt: { $gt: "2026-10-21T00:00:00", $lte: "2026-10-21T04:00:00" } });
const chatRight = db.chat_tickets.countDocuments({ updatedAt: { $gt: "2026-10-20T18:30:00Z", $lte: "2026-10-20T22:30:00Z" } });
total += chatRight;
print(` ${"chat_tickets".padEnd(16)}  ${String(chatRight).padStart(4)}  ${String(1).padStart(8)}  ${chatRight === 1 ? "✔" : "✘"}`);
print(` ${"TOTAL".padEnd(16)}  ${String(total).padStart(4)}  ${String(17).padStart(8)}`);

print(`\n ⚠️  Timezone trap demo (chat_tickets):`);
print(`     ❌ IST window sidhi UTC string sathe compare → ${chatWrong} doc  (CH-021 chhuti jay)`);
print(`     ✅ Window ne UTC ma convert kari ne compare → ${chatRight} doc`);

print("\n✔ Incremental data ready. Have 04:00 run karo: Job-1 → Job-2 → (Job-3, Job-4) → Job-5.");
