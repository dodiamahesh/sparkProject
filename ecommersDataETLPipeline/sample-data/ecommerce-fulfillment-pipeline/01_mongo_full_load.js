// =============================================================================
//  ecommerce-fulfillment-pipeline  |  01_mongo_full_load.js  |  MongoDB sample data for the FULL load
//
//  ⚠️ DEPENDS ON ecommerce-order-pipeline: every shipment's orderId must exist in silver.fact_order.
//     Run ecommerce-order-pipeline (its 01 script + FULL load) FIRST.
//
//  Run:   mongosh "mongodb://localhost:27017" sample-data/ecommerce-fulfillment-pipeline/01_mongo_full_load.js
//  Then:  POST http://localhost:8080/api/etl/pipelines/ecommerce-fulfillment-pipeline/run?loadType=FULL
//
//  Database: shopdb (default MONGO_DATABASE).
//      ecom_warehouses -> dim_warehouse, ecom_carriers -> dim_carrier,
//      ecom_shipments  -> fact_shipment (order, warehouse, carrier must exist; customer_key taken from fact_order)
//
//  EXPECTED FULL LOAD:
//      ecom_warehouses read 6  | valid 5 | rejected 1 (VALIDATION WH-06 pincode)
//      ecom_carriers   read 5  | valid 4 | rejected 1 (VALIDATION CRR-05 serviceType)
//      ecom_shipments  read 15 | valid 10 | rejected 5 (DUPLICATE SHP-00004, ORPHAN SHP-00009 order,
//                                                       VALIDATION SHP-00010 / SHP-00011, ORPHAN SHP-00012 carrier)
//      (4 bad of 15 = 26.7%: the reject gate of this source allows at most 30%)
//      warnings: WH-05 INACTIVE, CRR-04 no contactEmail, SHP-00006 weight > 10 kg
// =============================================================================
const DB_NAME = "shopdb";
db = db.getSiblingDB(DB_NAME);

const COLLECTIONS = ["ecom_warehouses", "ecom_carriers", "ecom_shipments"];
COLLECTIONS.forEach(c => db.getCollection(c).drop());

// ---------------------------------------------------------------- ecom_warehouses (6)
const wh = (id, name, city, state, pincode, capacity, status) => ({ warehouseId: id, warehouseName: name, city, state,
  pincode, capacityUnits: capacity, status, updatedAt: "2026-10-01T10:00:00" });
db.ecom_warehouses.insertMany([
  wh("WH-01", "Ahmedabad Fulfilment Centre", "ahmedabad", "gujarat",     "382210", 50000,     "ACTIVE"),   // sanitize: Proper Case
  wh("WH-02", "Bhiwandi Mega Warehouse",     "Bhiwandi",  "Maharashtra", "421302", "1,20,000","active"),   // sanitize: separators, case
  wh("WH-03", "Bengaluru Hub",               "Bengaluru", "Karnataka",   "562123", 40000,     "ACTIVE"),
  wh("WH-04", "Gurugram Hub",                "Gurugram",  "Haryana",     "122001", 60000,     "ACTIVE"),
  wh("WH-05", "Kolkata Old Depot",           "Kolkata",   "West Bengal", "700088", 8000,      "INACTIVE"), // WARNING not ACTIVE
  wh("WH-06", "Test Depot",                  "Nowhere",   "Unknown",     "12345",  1000,      "ACTIVE")    // VALIDATION pincode
]);

// ---------------------------------------------------------------- ecom_carriers (5)
const crr = (id, name, service, email, status) => ({ carrierId: id, carrierName: name, serviceType: service,
  contactEmail: email, status, updatedAt: "2026-10-01T10:00:00" });
db.ecom_carriers.insertMany([
  crr("CRR-01", "Delhivery",       "STANDARD", "support@delhivery.example", "ACTIVE"),
  crr("CRR-02", "Blue Dart",       "express",  "CARE@BLUEDART.EXAMPLE",     "ACTIVE"),   // sanitize
  crr("CRR-03", "Ekart Logistics", "STANDARD", "help@ekart.example",        "ACTIVE"),
  crr("CRR-04", "Dunzo",           "SAME_DAY", null,                        "ACTIVE"),   // WARNING no contactEmail
  crr("CRR-05", "SkyDrop",         "DRONE",    "hello@skydrop.example",     "ACTIVE")    // VALIDATION serviceType
]);

// ---------------------------------------------------------------- ecom_shipments (15)
function shp(id, order, warehouse, carrier, shipped, promised, delivered, status, cost, weight, updatedAt) {
  return { shipmentId: id, orderId: order, warehouseId: warehouse, carrierId: carrier,
           trackingNumber: "TRK" + id.slice(-5), shippedDate: shipped, promisedDate: promised,
           deliveredDate: delivered, shipmentStatus: status, shippingCost: cost, weightKg: weight,
           updatedAt: updatedAt || delivered || shipped };
}
db.ecom_shipments.insertMany([
  shp("SHP-00001", "ORD-00001", "WH-01", "CRR-01", "2026-11-01T18:00:00", "2026-11-05", "2026-11-03T14:00:00", "DELIVERED",  53.10, 0.45),   // on time
  shp("SHP-00002", "ORD-00002", "WH-02", "CRR-02", "2026-11-02T20:00:00", "2026-11-07", null,                  "in_transit", 89,    1.20),   // sanitize; incremental -> DELIVERED
  shp("SHP-00003", "ORD-00003", "WH-04", "CRR-03", "2026-11-03T19:00:00", "2026-11-05", "2026-11-08T12:00:00", "DELIVERED",  45,    0.30),   // late (3 days)
  shp("SHP-00004", "ORD-00004", "WH-03", "CRR-01", "2026-11-04T21:00:00", "2026-11-09", null,                  "IN_TRANSIT", 60,    2.10, "2026-11-05T08:00:00"),   // latest
  shp("SHP-00005", "ORD-00005", "WH-03", "CRR-04", "2026-11-05T20:30:00", "2026-11-06", "2026-11-06T11:00:00", "DELIVERED",  120,   3.50),
  shp("SHP-00006", "ORD-00007", "WH-01", "CRR-02", "2026-11-07T18:30:00", "2026-11-10", "2026-11-09T16:00:00", "DELIVERED",  "1,150.00", 12.0),  // WARNING weight > 10
  shp("SHP-00007", "ORD-00008", "WH-04", "CRR-03", "2026-11-08T22:00:00", "2026-11-12", null,                  "RETURNED_TO_ORIGIN", 45, 0.80, "2026-11-13T09:00:00"),
  shp("SHP-00008", "ORD-00006", "WH-04", "CRR-01", "2026-11-06T17:00:00", "2026-11-10", null,                  "LOST",       49,    1.00, "2026-11-14T09:00:00"),
  shp("SHP-00014", "ORD-00007", "WH-01", "CRR-03", "2026-11-07T19:00:00", "2026-11-11", "2026-11-10T13:00:00", "DELIVERED",  45,    0.70),   // 2nd parcel of the same order
  shp("SHP-00015", "ORD-00005", "WH-03", "CRR-02", "2026-11-05T22:00:00", "2026-11-08", "2026-11-07T18:00:00", "DELIVERED",  89,    1.10),
  shp("SHP-00004", "ORD-00004", "WH-03", "CRR-01", "2026-11-04T21:00:00", "2026-11-09", null,                  "IN_TRANSIT", 60,    2.10, "2026-11-04T21:00:00"),   // DUPLICATE (older)
  shp("SHP-00009", "ORD-00009", "WH-02", "CRR-02", "2026-11-09T18:00:00", "2026-11-12", null,                  "IN_TRANSIT", 49,    0.50),   // ORPHAN order (ORD-00009 was rejected)
  shp("SHP-00010", "ORD-00001", "WH-01", "CRR-01", "2026-11-02T18:00:00", "2026-11-05", null,                  "DELIVERED",  25,    0.20),   // VALIDATION: DELIVERED without deliveredDate (fixed in incremental)
  shp("SHP-00011", "ORD-00003", "WH-04", "CRR-03", "2026-11-05T18:00:00", "2026-11-07", "2026-11-04T10:00:00", "DELIVERED",  45,    0.30),   // VALIDATION: delivered before shipped (deleted in incremental)
  shp("SHP-00012", "ORD-00005", "WH-03", "CRR-05", "2026-11-05T21:00:00", "2026-11-06", null,                  "IN_TRANSIT", 99,    1.00)    // ORPHAN carrier (CRR-05 was rejected)
]);

COLLECTIONS.forEach(c => db.getCollection(c).createIndex({ processedFlag: 1 }));
db.ecom_warehouses.createIndex({ warehouseId: 1 }); db.ecom_carriers.createIndex({ carrierId: 1 });
db.ecom_shipments.createIndex({ shipmentId: 1 });   db.ecom_shipments.createIndex({ orderId: 1 });

print("");
COLLECTIONS.forEach(c => print(`✔ ${DB_NAME}.${c.padEnd(16)} ${db.getCollection(c).countDocuments()} documents`));
print("  expected: warehouses 6, carriers 5, shipments 15");
print("  Next: POST /api/etl/pipelines/ecommerce-fulfillment-pipeline/run?loadType=FULL");
