// =============================================================================
//  ecommerce-fulfillment-pipeline  |  02_mongo_incremental.js  |  changes AFTER the full load: UPDATE, INSERT, DELETE
//
//  ⚠️ SHP-00013 belongs to ORD-00011, which comes from the ecommerce-order-pipeline INCREMENTAL run.
//     Run the order pipeline's 02 script + INCREMENTAL load FIRST.
//
//  Run:   mongosh "mongodb://localhost:27017" sample-data/ecommerce-fulfillment-pipeline/02_mongo_incremental.js
//         POST http://localhost:8080/api/etl/pipelines/ecommerce-fulfillment-pipeline/run?loadType=INCREMENTAL
//
//  INCREMENTAL reads processedFlag = 'N' (or missing). A changed document gets 'N' again (this script does it).
//  DELETE is not pushed to PostgreSQL. We delete only a record that was never loaded (rejected).
//
//  EXPECTED INCREMENTAL RUN:
//      ecom_warehouses read 1 (WH-03 capacity change)
//      ecom_carriers   read 1 (CRR-06 new)
//      ecom_shipments  read 3 (SHP-00002 delivered late, SHP-00010 fixed -> resolved, SHP-00013 new) -> all valid
// =============================================================================
const DB_NAME = "shopdb";
db = db.getSiblingDB(DB_NAME);

const NOW = "2026-11-20T10:00:00";

function update(coll, filter, changes) {
  const r = db.getCollection(coll).updateOne(filter,
      { $set: Object.assign({}, changes, { updatedAt: changes.updatedAt || NOW, processedFlag: "N" }) });
  print(`  UPDATE ${coll} ${JSON.stringify(filter)} matched=${r.matchedCount}`);
}
function insert(coll, filter, doc) {
  db.getCollection(coll).updateOne(filter, { $set: Object.assign({}, doc, { processedFlag: "N" }) }, { upsert: true });
  print(`  INSERT ${coll} ${JSON.stringify(filter)}`);
}
function remove(coll, filter) {
  const r = db.getCollection(coll).deleteMany(filter);
  print(`  DELETE ${coll} ${JSON.stringify(filter)} deleted=${r.deletedCount}`);
}

print("\n--- ecommerce-fulfillment-pipeline incremental changes ---");

update("ecom_warehouses", { warehouseId: "WH-03" }, { capacityUnits: 55000, updatedAt: "2026-11-20T08:00:00" });
insert("ecom_carriers", { carrierId: "CRR-06" }, { carrierId: "CRR-06", carrierName: "XpressBees", serviceType: "EXPRESS",
  contactEmail: "support@xpressbees.example", status: "ACTIVE", updatedAt: "2026-11-20T08:05:00" });

// tracking update from the courier: parcel delivered (promised 11-07 -> 13 days late)
update("ecom_shipments", { shipmentId: "SHP-00002" },
       { shipmentStatus: "DELIVERED", deliveredDate: "2026-11-20T09:00:00", updatedAt: "2026-11-20T09:00:00" });
// fix: the missing delivered date was sent -> loaded now, removed from the rejected table
update("ecom_shipments", { shipmentId: "SHP-00010" }, { deliveredDate: "2026-11-04T15:00:00", updatedAt: "2026-11-20T09:05:00" });
// new shipment for the new order ORD-00011 (from the order pipeline's incremental run)
insert("ecom_shipments", { shipmentId: "SHP-00013" }, {
  shipmentId: "SHP-00013", orderId: "ORD-00011", warehouseId: "WH-04", carrierId: "CRR-06", trackingNumber: "TRK00013",
  shippedDate: "2026-11-20T09:30:00", promisedDate: "2026-11-23", deliveredDate: null, shipmentStatus: "IN_TRANSIT",
  shippingCost: 79, weightKg: 0.6, updatedAt: "2026-11-20T09:30:00" });
// delete: the impossible shipment (delivered before shipped, never loaded) was cancelled at the source
remove("ecom_shipments", { shipmentId: "SHP-00011" });

print("");
["ecom_warehouses", "ecom_carriers", "ecom_shipments"].forEach(c => print(
  `✔ ${c.padEnd(16)} to read: ${db.getCollection(c).countDocuments({ processedFlag: { $in: ["N", null] }, isTest: { $ne: true } })}`));
print("  expected: warehouses 1, carriers 1, shipments 3");
print("  Next: POST /api/etl/pipelines/ecommerce-fulfillment-pipeline/run?loadType=INCREMENTAL");
