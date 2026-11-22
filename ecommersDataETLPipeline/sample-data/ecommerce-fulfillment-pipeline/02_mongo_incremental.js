// =============================================================================
//  ecommerce-fulfillment-pipeline  |  02_mongo_incremental.js  |  changes AFTER the first run: UPDATE, INSERT, DELETE
//
//  ⚠️ SHP-00013 belongs to ORD-00011, which comes from the ecommerce-order-pipeline INCREMENTAL run.
//     Run the order pipeline's 02 script + its next run FIRST.
//
//  Run:   mongosh "mongodb://localhost:27017" sample-data/ecommerce-fulfillment-pipeline/02_mongo_incremental.js
//         POST http://localhost:8080/api/etl/pipelines/ecommerce-fulfillment-pipeline/run
//
//  INCREMENTAL by timestamp: the next run reads documents with updatedAt > last watermark (= start of the
//  previous successful run). Every changed / new document below gets updatedAt = NOW (the moment this
//  script runs), exactly like a source application stamps its changes. MongoDB is never written by the ETL.
//  DELETE is not pushed to PostgreSQL. We delete only a record that was never loaded (rejected).
//
//  EXPECTED INCREMENTAL RUN:
//      ecom_warehouses read 1 (WH-03 capacity change)
//      ecom_carriers   read 1 (CRR-06 new)
//      ecom_shipments  read 4 (SHP-00002 delivered late, SHP-00010 fixed -> loaded, SHP-00013 new) -> all valid
//                      + SHP-00008 soft-deleted (fact_shipment row is_deleted = TRUE)
//      status: SUCCESS (nothing rejected)
// =============================================================================
const DB_NAME = "shopdb";
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

print("\n--- ecommerce-fulfillment-pipeline incremental changes ---");

update("ecom_warehouses", { warehouseId: "WH-03" }, { capacityUnits: 55000 });
insert("ecom_carriers", { carrierId: "CRR-06" }, { carrierId: "CRR-06", carrierName: "XpressBees", serviceType: "EXPRESS",
  contactEmail: "support@xpressbees.example", status: "ACTIVE" });

// tracking update from the courier: parcel delivered (promised 11-07 -> 13 days late)
update("ecom_shipments", { shipmentId: "SHP-00002" },
       { shipmentStatus: "DELIVERED", deliveredDate: "2026-11-20T09:00:00" });
// fix: the missing delivered date was sent -> loaded now (its old rejected row stays as history)
update("ecom_shipments", { shipmentId: "SHP-00010" }, { deliveredDate: "2026-11-04T15:00:00" });
// new shipment for the new order ORD-00011 (from the order pipeline's incremental run)
insert("ecom_shipments", { shipmentId: "SHP-00013" }, {
  shipmentId: "SHP-00013", orderId: "ORD-00011", warehouseId: "WH-04", carrierId: "CRR-06", trackingNumber: "TRK00013",
  shippedDate: "2026-11-20T09:30:00", promisedDate: "2026-11-23", deliveredDate: null, shipmentStatus: "IN_TRANSIT",
  shippingCost: 79, weightKg: 0.6 });
// delete: the impossible shipment (delivered before shipped, never loaded) was cancelled at the source
remove("ecom_shipments", { shipmentId: "SHP-00011" });

// SOFT DELETE (isDeleted = true + new updatedAt): the lost shipment record was withdrawn at the source.
update("ecom_shipments", { shipmentId: "SHP-00008" }, { isDeleted: true });

print("");
["ecom_warehouses", "ecom_carriers", "ecom_shipments"].forEach(c => print(
  `✔ ${c.padEnd(16)} to read: ${db.getCollection(c).countDocuments({ updatedAt: { $gte: NOW }, isTest: { $ne: true } })}`));
print("  expected: warehouses 1, carriers 1, shipments 4");
print("  Next: POST /api/etl/pipelines/ecommerce-fulfillment-pipeline/run");
