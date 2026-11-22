// =============================================================================
//  ecommerce-fulfillment-pipeline  |  02_mongo_incremental.js  |  changes AFTER the first run: UPDATE, INSERT, DELETE
//
//  ⚠️ SHP-00013 belongs to ORD-00011, which comes from ecommerce-order-pipeline/02. Run that script FIRST.
//
//  Run (after 01, when the subscriber LAG = 0):
//      mongosh "mongodb://localhost:27017/?replicaSet=rs0" sample-data/ecommerce-fulfillment-pipeline/02_mongo_incremental.js
//      mongosh "mongodb://localhost:27017/?replicaSet=rs0" sample-data/99_verify_bronze.js
//
//  sourceMongo_db is the TRANSACTION database: a removed record is HARD deleted (the document is gone).
//  The subscriber keeps it in bronze_db with its last values: _deleted: true, _op: "delete".
//  Every changed / new document still gets updatedAt = NOW, like a source application stamps its changes.
//
//  HARD DELETES in this script (expected in bronze_db.ecom_shipments with _deleted: true):
//      SHP-00011 (rejected: delivered before shipped), SHP-00008 (LOADED shipment, LOST, record withdrawn)
//
//  EXPECTED after this script (fresh start: 01 ran once):
//                         source   bronze total = live + _deleted
//      ecom_warehouses       6           6      =   6  +  0     (WH-03 capacityUnits 55000)
//      ecom_carriers         6           6      =   6  +  0     (CRR-06 new)
//      ecom_shipments       14          16      =  14  +  2     (SHP-00002 DELIVERED, SHP-00010 fixed, SHP-00013 new)
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
/** HARD delete at the source (document is gone). Bronze keeps it with _deleted: true. */
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
// HARD DELETE: the impossible shipment (delivered before shipped, never loaded) was cancelled at the source
remove("ecom_shipments", { shipmentId: "SHP-00011" });

// HARD DELETE of a LOADED shipment: the lost shipment record was withdrawn at the source.
// bronze_db: SHP-00008 stays with its last values (status LOST) and _deleted: true.
remove("ecom_shipments", { shipmentId: "SHP-00008" });

print("");
["ecom_warehouses", "ecom_carriers", "ecom_shipments"].forEach(c => print(
  `✔ ${DB_NAME}.${c.padEnd(16)} ${db.getCollection(c).countDocuments()} documents`));
print("  expected source: warehouses 6, carriers 6, shipments 14");
print("  expected bronze_db _deleted: shipments 2 (SHP-00008, SHP-00011)");
print("  Next: wait until the subscriber LAG = 0, then run sample-data/99_verify_bronze.js");
