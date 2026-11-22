// =============================================================================
//  ecommerce-fulfillment-pipeline  |  03_bronze_unique_indexes.js  |  unique indexes (duplicate check, option C) in bronze_db
//
//  Run (after 00_create_bronze_collections.js; again after every rule change):
//      mongosh "mongodb://localhost:27017/?replicaSet=rs0" sample-data/ecommerce-fulfillment-pipeline/03_bronze_unique_indexes.js
//
//  The fields MUST be the same (same order) as the rule in PostgreSQL meta.cdc_duplicate_rule
//  (sample-data/00_cdc_duplicate_rule.sql); the subscriber warns at start when they differ.
//  "collection" = the bronze_db destination of the source (meta.cdc_collection_mapping; here the same name).
//  Duplicates already in bronze_db block the index: they are listed, nothing is deleted.
// =============================================================================
load(__dirname + "/../lib_unique_index.js");

applyUniqueIndexes("ecommerce-fulfillment-pipeline", [
  { collection: "ecom_warehouses", fields: ["warehouseId"] },
  { collection: "ecom_carriers",   fields: ["carrierId"] },
  { collection: "ecom_shipments",  fields: ["shipmentId"] }
]);
