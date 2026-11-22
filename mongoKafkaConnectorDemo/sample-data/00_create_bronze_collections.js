// =============================================================================
//  00_create_bronze_collections.js  |  creates the destination collections in bronze_db
//
//  The CDC subscriber NEVER creates a collection: a message whose destination collection does not exist
//  goes to the DLQ (cdc-dlq). Create every destination of meta.cdc_collection_mapping (active rows, MongoDB and SQL sources)
//  here BEFORE the subscriber gets its events.
//
//  Run:   mongosh "mongodb://localhost:27017/?replicaSet=rs0" sample-data/00_create_bronze_collections.js
//  Check the mapping table:  SELECT destination FROM meta.cdc_collection_mapping WHERE active;
// =============================================================================
const BRONZE = db.getSiblingDB("bronze_db");

// SAME names as the "destination" column of the active rows in meta.cdc_collection_mapping (MongoDB and SQL sources)
const DESTINATIONS = ["customers", "products"];

const existing = BRONZE.getCollectionNames();
DESTINATIONS.forEach(name => {
  if (existing.includes(name)) {
    print(`  exists   bronze_db.${name}`);
  } else {
    BRONZE.createCollection(name);
    print(`  created  bronze_db.${name}`);
  }
});
print(`\n✔ bronze_db collections: ${BRONZE.getCollectionNames().sort().join(", ")}`);
