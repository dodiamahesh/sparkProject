// =============================================================================
//  lib_unique_index.js  |  shared helper of sample-data/<pipeline>/03_bronze_unique_indexes.js (do not run alone)
//
//  Option C of the duplicate check: a partial unique index named "cdc_dup_rule" on the rule fields of a
//  bronze_db collection. MongoDB then rejects a second LIVE document with the same values (E11000) and the
//  subscriber stores it in meta.meta_duplicate_record.
//
//      key:      the rule fields, in the rule order           { name: 1, email: 1, phone: 1 }
//      unique:   true
//      partial:  _deleted: false  -> a soft-deleted document does not block a new one
//                <field>: { $exists: true } for every field -> a record without a rule field is not checked
//                                                              (same as the subscriber's query check)
//
//  Per collection:
//      collection missing           -> skip (the subscriber never creates one: 00_create_bronze_collections.js)
//      index exists and is the same -> ok
//      bronze already has duplicates for these fields -> BLOCKED: they are listed, nothing is changed or deleted
//      else                         -> old "cdc_dup_rule" index (other fields) is dropped, the new one is created
// =============================================================================
const CDC_DUP_INDEX = "cdc_dup_rule";

function applyUniqueIndexes(pipeline, rules) {
  const bronze = db.getSiblingDB("bronze_db");
  const existing = bronze.getCollectionNames();
  const count = { created: 0, ok: 0, skipped: 0, blocked: 0 };
  print(`\n--- ${pipeline}: unique indexes "${CDC_DUP_INDEX}" in bronze_db ---`);

  rules.forEach(({ collection, fields }) => {
    if (!existing.includes(collection)) {
      print(`  skip     ${collection}: collection does not exist (sample-data/00_create_bronze_collections.js)`);
      count.skipped++;
      return;
    }
    const coll = bronze.getCollection(collection);
    const key = {};
    const partial = { _deleted: false };
    fields.forEach(f => { key[f] = 1; partial[f] = { $exists: true }; });

    const current = coll.getIndexes().find(i => i.name === CDC_DUP_INDEX);
    if (current && current.unique
        && JSON.stringify(Object.keys(current.key)) === JSON.stringify(fields)
        && JSON.stringify(current.partialFilterExpression) === JSON.stringify(partial)) {
      print(`  ok       ${collection}: ${fields.join(", ")}`);
      count.ok++;
      return;
    }

    // live documents with the same rule values already in bronze -> a unique index cannot be built
    const groupId = {};
    fields.forEach((f, i) => { groupId["k" + i] = "$" + f; });
    const dups = coll.aggregate([
      { $match: partial },
      { $group: { _id: groupId, ids: { $push: "$_id" }, n: { $sum: 1 } } },
      { $match: { n: { $gt: 1 } } },
      { $limit: 20 }
    ]).toArray();
    if (dups.length) {
      print(`  BLOCKED  ${collection}: ${dups.length}${dups.length === 20 ? "+" : ""} group(s) of duplicates already in bronze_db, index NOT created:`);
      dups.forEach(d => print(`             ${JSON.stringify(d._id)} -> ${d.n} documents: ${d.ids.map(String).join(", ")}`));
      count.blocked++;
      return;
    }

    if (current) {
      coll.dropIndex(CDC_DUP_INDEX);
      print(`  dropped  ${collection}: old index on ${Object.keys(current.key).join(", ")}`);
    }
    coll.createIndex(key, { name: CDC_DUP_INDEX, unique: true, partialFilterExpression: partial });
    print(`  created  ${collection}: ${fields.join(", ")}`);
    count.created++;
  });

  print(`\n✔ ${pipeline}: created ${count.created}, ok ${count.ok}, skipped ${count.skipped}, blocked ${count.blocked}`);
  if (count.blocked) {
    print("  BLOCKED = bronze_db already has duplicates. Decide which document stays, clean the others");
    print("  (for example: soft delete with { $set: { _deleted: true } }), then run this script again.");
  }
}
