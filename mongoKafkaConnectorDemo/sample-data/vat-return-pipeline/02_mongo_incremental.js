// =============================================================================
//  vat-return-pipeline  |  02_mongo_incremental.js  |  changes AFTER the first run: UPDATE, INSERT, DELETE
//
//  Run (after 01, when the subscriber LAG = 0):
//      mongosh "mongodb://localhost:27017/?replicaSet=rs0" sample-data/vat-return-pipeline/02_mongo_incremental.js
//      mongosh "mongodb://localhost:27017/?replicaSet=rs0" sample-data/99_verify_bronze.js
//
//  sourceMongo_db is the TRANSACTION database: a removed record is HARD deleted (the document is gone).
//  The subscriber keeps it in bronze_db with its last values: _deleted: true, _op: "delete".
//  Every changed / new document still gets updatedAt = NOW, like the source application stamps its changes.
//
//  CHANGES:
//      UPDATE 120000000612  Accra Foods Oct: status PEND -> APRV (nested field rtnRegisterVatDTO.rtnStatus)
//      UPDATE 120000000604  Asante Oct: taxpayer renamed
//      FIX    120000000619  Ama Owusu Oct: netOutputTax corrected
//      INSERT 120000000621  Tema Steel Works Ltd Oct (NEW taxpayer)
//      INSERT 120000000622  Volta Logistics Oct
//      INSERT 120000000623  Kwame Nov, filed before the month ended
//      HARD DELETE 120000000617  bad tin (rejected), cancelled at the source
//      HARD DELETE 120000000614  BNSJ Aug (LOADED return), withdrawn at the source
//      HARD DELETE isTest: true  every QA test document (deleteMany by a condition, here 120000000699)
//
//  EXPECTED after this script (fresh start: 01 ran once):
//      sourceMongo_db.vat_return_coll  22   (22 + 3 new - 3 deleted)
//      bronze_db.vat_return_coll       25 = 22 live (_deleted false) + 3 soft-deleted (_deleted true)
// =============================================================================
const DB_NAME = "sourceMongo_db";   // TRANSACTION (source) database, captured by the CDC connector
db = db.getSiblingDB(DB_NAME);
const COLL = "vat_return_coll";
const coll = db.getCollection(COLL);

const NOW = new Date();   // every change is stamped with the current time -> newer than the last watermark

function update(ackNo, changes) {
  const r = coll.updateMany({ ackNo }, { $set: Object.assign({}, changes, { updatedAt: NOW }) });
  print(`  UPDATE ${ackNo} matched=${r.matchedCount}`);
}
/** HARD delete at the source (document is gone). Bronze keeps it with _deleted: true. */
function remove(ackNo) {
  const r = coll.deleteMany({ ackNo });
  print(`  DELETE ${ackNo} deleted=${r.deletedCount}`);
}
/** New return: copy an existing return as template and change the fields of the new filing. */
function insertFrom(templateAckNo, ackNo, changes) {
  const doc = coll.findOne({ ackNo: templateAckNo });
  delete doc._id;
  doc.ackNo = ackNo;
  doc.rtnRegisterVatDTO.genAcknowledgementVatHeader.ackNo = ackNo;
  doc.rtnRegisterVatDTO.genAcknowledgementVatHeader._id = NumberLong("12031" + ackNo.slice(-7));
  Object.keys(changes).forEach(path => {                  // "a.b.c" -> nested field
    const keys = path.split("."), last = keys.pop();
    keys.reduce((o, k) => o[k], doc)[last] = changes[path];
  });
  doc.updatedAt = NOW;
  coll.insertOne(doc);
  print(`  INSERT ${ackNo} (from ${templateAckNo})`);
}

print("\n--- vat-return-pipeline incremental changes ---");

// 1) approval of a pending return (nested status)
update("120000000612", { "rtnRegisterVatDTO.rtnStatus": "APRV" });

// 2) taxpayer renamed (every return carries the taxpayer name -> dim_taxpayer gets the latest name)
update("120000000604", { taxpayerName: "Asante Trading Company Ltd" });

// 3) fix of a rejected return: net output tax = 14,000 - 6,000 = 8,000
update("120000000619", { "vatComputationData.netOutputTaxValue": "8000.00" });

// 4) new taxpayer Tema Steel Works Ltd, October: outputs 12,00,000, purchases 7,00,000
insertFrom("120000000604", "120000000621", {
  tin: "P0000001286", taxpayerName: "Tema Steel Works Ltd", taxpayerCategory: "Non-Individual",
  taxpayerJurisdiction: "T", taxpayerId: "110330002865",
  "taxableOutputDataDto.taxableOutputsValue": "1200000", "taxableOutputDataDto.vatLeveies": "240000",
  "purchExpnsData.taxableValueOfLocalPurchases": "700000", "purchExpnsData.taxableValueOfTotalInput": "700000",
  "purchExpnsData.taxableValueOfTaxOnPurchasesExp": "140000",
  "vatComputationData.totalOutputTaxValue": "240000", "vatComputationData.totalInputTaxValue": "140000.00",
  "vatComputationData.dedInputTaxValue": "140000.00", "vatComputationData.netOutputTaxValue": "100000.00",
  "vatComputationData.netPayableValue": "100000.00", netTax: "100000.00",
  "rtnRegisterVatDTO.rtnTin": "P0000001286", "rtnRegisterVatDTO.rtnInputTax": 140000,
  "rtnRegisterVatDTO.rtnOutputTax": 240000, "rtnRegisterVatDTO.rtnTaxPayable": 100000,
  "rtnRegisterVatDTO.crtDt": new Date("2026-11-20T10:00:00Z"), "rtnRegisterVatDTO.genAcknowledgementVatHeader.ackDate": new Date("2026-11-20T00:00:00Z"),
  delayDays: "-11",
  "rtnRegisterVatDTO.rtnDelayedDays": NumberLong("-11")
});

// 5) Volta Logistics, October: outputs 3,10,000, purchases 1,40,000
insertFrom("120000000604", "120000000622", {
  tin: "P0000001064", taxpayerName: "Volta Logistics Ltd", taxpayerJurisdiction: "V", taxpayerId: "110330002643",
  "taxableOutputDataDto.taxableOutputsValue": "310000", "taxableOutputDataDto.vatLeveies": "62000",
  "purchExpnsData.taxableValueOfLocalPurchases": "140000", "purchExpnsData.taxableValueOfTotalInput": "140000",
  "purchExpnsData.taxableValueOfTaxOnPurchasesExp": "28000",
  "vatComputationData.totalOutputTaxValue": "62000", "vatComputationData.totalInputTaxValue": "28000.00",
  "vatComputationData.dedInputTaxValue": "28000.00", "vatComputationData.netOutputTaxValue": "34000.00",
  "vatComputationData.netPayableValue": "34000.00", netTax: "34000.00",
  "rtnRegisterVatDTO.rtnTin": "P0000001064", "rtnRegisterVatDTO.rtnInputTax": 28000,
  "rtnRegisterVatDTO.rtnOutputTax": 62000, "rtnRegisterVatDTO.rtnTaxPayable": 34000,
  "rtnRegisterVatDTO.crtDt": new Date("2026-11-19T11:00:00Z"), "rtnRegisterVatDTO.genAcknowledgementVatHeader.ackDate": new Date("2026-11-19T00:00:00Z"),
  delayDays: "-12",
  "rtnRegisterVatDTO.rtnDelayedDays": NumberLong("-12")
});

// 6) November return filed BEFORE November ended -> rejected (rtnToDate cannot be in the future)
insertFrom("120000000606", "120000000623", {
  rtnFromDate: "2026-11-01", rtnToDate: "2026-11-30", month: 11, rtnDueDt: "2027-01-01",
  "rtnRegisterVatDTO.rtnPeriodFromDate": "2026-11-01", "rtnRegisterVatDTO.rtnPeriodToDate": "2026-11-30",
  "rtnRegisterVatDTO.rtnDueDt": "2027-01-01", "rtnRegisterVatDTO.rtnMonth": 11,
  "rtnRegisterVatDTO.crtDt": new Date("2026-11-21T09:00:00Z"), "rtnRegisterVatDTO.genAcknowledgementVatHeader.ackDate": new Date("2026-11-21T00:00:00Z"),
  delayDays: "-41",
  "rtnRegisterVatDTO.rtnDelayedDays": NumberLong("-41")
});

// 7) HARD DELETE: the return with the invalid tin (never loaded) was cancelled at the source
remove("120000000617");

// 8) HARD DELETE of a LOADED return: the August return of BNSJ was withdrawn at the source.
//    bronze_db: 120000000614 stays with its last values and _deleted: true
remove("120000000614");

// 9) HARD DELETE by a condition: every QA test document (deleteMany sends one delete event per document)
const tests = coll.deleteMany({ isTest: true });
print(`  DELETE isTest=true deleted=${tests.deletedCount}`);

print("");
print(`✔ ${DB_NAME}.${COLL} ${coll.countDocuments()} documents (expected 22)`);
print("  expected in bronze_db.vat_return_coll: 25 = 22 live + 3 _deleted (120000000614, 120000000617, 120000000699)");
print("  Next: wait until the subscriber LAG = 0, then run sample-data/99_verify_bronze.js");
