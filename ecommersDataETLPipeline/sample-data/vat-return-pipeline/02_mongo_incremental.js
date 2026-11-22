// =============================================================================
//  vat-return-pipeline  |  02_mongo_incremental.js  |  changes AFTER the first run: UPDATE, INSERT, DELETE
//
//  Run (after 01 + the first run):
//      mongosh "mongodb://localhost:27017" sample-data/vat-return-pipeline/02_mongo_incremental.js
//      POST http://localhost:8080/api/etl/pipelines/vat-return-pipeline/run
//
//  INCREMENTAL by timestamp: the next run reads documents with updatedAt > last watermark (= start of the
//  previous successful run). Every changed / new document below gets updatedAt = NOW, exactly like the source
//  application stamps its changes. MongoDB is never written by the ETL.
//  DELETE is not pushed to PostgreSQL (the job only reads existing documents). We delete a record that was
//  never loaded (rejected); its rejected-table rows stay as history.
//
//  EXPECTED INCREMENTAL RUN:
//      vat_return_coll  read 7 | valid 5 | rejected 1 | deleted 1
//          UPDATE 120000000612  Accra Foods Oct: status PEND -> APRV           (fact row updated in place)
//          UPDATE 120000000604  Asante Oct: taxpayer renamed                  (dim_taxpayer overwritten, SCD1)
//          FIX    120000000619  Ama Owusu Oct: netOutputTax corrected          (was rejected, now loaded)
//          INSERT 120000000621  Tema Steel Works Ltd Oct (NEW taxpayer)
//          INSERT 120000000622  Volta Logistics Oct
//          INSERT 120000000623  Kwame Nov, filed before the month ended -> VALIDATION rtnToDate in the future
//          DELETE 120000000617  bad tin, cancelled at the source
//          SOFT DELETE 120000000614  BNSJ Aug withdrawn (isDeleted = true) -> fact row is_deleted = TRUE
//      -> dim_taxpayer 7, dim_tax_period 3, fact_vat_return 17 (14 + 3 new; 2 updated in place; 1 is_deleted)
//      status: SUCCESS_WITH_FAILED (120000000623 rejected)
// =============================================================================
const DB_NAME = "shopdb";
db = db.getSiblingDB(DB_NAME);
const COLL = "vat_return_coll";
const coll = db.getCollection(COLL);

const NOW = new Date();   // every change is stamped with the current time -> newer than the last watermark

function update(ackNo, changes) {
  const r = coll.updateMany({ ackNo }, { $set: Object.assign({}, changes, { updatedAt: NOW }) });
  print(`  UPDATE ${ackNo} matched=${r.matchedCount}`);
}
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

// 7) delete: the return with the invalid tin (never loaded) was cancelled at the source
remove("120000000617");

// 8) SOFT DELETE: the August return of BNSJ was withdrawn at the source (isDeleted = true + new updatedAt)
//    -> fact_vat_return 120000000614 gets is_deleted = TRUE (row kept); same transaction as everything else
update("120000000614", { isDeleted: true });

print("");
print(`✔ ${COLL} to read: ${coll.countDocuments({ updatedAt: { $gte: NOW }, isTest: { $ne: true } })}`);
print("  expected: 7");
print("  Next: POST /api/etl/pipelines/vat-return-pipeline/run");
