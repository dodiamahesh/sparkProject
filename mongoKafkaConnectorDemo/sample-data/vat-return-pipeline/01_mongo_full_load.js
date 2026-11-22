// =============================================================================
//  vat-return-pipeline  |  01_mongo_full_load.js  |  MongoDB sample data for the FIRST run
//
//  Run:   mongosh "mongodb://localhost:27017/?replicaSet=rs0" sample-data/vat-return-pipeline/01_mongo_full_load.js
//  Check: mongosh "mongodb://localhost:27017/?replicaSet=rs0" sample-data/99_verify_bronze.js
//
//  CDC flow: sourceMongo_db (this script writes here) -> connector "mongo-cdc-sourcemongo-source"
//            -> Kafka topic "sourceMongo_db" -> mongo-cdc-subscriber (subscriber-sourcemongo.properties) -> bronze_db
//  Collection: vat_return_coll = one document per FILED monthly VAT return (same shape as the GRA source document).
//
//  ⚠️ updatedAt (top level, BSON Date) is NOT in the original source document. The ETL watermark needs it:
//     the source application must set it on every insert / update.
//
//  EXPECTED in bronze_db.vat_return_coll: 22 documents (1 of them the test document), all _op "insert", _deleted false
//
//  ETL later (reads bronze_db), EXPECTED FIRST RUN (test document is never read):
//      vat_return_coll  read 21 | valid 14 | rejected 7
//          DUPLICATE  120000000603 (older copy of Asante Sep)
//          CORRUPT    120000000615 taxableOutputsValue "20O000",  120000000616 rtnFromDate "2026-08-32"
//          VALIDATION 120000000617 tin "X12345", 120000000618 nil return with outputs,
//                     120000000619 netOutputTax does not match (fixed in 02)
//          ORPHAN     120000000620 formId 99999 not in dim_tax_form
//      (6 bad of 21 = 28.6%: the reject gate allows at most 30%)
//      dim_taxpayer 6, dim_tax_period 3 (Aug, Sep, Oct 2026), fact_vat_return 14
//      status: SUCCESS_WITH_FAILED (valid rows loaded, the rejected ones are in meta.meta_rejected_record)
// =============================================================================
const DB_NAME = "sourceMongo_db";   // TRANSACTION (source) database, captured by the CDC connector
db = db.getSiblingDB(DB_NAME);
const COLL = "vat_return_coll";
// EMPTY, not drop: deleteMany sends one CDC delete event per old document (bronze: _deleted = true)
db.getCollection(COLL).deleteMany({});

// Ghana = GMT (UTC+0): every time below is UTC
const utc = (s) => new Date(s.length === 10 ? s + "T00:00:00Z" : s + "Z");
const pad = (n) => String(n).padStart(2, "0");
const txt = (n) => String(n);              // the source sends amounts as TEXT
const dec = (n) => Number(n).toFixed(2);   // ... sometimes with 2 decimals

const TAXPAYERS = {
  P0000000620: { name: "BNSJ",                 category: "Non-Individual", jurisdiction: "Q", taxpayerId: "110330002299" },
  P0000000731: { name: "Asante Trading Ltd",   category: "Non-Individual", jurisdiction: "A", taxpayerId: "110330002310" },
  P0000000842: { name: "Kwame Boateng",        category: "Individual",     jurisdiction: "K", taxpayerId: "110330002421" },
  P0000000953: { name: "Accra Foods Ltd",      category: "Non-Individual", jurisdiction: "A", taxpayerId: "110330002532" },
  P0000001064: { name: "Volta Logistics Ltd",  category: "Non-Individual", jurisdiction: "V", taxpayerId: "110330002643" },
  P0000001175: { name: "Ama Owusu",            category: "Individual",     jurisdiction: "Q", taxpayerId: "110330002754" },
  P0000001286: { name: "Tema Steel Works Ltd", category: "Non-Individual", jurisdiction: "T", taxpayerId: "110330002865" }
};

/**
 * One VAT return document. The VAT numbers are COMPUTED, so a good return is always consistent:
 *   vatLevies = outputs * 20%, input tax = total input * 20%, netOutputTax = vatLevies - input tax,
 *   netPayable = netOutputTax - withholding - creditLastMonth, netTax = netPayable + penalty + interest,
 *   due date = 1st day of (month + 2), delayDays = filed date - due date (> 0 = late return).
 * p.patch(doc) changes the finished document (sanitization cases and bad records).
 */
function vat(p) {
  const tp = TAXPAYERS[p.tin] || { name: p.name, category: "Non-Individual", jurisdiction: "Q", taxpayerId: "110330009999" };
  const from = `${p.year}-${pad(p.month)}-01`;
  const to = `${p.year}-${pad(p.month)}-${pad(new Date(Date.UTC(p.year, p.month, 0)).getUTCDate())}`;
  const due = `${p.month >= 11 ? p.year + 1 : p.year}-${pad(((p.month + 1) % 12) + 1)}-01`;
  const filed = utc(p.filedAt);
  const delay = Math.round((utc(p.filedAt.slice(0, 10)) - utc(due)) / 86400000);

  const outputs = p.outputs || 0, zeroRated = p.zeroRated || 0, exempt = p.exempt || 0;
  const local = p.purchases || 0, importedGoods = p.importedGoods || 0, totalInput = local + importedGoods;
  const levies = outputs * 0.2, inputTax = totalInput * 0.2, net = levies - inputTax;
  const withholding = p.withholding || 0, creditLastMonth = p.creditLastMonth || 0;
  const payable = net - withholding - creditLastMonth;
  const penalty = p.penalty || 0, interest = p.interest || 0;
  const formId = p.formId || "12027", channel = p.channel || "INET";
  const amended = String(p.rtnType || "O").toUpperCase().startsWith("A");

  const doc = {
    tin: p.tin, taxpayerName: tp.name, taxpayerCategory: tp.category, taxpayerJurisdiction: tp.jurisdiction,
    rtnFromDate: from, rtnToDate: to, rtnType: p.rtnType || "Original", taxpayerId: tp.taxpayerId,
    isNilReturn: !!p.nil, month: p.month, year: p.year, fileFrom: channel, ackNo: p.ackNo,
    taxTypeId: "101", formId: NumberLong(formId),
    taxableOutputDataDto: {
      taxableOutputsValue: txt(outputs), vatLeveies: txt(levies), zeroRatedTaxableValue: txt(zeroRated),
      reliefTaxableValue: "0", exemptSupTaxableValue: txt(exempt) },
    purchExpnsData: {
      taxableValueOfLocalPurchases: txt(local), taxableValueOfImportedGoods: txt(importedGoods),
      taxableValueOfImportedServices: "0", taxableValueOfTotalInput: txt(totalInput),
      taxableValueOfTaxOnPurchasesExp: txt(inputTax) },
    vatComputationData: {
      totalOutputTaxValue: txt(levies), totalInputTaxValue: dec(inputTax), dedInputTaxValue: dec(inputTax),
      netOutputTaxValue: dec(net), withHoldingVatCrValue: txt(withholding), creditLastMonth: txt(creditLastMonth),
      netPayableValue: dec(payable) },
    rtnRegisterVatDTO: {
      rtnTin: p.tin, rtnPeriodFromDate: from, rtnPeriodToDate: to, rtnDueDt: due,
      rtnStatus: p.status || "APRV", rtnType: amended ? "A" : "O", isNilRtn: p.nil ? "Y" : "N",
      rtnInputTax: inputTax, rtnOutputTax: levies, rtnCrdtBroughtFrwd: creditLastMonth,
      rtnCrdtCarriedFrwd: payable < 0 ? payable : 0, rtnTaxPayable: payable, applicationSource: channel,
      branchId: NumberLong("1"), rtnDelayedDays: NumberLong(String(delay)), rtnFormId: NumberLong(formId),
      crtBy: NumberLong("290230001842"), crtDt: filed,
      genAcknowledgementVatHeader: {
        _id: NumberLong("12031" + p.ackNo.slice(-7)), ackNo: p.ackNo, ackDate: utc(p.filedAt.slice(0, 10)),
        ackStatus: "GEN", formId: NumberLong(formId), applicationSource: channel,
        crtBy: NumberLong("290230001842"), crtDt: filed },
      rtnMonth: p.month, rtnYear: p.year, isLateRtn: delay > 0 ? "Y" : "N", isActive: "Y",
      _class: "com.tcs.gra.returns.service.impl.RtnRegisterVatDTOEx" },
    penaltyAmount: dec(penalty), netTax: dec(payable + penalty + interest), interest: dec(interest),
    delayDays: String(delay), rtnDueDt: due,
    additionalInputData: {
      valueOfStockBoughtStandard: "0", leviesOfStockBoughtStandard: "0",
      valueStockBoughtFlateRate: "0", leviesStockBoughtFlateRate: "0" },
    _class: "com.tcs.gra.returns.web.rest.vm.VatStandardRateDTO",
    prevRtnRegisterVatDTO: {},
    updatedAt: utc(p.updatedAt || p.filedAt)       // WATERMARK field (added for the ETL)
  };
  if (p.patch) p.patch(doc);
  return doc;
}

db.getCollection(COLL).insertMany([
  // ---------------------------------------------------------------- 14 GOOD returns
  vat({ ackNo: "120000000601", tin: "P0000000620", month: 9,  year: 2026, outputs: 200000, purchases: 300000,
        filedAt: "2026-10-14T10:43:38" }),                                            // credit -20,000 carried forward
  vat({ ackNo: "120000000602", tin: "P0000000620", month: 10, year: 2026, outputs: 350000, purchases: 150000,
        creditLastMonth: 20000, filedAt: "2026-11-12T09:15:00" }),                    // uses last month's credit
  vat({ ackNo: "120000000603", tin: "P0000000731", month: 9,  year: 2026, outputs: 500000, purchases: 200000,
        filedAt: "2026-10-20T11:00:00", updatedAt: "2026-10-21T08:00:00" }),          // latest version (status APRV)
  vat({ ackNo: "120000000604", tin: "P0000000731", month: 10, year: 2026, outputs: 450000, purchases: 250000,
        filedAt: "2026-11-10T14:00:00" }),                                            // incremental: taxpayer renamed
  vat({ ackNo: "120000000605", tin: "P0000000842", month: 9,  year: 2026, nil: true,
        filedAt: "2026-10-05T08:30:00" }),                                            // NIL return (all zero)
  vat({ ackNo: "120000000606", tin: "P0000000842", month: 10, year: 2026, nil: true,
        filedAt: "2026-11-04T08:30:00" }),
  vat({ ackNo: "120000000607", tin: "P0000000953", month: 9,  year: 2026, outputs: 800000, zeroRated: 150000,
        purchases: 400000, channel: "OFFC", rtnType: "original", filedAt: "2026-10-25T13:00:00" }),  // sanitize: "original"
  vat({ ackNo: "120000000608", tin: "P0000000953", month: 9,  year: 2026, outputs: 820000, zeroRated: 150000,
        purchases: 400000, rtnType: "A", filedAt: "2026-10-28T10:00:00" }),           // AMENDED return, sanitize: "A"
  vat({ ackNo: "120000000609", tin: "P0000001064", month: 8,  year: 2026, outputs: 300000, purchases: 100000,
        importedGoods: 50000, penalty: 500, interest: 1200, filedAt: "2026-10-15T09:00:00" }),   // LATE: 14 days
  vat({ ackNo: "120000000610", tin: "P0000001064", month: 9,  year: 2026, outputs: 280000, purchases: 120000,
        filedAt: "2026-10-22T09:00:00",
        patch: d => { d.taxableOutputDataDto.taxableOutputsValue = "2,80,000"; } }), // sanitize: thousand separators
  vat({ ackNo: "120000000611", tin: "P0000001175", month: 9,  year: 2026, outputs: 60000, purchases: 20000,
        channel: "mobl", filedAt: "2026-10-18T16:00:00",
        patch: d => { d.taxpayerCategory = " individual "; } }),                      // sanitize: case + spaces
  vat({ ackNo: "120000000612", tin: "P0000000953", month: 10, year: 2026, outputs: 900000, purchases: 500000,
        status: "PEND", filedAt: "2026-11-18T12:00:00" }),                            // incremental: PEND -> APRV
  vat({ ackNo: "120000000613", tin: "P0000000731", month: 8,  year: 2026, outputs: 420000, purchases: 180000,
        filedAt: "2026-09-18T10:00:00" }),
  vat({ ackNo: "120000000614", tin: "P0000000620", month: 8,  year: 2026, outputs: 150000, purchases: 90000,
        filedAt: "2026-09-20T10:00:00" }),

  // ---------------------------------------------------------------- 7 BAD returns
  vat({ ackNo: "120000000603", tin: "P0000000731", month: 9,  year: 2026, outputs: 500000, purchases: 200000,
        status: "PEND", filedAt: "2026-10-20T11:00:00" }),                            // DUPLICATE (older copy)
  vat({ ackNo: "120000000615", tin: "P0000000953", month: 8,  year: 2026, outputs: 200000, purchases: 100000,
        filedAt: "2026-09-25T10:00:00",
        patch: d => { d.taxableOutputDataDto.taxableOutputsValue = "20O000"; } }),   // CORRUPT: letter O
  vat({ ackNo: "120000000616", tin: "P0000001175", month: 8,  year: 2026, outputs: 40000, purchases: 10000,
        filedAt: "2026-09-21T10:00:00", patch: d => { d.rtnFromDate = "2026-08-32"; } }),   // CORRUPT date
  vat({ ackNo: "120000000617", tin: "X12345", name: "Unknown Traders", month: 9, year: 2026, outputs: 10000,
        purchases: 5000, filedAt: "2026-10-10T10:00:00" }),                          // VALIDATION: tin format
  vat({ ackNo: "120000000618", tin: "P0000000842", month: 8,  year: 2026, nil: true, filedAt: "2026-09-08T08:30:00",
        patch: d => { d.taxableOutputDataDto.taxableOutputsValue = "50000"; } }),    // VALIDATION: nil with outputs
  vat({ ackNo: "120000000619", tin: "P0000001175", month: 10, year: 2026, outputs: 70000, purchases: 30000,
        filedAt: "2026-11-15T10:00:00",
        patch: d => { d.vatComputationData.netOutputTaxValue = "99999.00"; } }),     // VALIDATION: wrong net (fixed in 02)
  vat({ ackNo: "120000000620", tin: "P0000000620", month: 10, year: 2026, outputs: 360000, purchases: 150000,
        creditLastMonth: 20000, rtnType: "Amended", formId: "99999", filedAt: "2026-11-16T10:00:00" }),  // ORPHAN form

  // ---------------------------------------------------------------- test document: never read (isTest)
  Object.assign(vat({ ackNo: "120000000699", tin: "P0000000620", month: 9, year: 2026, outputs: 1, purchases: 1,
        filedAt: "2026-10-01T10:00:00" }), { isTest: true })
]);

db.getCollection(COLL).createIndex({ updatedAt: 1 });   // watermark field: makes the incremental $match fast
db.getCollection(COLL).createIndex({ ackNo: 1 });
db.getCollection(COLL).createIndex({ tin: 1, year: 1, month: 1 });

print("");
print(`✔ ${DB_NAME}.${COLL} ${db.getCollection(COLL).countDocuments()} documents ` +
      `(${db.getCollection(COLL).countDocuments({ isTest: { $ne: true } })} without the test document)`);
print("  expected: 22 documents (21 without the test document)");
print("  Next: wait until the subscriber LAG = 0, then run sample-data/99_verify_bronze.js");
