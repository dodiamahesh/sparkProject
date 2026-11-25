# Pipeline guide: aakhi VAT pipeline kevi rite chale che, ane shu badlavu hoy to kya badlavu

Aa file 3 prashno no jawab che:

1. **Navo rule** umervo hoy to kai file ma shu badlavu? (section 5)
2. Aa pipeline ma **ek thi vadhu collections** vanchva hoy to? (section 7)
3. Aa project ma **navi pipeline** banavvi hoy to kaya folder ma shu karvu? (section 8)

Pehla section 1 thi 4 vancho: aakho flow samjase, pachhi badlav saral lagshe.
Files kya kram ma lakhvi e mate: [BUILD_ORDER_PHASE_WISE.md](BUILD_ORDER_PHASE_WISE.md).

---

## 1. Ek nazar ma

```text
 MongoDB bronze_db.vat_return_coll                     PostgreSQL database "silver"
 (1 document = 1 filed VAT return)                     ┌─────────────────────────────────────────┐
            │                                          │ silver.dim_taxpayer      (job bhare)    │
            │ updatedAt > last_run_time                │ silver.dim_tax_period    (job bhare)    │
            ▼                                          │ silver.dim_date          (job bhare)    │
   ┌──────────────────── Spark (jar ni andar) ───┐     │ silver.dim_tax_form      (seeded)       │
   │ EXTRACT   MongoSourceReader                 │     │ silver.dim_vat_return_type   (seeded)   │
   │ SANITIZE  DataSanitizer (VatReturnSources)  │     │ silver.dim_vat_return_status (seeded)   │
   │ VALIDATE  DataQualityRules + Engine         │──►  │ silver.fact_vat_return   (1 row/ackNo)  │
   │ TRANSFORM VatReturnSchemaMapper             │     ├─────────────────────────────────────────┤
   │ LOAD      DirectPostgresWriter (no staging) │     │ meta.meta_pipeline / _run / _rejected   │
   └─────────────────────────────────────────────┘     │ meta.meta_error_master / lineage tables │
                                                       └─────────────────────────────────────────┘
   Badhu load + rejected rows + last_run_time = EK transaction (COMMIT badhu / ROLLBACK kai nahi)
```

- **Ek jar**, be rite chale: REST service (`POST /api/admin/dwh/pipelines/vat-return-pipeline/run`) ke batch
  mode (`java -jar dmj-dwh-1.0.0.jar --run-pipeline=vat-return-pipeline`, Airflow mate).
- **Incremental:** darek run fakt e documents vanche jenu `updatedAt` chhelli safal run pachhi badlayu che.
- **MongoDB fakt vanchay che**, kyarey lakhatu nathi. Tables `03_postgres_tables.sql` banave che, app nahi.

---

## 2. Folder map: kai folder nu shu kaam, ane kone badlavanu

| Folder (`src/main/java/com/tcs/dmj/dwh/...`) | Kaam | Navo rule / collection / pipeline mate badlavu pade? |
|---|---|---|
| `pipeline/vatreturn/` | **VAT pipeline**: shu vanchvu, rules, transform, load kram | **Haa: mukhya jagya** |
| `validation/` | sanitizer + rules nu engine (generic) | Na (navo *prakar* no rule joie to j) |
| `connector/source/` | Mongo read, window, soft delete, reject gate (generic) | Na |
| `connector/target/` | PostgreSQL writers (direct, staging, SCD2, soft delete) | Na |
| `core/` | pipeline contract + base class + registry + lineage | Na |
| `service/`, `service/impl/`, `service/dto/` | run flow, commit, lock, lineage save (interfaces + impl) | Na |
| `exception/` | **badhi custom exceptions** (`EtlException` base, `DataQualityGateException`, `PipelineConfigurationException`, `SoftDeletesNotAppliedException`, `PipelineNotFound...` ...) | Navo error prakar joie to j: `EtlException` extend karo |
| `repository/` | meta tables nu SQL | Na |
| `web/rest/`, `batch/`, `DmjDwhApp` | REST API, Airflow batch mode, `main` | Na |
| `config/`, `src/main/resources/*.yml` | Spark / Mongo / Postgres settings | Fakt navo DB / setting hoy to |
| `sample-data/vat-return-pipeline/` | `01` Mongo data, `02` badlav, `03` **tables + error seed** | **Haa** (table / column / error message) |
| `src/test/java/.../pipeline/vatreturn/` | rules no unit test | **Haa** (navo rule = navo test) |
| `airflow/dags/dmj_dwh_pipelines.yaml` | schedule | Fakt navi pipeline mate |

**Niyam:** pipeline-specific vastu fakt `pipeline/<naam>/`, `sample-data/<naam>/`, test ane Airflow YAML ma.
Baki nu badhu **framework** che: badhi pipelines e vapre che, ema badlav sauthi chhelle.

---

## 3. Ek run ma shu thay (step by step, class sathe)

`POST .../vat-return-pipeline/run` ke `--run-pipeline=vat-return-pipeline`:

| # | Step | Kon kare | Shu thay |
|---|---|---|---|
| 0 | Start-up | `PipelineRegistry`, `DwhPipelineServiceImpl.onStartup` | `@Component` pipelines shodhe, `meta.meta_pipeline` ma register |
| 1 | Lock | `PipelineRunLock` | PostgreSQL advisory lock: same pipeline be vaar ek saathe nahi (bijo = 409 / exit 1) |
| 2 | Run row | `MetaPipelineRunRepository.createRun` | `meta_pipeline_run` = RUNNING, window end = **atyare** |
| 3 | Transaction shuru | `DwhPipelineServiceImpl.execute` | `directLoad() = true` → aakho run ek `TransactionTemplate` ma |
| 4 | Window | `MetaPipelineRepository.window` | `updatedAt > last_run_time AND <= window end` (pehli run: upper bound j) |
| 5 | Extract | `MongoSourceReader` | `$match` MongoDB ma chale; badha fields **String** tarike; `isTest: true` skip |
| 6 | Sanitize | `VatReturnSources.sanitizer()` | nested JSON → columns (`fromJson`), trim, upper/lower, codes map, `"2,80,000"` → `280000` |
| 7 | Soft delete split | `SourceProcessor` | `isDeleted = true` vala documents validate nathi thata, delete list ma jay |
| 8 | Validate | `VatReturnSources.rules()` + `DataQualityEngine` | CORRUPT → VALIDATION → ORPHAN; pachhi same `ackNo` → DUPLICATE (latest `updatedAt` jite) |
| 9 | Reject gate | `SourceProcessor` | 30% thi vadhu kharab (≥ 10 rows hoy to) → `DataQualityGateException`, job FAILED, kai load nahi |
| 10 | Transform | `VatReturnSchemaMapper` | `toTyped` → `toDimTaxpayer`, `toDimTaxPeriod`, `toFactVatReturn` |
| 11 | Load dims | `loadDirect` → `DirectPostgresWriter` | `INSERT ... ON CONFLICT (tin) DO UPDATE` (SCD1), pachhi `dim_tax_period`, `dim_date` |
| 12 | Load fact | `loadDirect(..., FACT_KEYS)` | surrogate keys SQL ni andar sub-select thi (`KeyLookup`), kem ke nava taxpayers haju commit nathi |
| 13 | Soft delete | `applyDeletes` → `SoftDeleteWriter` | `fact_vat_return.is_deleted = TRUE` (row kadhati nathi) |
| 14 | Commit phase | `SourceCommitServiceImpl` | (a) badha deletes apply thaya? (b) rejected → `meta_rejected_record` (c) `last_run_time` = window end |
| 15 | COMMIT | `TransactionTemplate` | badhu ek saathe dekhay. Koi pan error → ROLLBACK: kai nathi badalatu, next run e j window fari vanche |
| 16 | Status | `PipelineStatus.of` | SUCCESS (kai reject nahi) / SUCCESS_WITH_FAILED / FAILED |
| 17 | Lineage | `LineageServiceImpl` | `meta_asset`, `meta_lineage` (jate shodhe), `meta_column_lineage` (`columnLineage()` mathi) |
| 18 | Cleanup | `PipelineContext.releaseResources` | Spark cache free, lock chhuto |

---

## 4. VAT pipeline ni 3 files

| File | Javabdari | Aaje shu che |
|---|---|---|
| `VatReturnSources.java` | **SHU vanchvu + kevi rite saaf karvu + kya rules** | `VAT_RETURNS` (25 fields, key `ackNo`, watermark `updatedAt`, `softDelete()`, `excludeTestRecords()`, `maxRejectPercent(30)`), `AMOUNTS` (26 amounts: column → nested JSON path), `sanitizer()`, `rules(formIds)` |
| `VatReturnSchemaMapper.java` | **TRANSFORM** (fakt Spark functions, koi I/O nahi) | `toTyped` (types + warehouse naam), `toDimTaxpayer` (tin dith latest), `toDimTaxPeriod`, `toFactVatReturn`, `factColumn("vatLevies") = "vat_levies"` |
| `VatReturnPipeline.java` | **ORCHESTRATION**: kram ane tables | `name()`, `sources()`, `targetTables()`, `columnLineage()`, `directLoad() = true`, `run()` |

### Data quality: 4 error types

| Type | Matlab | VAT udaharan |
|---|---|---|
| CORRUPT | value che pan type khoto | `taxableOutputsValue = "20O000"` (O letter) |
| VALIDATION | type saacho, business rule tute | `tin = "X12345"`, nil return pan outputs > 0 |
| ORPHAN | reference bija table ma nathi | `formId 99999` `dim_tax_form` ma nathi |
| DUPLICATE | same business key be vaar | same `ackNo`: latest `updatedAt` vado rahe |

Ek record ghana rules todi shake: `meta_rejected_record` ma **ek row**, `error_ids` = badha todela rules.
Darek rule no message **fix text** che ane `meta_error_master` ma ek j vaar hoy che (error_type + message = key).

---

## 5. HOW-TO: navo rule umervo

### 5.1 Checklist

| # | File | Shu karvu | Jaruri? |
|---|---|---|---|
| 1 | `pipeline/vatreturn/VatReturnSources.java` → `rules()` | rule umero | **Haa** |
| 2 | e j file → `VAT_RETURNS.fields(...)` | field haju na vanchata hoy to umero (top-level field) | field navo hoy to |
| 3 | e j file → `sanitizer()` | nested field hoy to `fromJson(...)` umero (**`trimAllAndBlankToNull()` pehla**); cleaning joie to (upper, mapValues ...) | nested / messy hoy to |
| 4 | `sample-data/vat-return-pipeline/03_postgres_tables.sql` PART 3 | `meta_error_master` ma navo message seed karo (exact same text) | bhalaman (na karo to pehla reject vakhte app jate umere) |
| 5 | `src/test/.../VatReturnPipelineRulesTest.java` | kharab document no test: sachi error aave | **Haa** |
| 6 | `sample-data/vat-return-pipeline/01_mongo_full_load.js` | ek kharab sample document (optional) | optional |
| 7 | `sample-data/vat-return-pipeline/vat-return-pipeline.md` section 6 | rule list update | optional |

Framework (`validation/`) ma kai badlavanu nathi, jo `DataQualityRules` ni hal ni method thi kaam thatu hoy.

### 5.2 Kai method kyare (`DataQualityRules`)

| Method | Type | Message (aa j text seed karvo) |
|---|---|---|
| `mustBeDecimal(c)` | CORRUPT | `c is not a valid number` |
| `mustBeInteger(c)` | CORRUPT | `c is not a whole number` |
| `mustBeDate(c)` | CORRUPT | `c is not a valid date (expected yyyy-MM-dd)` |
| `mustBeTimestamp(c)` | CORRUPT | `c is not a valid timestamp (expected yyyy-MM-ddTHH:mm:ss)` |
| `required(c...)` | VALIDATION | `c is required` |
| `mustBePositive(c)` | VALIDATION | `c must be greater than 0` |
| `mustNotBeNegative(c)` | VALIDATION | `c cannot be negative` |
| `mustNotBeMoreThan(c, max)` | VALIDATION | `c is more than <max>` (double: `5000.0`) |
| `mustBeBetween(c, min, max)` | VALIDATION | `c must be between <min> and <max>` (double: `1.0 and 12.0`) |
| `mustNotBeInFuture(c)` | VALIDATION | `c cannot be in the future` |
| `mustBeOneOf(c, values...)` | VALIDATION | `c must be one of [A, B]` |
| `mustMatch(c, regex, text)` | VALIDATION | `c <text>` |
| `mustBeValidEmail(c)` | VALIDATION | `c is not a valid email address` |
| `custom(type, message, failWhen)` | tame aapo | `<message>` (field_name = NULL) |
| `mustExistIn(c, dataset, column, name)` | ORPHAN | `c not found in <name>` |

`failWhen` = Spark condition je **true** hoy tyare record **kharab** che. Badha values String che, etle
sarkhamani pehla `cast(SparkTypes.MONEY / INT / DATE)` karo (khoti value cast par NULL bane, ane NULL sathe
sarkhamani false thay, etle CORRUPT rule ane business rule ek-bija ne nadta nathi).

### 5.3 Udaharan

```java
// VatReturnSources.rules(...) ni andar, return rules; pehla

// a) simple: delay 365 divas thi vadhu na hoy
rules.mustNotBeMoreThan("delayDays", 365);
//    message: "delayDays is more than 365.0"

// b) be fields ni sarkhamani (custom)
rules.custom(ErrorType.VALIDATION, "netTax must equal netPayable + penaltyAmount + interest",
        abs(col("netTax").cast(SparkTypes.MONEY)
                .minus(col("netPayable").cast(SparkTypes.MONEY)
                        .plus(col("penaltyAmount").cast(SparkTypes.MONEY))
                        .plus(col("interest").cast(SparkTypes.MONEY)))).gt(0.01));

// c) late return par penalty hovi j joie
rules.custom(ErrorType.VALIDATION, "a late return must have a penalty",
        col("delayDays").cast(SparkTypes.INT).gt(0)
                .and(coalesce(col("penaltyAmount").cast(SparkTypes.MONEY), lit(0)).leq(0)));

// d) navo nested field + rule (e.g. rtnRegisterVatDTO.branchId)
//    sanitizer():  .fromJson("branchId", "rtnRegisterVatDTO", "$.branchId")   <- trimAllAndBlankToNull() pehla
//    rules():
rules.required("branchId").mustBeInteger("branchId");
```

(c) mate `VatReturnSources.java` ma `import static org.apache.spark.sql.functions.coalesce;` ane `...functions.lit;`
umerva pade; `abs`, `col` pehla thi imported che.

`03_postgres_tables.sql` PART 3 ma (b) mate:

```sql
    ('VALIDATION', NULL,  'netTax must equal netPayable + penaltyAmount + interest'),
```

Test (`VatReturnPipelineRulesTest.everyBadReturnIsRejectedWithItsOwnError` jevo):

```java
document("7", "120000000624", d -> d.put("netTax", "1.00"))
// expected: "120000000624" -> "netTax must equal netPayable + penaltyAmount + interest"
```

### 5.4 Dhyan rakhvanu

- **Message ma record ni value na mukvi** (`"tin " + value` nahi). Value alag `field_values` ma save thay che.
- **Message no text badlo = navo `error_id`.** Juno message `meta_error_master` ma rahe (history).
- **ORPHAN** lookup: `readTable("silver.dim_x", "col")` fakt **commit thayelo** data vanche. Aa j run ma lakhata
  dimension mate `KeyLookup` vapro (section 6.3), ORPHAN nahi.
- **Reject gate:** navo kadak rule ghana juna records reject kare to 30% gate job ne FAILED kari shake.
  Pehla data par chakaso, jarur hoy to `maxRejectPercent(...)` badlo.
- Juna **rejected** records fari tyare j vanchay jyare source ma emnu `updatedAt` badlay.
- Rule na badle fakt **cleaning** joie (e.g. `"inet"` → `INET`)? To `sanitizer()` ma: `toUpperCase`, `toLowerCase`,
  `toProperCase`, `mapValues`, `defaultIfNull`, `removeThousandSeparators`, `custom(column, c -> ...)`.

---

## 6. HOW-TO: navo field / column / dimension

### 6.1 Hal na table ma navo column (e.g. `fact_vat_return.branch_id`)

| # | File | Shu |
|---|---|---|
| 1 | `VatReturnSources.java` | `fields(...)` (top-level) ke `sanitizer().fromJson(...)` (nested); rules |
| 2 | `VatReturnSchemaMapper.toTyped` | `col("branchId").cast(SparkTypes.INT).as("branch_id")` |
| 3 | `VatReturnSchemaMapper.toFactVatReturn` | select list ma `col("branch_id")` |
| 4 | `03_postgres_tables.sql` | `CREATE TABLE` ma column + juna DB mate `ALTER TABLE silver.fact_vat_return ADD COLUMN IF NOT EXISTS branch_id INT;` |
| 5 | test + `01` sample | |
| 6 | `VatReturnPipeline.columnLineage()` | critical field hoy to `ColumnLineage.of(...)` |

> `DirectPostgresWriter` dataset na **badha** columns lakhe che. Dataset ma column che pan table ma nathi →
> SQL error → aakho run ROLLBACK. Etle pehla `03` (ALTER) chalavo, pachhi navo jar.

Navo **amount** umervo saral che: `AMOUNTS` ma ek line
`amount("myAmount", "vatComputationData", "$.myAmountValue");` → `fromJson`, `mustBeDecimal`,
`removeThousandSeparators`, typed + fact column (`my_amount`) **jate** bane. Table ma fakt column + error seed
(`myAmount is not a valid number`) umero.

### 6.2 Navo seeded reference dimension (e.g. `dim_jurisdiction`)

1. `03`: `CREATE TABLE silver.dim_jurisdiction (jurisdiction_key BIGSERIAL PRIMARY KEY, jurisdiction_code VARCHAR(10) NOT NULL UNIQUE, ...)` + `INSERT ... ON CONFLICT DO NOTHING`; fact ma `jurisdiction_key BIGINT NOT NULL REFERENCES ...`.
2. `VatReturnPipeline`: `static final String DIM_JURISDICTION = "silver.dim_jurisdiction";` ane `FACT_KEYS` ma
   `new KeyLookup("jurisdiction_code", DIM_JURISDICTION, "jurisdiction_code", "jurisdiction_key")`.
3. `toFactVatReturn` ma `col("jurisdiction_code")` (writer ene key ma badli nakhe, code pote table ma nathi jato).
4. Rule: `mustBeOneOf(...)` ke `mustExistIn("taxpayerJurisdiction", readTable(DIM_JURISDICTION, "jurisdiction_code"), "jurisdiction_code", "dim_jurisdiction")`.
   Rule vagar ajanyo code = `KeyLookup` NULL = NOT NULL error = **aakho run ROLLBACK**. Etle reference dims mate rule hamesha rakho.

### 6.3 Data mathi banto dimension (jem `dim_taxpayer`)

Mapper ma `latestPer(typed, "key")` thi key dith ek row, `run()` ma fact **pehla** `loadDirect(context, rows, TABLE, List.of("key"))`,
`targetTables()` ma table. Fact mate `KeyLookup` (dimension aa j transaction ma lakhay che, Spark ne dekhatu nathi; SQL sub-select ne dekhay che).

---

## 7. HOW-TO: aa pipeline ma ek thi vadhu collections

Udaharan: navo collection **`taxpayer_master_coll`** (taxpayer registration master: `tin`, `taxpayerName`,
`taxpayerCategory`, `taxpayerJurisdiction`, `registrationDate`, `updatedAt`). Have `dim_taxpayer` master
mathi bharay, ane returns no `tin` master/dimension ma hovo joie (ORPHAN).

### 7.1 Checklist

| # | File | Shu karvu |
|---|---|---|
| 1 | `pipeline/vatreturn/VatReturnSources.java` (ke navi `TaxpayerMasterSources.java` e j package ma) | navo `SourceDefinition` constant + eno `sanitizer` + `rules` |
| 2 | `VatReturnPipeline.sources()` | `List.of(VatReturnSources.VAT_RETURNS, VatReturnSources.TAXPAYERS)` |
| 3 | `VatReturnPipeline.run()` | darek collection mate `extractAndValidate`; pehla master/dims, pachhi fact; darek soft-delete source mate `applyDeletes` |
| 4 | `VatReturnSchemaMapper.java` | navo `toDimTaxpayerFromMaster(...)` |
| 5 | `VatReturnPipeline.targetTables()` / `columnLineage()` | nava tables / mappings |
| 6 | `03_postgres_tables.sql` | nava tables / columns + navo collection na **badha** error messages (DUPLICATE sathe) |
| 7 | `01_mongo_full_load.js` / `02_mongo_incremental.js` | navo collection drop + insert + `createIndex({updatedAt: 1})` |
| 8 | test | navo collection na rules no test |
| 9 | `vat-return-pipeline.md` | design update |

### 7.2 Code

```java
// VatReturnSources.java
static final SourceDefinition TAXPAYERS = SourceDefinition.builder("taxpayer_master_coll")
        .fields("tin", "taxpayerName", "taxpayerCategory", "taxpayerJurisdiction", "registrationDate", "updatedAt")
        .businessKey("tin")
        .latestRecordColumn("updatedAt")      // BSON Date, indexed, source darek insert / update par set kare
        .softDelete()
        .excludeTestRecords()
        .maxRejectPercent(30)
        .description("Registered taxpayers (master data)")
        .owner("registration-team@demo.com")
        .build();

static DataSanitizer taxpayerSanitizer() {
    return DataSanitizer.create()
            .trimAllAndBlankToNull()
            .toUpperCase("tin", "taxpayerCategory")
            .mapValues("taxpayerCategory", Map.of("INDIVIDUAL", "Individual", "NON-INDIVIDUAL", "Non-Individual"));
}

static DataQualityRules taxpayerRules() {
    return DataQualityRules.create()
            .mustBeDate("registrationDate")
            .required("tin", "taxpayerName", "taxpayerCategory")
            .mustMatch("tin", "^P[0-9]{10}$", "must look like P0000000001")
            .mustBeOneOf("taxpayerCategory", CATEGORIES);
}
```

```java
// VatReturnPipeline.run()
@Override
public void run(PipelineContext context) {
    // 1. master pehla
    SourceBatch taxpayers = extractAndValidate(context, VatReturnSources.TAXPAYERS,
            VatReturnSources.taxpayerSanitizer(), VatReturnSources.taxpayerRules());

    // 2. returns: tin = aa run na valid taxpayers + pehla commit thayela dim_taxpayer
    Dataset<Row> knownTins = taxpayers.valid().select("tin")
            .union(readTable(DIM_TAXPAYER, "tin"));
    SourceBatch returns = extractAndValidate(context, VatReturnSources.VAT_RETURNS,
            VatReturnSources.sanitizer(),
            VatReturnSources.rules(readTable(DIM_TAX_FORM, "form_id"), knownTins));
    // rules(...) ne bijo parameter aapo: static DataQualityRules rules(Dataset<Row> formIds, Dataset<Row> tins)
    //   ane andar: .mustExistIn("tin", tins, "tin", "taxpayer_master_coll / dim_taxpayer")
    //   (test VatReturnPipelineRulesTest.check(...) ma pan e bijo dataset aapvo pade)

    // 3. dims (master mathi), pachhi period / date / fact
    loadDirect(context, toDimTaxpayerFromMaster(taxpayers.valid()), DIM_TAXPAYER, List.of("tin"));
    Dataset<Row> typed = context.cache(toTyped(returns.valid()));
    loadDirect(context, toDimTaxPeriod(typed), DIM_TAX_PERIOD, List.of("period_key"));
    loadDateDimensionDirect(context, typed, "filed_date", "due_date", "ack_date");
    loadDirect(context, toFactVatReturn(typed), FACT_VAT_RETURN, List.of("ack_no"), FACT_KEYS);

    // 4. soft deletes: DAREK softDelete() vala source mate (na karo to job FAILED thay)
    applyDeletes(context, returns, FACT_VAT_RETURN, "ack_no");
    applyDeletes(context, taxpayers, DIM_TAXPAYER, "tin");    // dim_taxpayer ma is_deleted / deleted_at columns joie
}
```

### 7.3 Mahatva na niyam

| Niyam | Kem |
|---|---|
| **Ek `last_run_time` aakhi pipeline mate** | badha collections e j window vanche, ane run safal thay tyare j aagal khase |
| **Be incremental batch ne seedha `join` na karo** | aa run ma fakt *badlayela* documents che: return badlayu pan master nahi, to join ma taxpayer madse nahi. Bija collection ni mahiti **silver table** mathi lo (`KeyLookup` ke `readTable`) |
| **Ek table no ek j "owner" source** | `dim_taxpayer` ne master ane returns banne lakhe to chhelle lakhnar jite. Ek pasand karo (upar: master), ane `toDimTaxpayer(typed)` vado load kadhi nakho |
| **Dims pehla, fact pachhi** | `KeyLookup` sub-select ne dimension ni row joie |
| **Darek soft-delete source mate `applyDeletes`** | nahi to `SoftDeletesNotAppliedException`: job FAILED (delete gumay nahi) |
| **Badha collections ek j Mongo database ma** | app ek j `MONGO_DATABASE` vanche (`MongoSourceReader`) |
| **Reject gate collection dith** | darek `SourceDefinition` no potano `maxRejectPercent` |
| **Collection naam = lineage + rejected table** | `meta_rejected_record.source_collection` ma collection naam aave che |

### 7.4 Array field (ek document, ghani lines)

Document ni andar list hoy (e.g. `lines: [...]`) ane darek line ek row joie: `explode` thi rows banavo
(`_id` = `"<_id>#<n>"`, `_raw_record` = line no JSON), pachhi
`validateDerived(context, LINES_DEFINITION, rows, lineRules)`. Kharab line ekli reject thay, baki document load thay.
`LINES_DEFINITION` = `SourceDefinition.builder("vat_return_coll.lines")...` (fields, line nu business key).

---

## 8. HOW-TO: navi pipeline banavvi

Udaharan: **`vat-payment-pipeline`**: MongoDB `vat_payment_coll` → `silver.fact_vat_payment`
(`dim_taxpayer` VAT pipeline nu j vapre).

### 8.1 Kaya folder ma shu

| # | Folder / file | Shu karvu |
|---|---|---|
| 1 | `src/main/java/com/tcs/dmj/dwh/pipeline/vatpayment/VatPaymentSources.java` | **navi**: `SourceDefinition`, `sanitizer()`, `rules()` |
| 2 | `.../pipeline/vatpayment/VatPaymentSchemaMapper.java` | **navi**: transform |
| 3 | `.../pipeline/vatpayment/VatPaymentPipeline.java` | **navi**: `@Component`, `extends AbstractEtlPipeline` |
| 4 | `src/test/java/com/tcs/dmj/dwh/pipeline/vatpayment/VatPaymentRulesTest.java` | **navi**: `VatReturnPipelineRulesTest` jevo |
| 5 | `sample-data/vat-payment-pipeline/01_mongo_full_load.js` | **navi**: sample data (sara + kharab) + `createIndex({updatedAt: 1})` |
| 6 | `sample-data/vat-payment-pipeline/02_mongo_incremental.js` | **navi**: badlav (`updatedAt = now`) |
| 7 | `sample-data/vat-payment-pipeline/03_postgres_tables.sql` | **navi**: PART 1 meta (VAT `03` mathi copy, re-run safe) + PART 2 tables + PART 3 error seed |
| 8 | `airflow/dags/dmj_dwh_pipelines.yaml` | navi entry: `vat-payment-pipeline: depends_on: [vat-return-pipeline]` |
| 9 | `sample-data/README.md` "Jobs" table, `vat-payment-pipeline.md` | docs (optional) |
| — | `core/`, `connector/`, `validation/`, `exception/`, `service/`, `repository/`, `web/`, `batch/`, `config/` | **kai nahi**: `PipelineRegistry` navi `@Component` jate shodhe, REST URL ane `--run-pipeline` tarat chale |

Pachhi: `03` chalavo → jar build → app restart (start-up par `meta_pipeline` ma register) →
`POST /api/admin/dwh/pipelines/vat-payment-pipeline/run`.

### 8.2 Pehla 3 nirnay

| Prashna | Vikalp |
|---|---|
| **Naam** | unique, ≤ 30 akshar, fakt `A-Z a-z 0-9 . _ -` (Airflow DAG id pan aa j). Nahi to app start nahi thay |
| **`directLoad()`** | `true` = VAT jevu: staging vagar, aakho run **ek transaction** (hajaro-laakh rows sudhi saru). API: `loadDirect`, `loadDateDimensionDirect`, `KeyLookup`. `false` (default) = `etl_staging` thi, darek table alag commit, **moti volume** mate saru. API: `load`, `loadDateDimension`, `addSurrogateKey`, `loadScd2`, `applyDeletesScd2`. **Be mix na karo** (`loadDirect` ne active transaction joie) |
| **Mongo database** | VAT jevu `bronze_db`? Kai nahi. Bijo database hoy to e pipeline alag process ma chalavo: Airflow YAML ma `env: MONGO_DATABASE: <db>` (REST service ek j database vanche) |

### 8.3 Template code

```java
// VatPaymentSources.java
package com.tcs.dmj.dwh.pipeline.vatpayment;

final class VatPaymentSources {
    private VatPaymentSources() {
    }

    static final SourceDefinition PAYMENTS = SourceDefinition.builder("vat_payment_coll")
            .fields("paymentNo", "tin", "ackNo", "amount", "paidAt", "channel", "updatedAt")
            .businessKey("paymentNo")
            .latestRecordColumn("updatedAt")
            .softDelete()
            .excludeTestRecords()
            .maxRejectPercent(30)
            .description("VAT payments made by taxpayers")
            .owner("vat-payments-team@demo.com")
            .sla("daily")
            .build();

    static DataSanitizer sanitizer() {
        return DataSanitizer.create()
                .trimAllAndBlankToNull()
                .toUpperCase("tin", "channel")
                .removeThousandSeparators("amount");
    }

    /** @param tins every tin of silver.dim_taxpayer (filled by vat-return-pipeline, already committed) */
    static DataQualityRules rules(Dataset<Row> tins) {
        return DataQualityRules.create()
                .mustBeDecimal("amount")
                .mustBeTimestamp("paidAt")
                .required("paymentNo", "tin", "amount", "paidAt", "channel")
                .mustBePositive("amount")
                .mustBeOneOf("channel", "BANK", "MOMO", "CARD")
                .mustExistIn("tin", tins, "tin", "dim_taxpayer");
    }
}
```

```java
// VatPaymentSchemaMapper.java
package com.tcs.dmj.dwh.pipeline.vatpayment;

final class VatPaymentSchemaMapper {
    private VatPaymentSchemaMapper() {
    }

    static Dataset<Row> toTyped(Dataset<Row> valid) {
        return valid.select(
                col("paymentNo").as("payment_no"),
                col("tin"),
                col("ackNo").as("ack_no"),
                col("amount").cast(SparkTypes.MONEY).as("amount"),
                regexp_replace(col("paidAt"), "Z$", "").cast(DataTypes.TimestampNTZType).as("paid_at"),
                to_date(substring(col("paidAt"), 1, 10)).as("paid_date"),
                col("channel").as("channel_code"));
    }

    /** tin stays a business key: the writer turns it into taxpayer_key inside the SQL (KeyLookup). */
    static Dataset<Row> toFactVatPayment(Dataset<Row> typed) {
        return typed.select(col("payment_no"), col("tin"), col("ack_no"), col("amount"), col("paid_at"),
                DateDimension.dateKey(col("paid_date")).as("paid_date_key"), col("channel_code"));
    }
}
```

```java
// VatPaymentPipeline.java
package com.tcs.dmj.dwh.pipeline.vatpayment;

@Component
public class VatPaymentPipeline extends AbstractEtlPipeline {

    static final String DIM_TAXPAYER = "silver.dim_taxpayer";
    static final String FACT_VAT_PAYMENT = "silver.fact_vat_payment";

    private static final KeyLookup[] FACT_KEYS = {
            new KeyLookup("tin", DIM_TAXPAYER, "tin", "taxpayer_key")
    };

    public VatPaymentPipeline(PipelineToolkit toolkit) {
        super(toolkit);
    }

    @Override public String name() { return "vat-payment-pipeline"; }
    @Override public String description() { return "VAT payments: vat_payment_coll -> fact_vat_payment"; }
    @Override public List<SourceDefinition> sources() { return List.of(VatPaymentSources.PAYMENTS); }
    @Override public List<String> targetTables() { return List.of(DateDimension.TABLE, FACT_VAT_PAYMENT); }
    @Override public boolean directLoad() { return true; }

    @Override
    public void run(PipelineContext context) {
        SourceBatch payments = extractAndValidate(context, VatPaymentSources.PAYMENTS,
                VatPaymentSources.sanitizer(), VatPaymentSources.rules(readTable(DIM_TAXPAYER, "tin")));
        Dataset<Row> typed = context.cache(VatPaymentSchemaMapper.toTyped(payments.valid()));

        loadDateDimensionDirect(context, typed, "paid_date");
        loadDirect(context, VatPaymentSchemaMapper.toFactVatPayment(typed), FACT_VAT_PAYMENT,
                List.of("payment_no"), FACT_KEYS);
        applyDeletes(context, payments, FACT_VAT_PAYMENT, "payment_no");
    }
}
```

(Imports: `VatReturnPipeline.java` / `VatReturnSources.java` / `VatReturnSchemaMapper.java` jeva j:
`SourceBatch`, `SourceDefinition`, `DateDimension`, `DirectPostgresWriter.KeyLookup`, `AbstractEtlPipeline`,
`PipelineContext`, `PipelineToolkit`, `DataSanitizer`, `DataQualityRules`, `SparkTypes`, Spark `Dataset`, `Row`,
`DataTypes`, `functions.*`, Spring `@Component`, `java.util.List`.)

```sql
-- 03_postgres_tables.sql, PART 2
CREATE TABLE IF NOT EXISTS silver.fact_vat_payment (
    vat_payment_key  BIGSERIAL     PRIMARY KEY,
    payment_no       VARCHAR(30)   NOT NULL UNIQUE,     -- business key: ON CONFLICT (payment_no)
    taxpayer_key     BIGINT        NOT NULL REFERENCES silver.dim_taxpayer (taxpayer_key),
    ack_no           VARCHAR(20),
    amount           NUMERIC(18,2) NOT NULL,
    paid_at          TIMESTAMP     NOT NULL,
    paid_date_key    INT           NOT NULL REFERENCES silver.dim_date (date_key),
    channel_code     VARCHAR(10)   NOT NULL,
    job_run_id       BIGINT,                            -- writer jate bhare
    loaded_at        TIMESTAMP                          -- writer jate bhare
);
ALTER TABLE silver.fact_vat_payment ADD COLUMN IF NOT EXISTS is_deleted BOOLEAN NOT NULL DEFAULT FALSE,
                                    ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMP;   -- softDelete() mate

-- PART 3: darek rule no message (DataQualityRules na exact text)
INSERT INTO meta.meta_error_master (error_type, field_name, error_message)
SELECT v.* FROM (VALUES
    ('CORRUPT',    'amount',    'amount is not a valid number'),
    ('CORRUPT',    'paidAt',    'paidAt is not a valid timestamp (expected yyyy-MM-ddTHH:mm:ss)'),
    ('VALIDATION', 'paymentNo', 'paymentNo is required'),
    ('VALIDATION', 'tin',       'tin is required'),
    ('VALIDATION', 'amount',    'amount is required'),
    ('VALIDATION', 'paidAt',    'paidAt is required'),
    ('VALIDATION', 'channel',   'channel is required'),
    ('VALIDATION', 'amount',    'amount must be greater than 0'),
    ('VALIDATION', 'channel',   'channel must be one of [BANK, MOMO, CARD]'),
    ('ORPHAN',     'tin',       'tin not found in dim_taxpayer'),
    ('DUPLICATE',  'paymentNo', 'another record with the same paymentNo was kept (latest updatedAt wins)')
) AS v (error_type, field_name, error_message)
WHERE NOT EXISTS (SELECT 1 FROM meta.meta_error_master e
                   WHERE e.error_type = v.error_type AND e.error_message = v.error_message)
ON CONFLICT (error_type, error_message) DO NOTHING;
```

### 8.4 Target table na niyam

- Spark dataset na column naam = table na column naam (`KeyLookup` na source column sivay: e table ma nathi jata).
- Business key column(s) par `UNIQUE` (`ON CONFLICT (key)` mate).
- Badha tables ma `job_run_id BIGINT`, `loaded_at TIMESTAMP` (writer jate bhare).
- `softDelete()` vala source na tables ma `is_deleted BOOLEAN NOT NULL DEFAULT FALSE`, `deleted_at TIMESTAMP`.
- `KeyLookup` vala fact columns `NOT NULL`: dimension ma row na hoy to run ROLLBACK (gap kyarey chupe nahi).

### 8.5 Pipeline kadhvi hoy to

`pipeline/<naam>/` package, eno test, `sample-data/<naam>/` ane YAML entry delete karo. Restart par
`meta.meta_pipeline.active_flag = 'N'` jate thay; run history ane silver tables rahe (jate drop karo).

---

## 9. Cheat sheet: "mare X karvu che" → aa files

| Mare karvu che | Files |
|---|---|
| Validation rule umervo / badlavo | `VatReturnSources.rules()`, `03` PART 3, `VatReturnPipelineRulesTest` |
| Cleaning (upper, map, trim ...) | `VatReturnSources.sanitizer()` (+ test) |
| Source no navo field vanchvo | `VatReturnSources.VAT_RETURNS.fields()` ke `sanitizer().fromJson()` |
| Navo amount (fact measure) | `VatReturnSources.AMOUNTS` + `03` (column + error seed) |
| Fact / dim ma navo column | `VatReturnSchemaMapper` (`toTyped` + `toFact...`) + `03` (`ALTER TABLE ... ADD COLUMN IF NOT EXISTS`) |
| Navo reference dimension | `03` (table + seed + FK), `VatReturnPipeline.FACT_KEYS`, rule (`mustBeOneOf` / `mustExistIn`) |
| Reject % badlavo | `VatReturnSources.VAT_RETURNS.maxRejectPercent(...)` (gate na minimum rows: `application.yml` `reject-gate-min-records`) |
| Navo collection aa pipeline ma | section 7 |
| Navi pipeline | section 8 |
| Schedule badlavo | `airflow/dags/dmj_dwh_pipelines.yaml` |
| DB / Mongo connection | `application-dev.yml` / env variables (`POSTGRES_URL`, `MONGO_URI`, `MONGO_DATABASE` ...) |
| Spark settings (cores, partitions) | `application.yml` `application.dwh.spark.config`, prod ma `SPARK_MASTER` |
| Critical column ni lineage | `VatReturnPipeline.columnLineage()` |
| Navo custom exception | `exception/` ma navi class (`extends EtlException`); REST ma alag HTTP status joie to `web/rest/errors/ExceptionTranslator` ma `@ExceptionHandler`. Run vakhte throw thay to `meta_pipeline_run.error_code` = class naam |

---

## 10. Badlav pachhi test kram

1. `mvn test` → badha unit tests pass.
2. `03_postgres_tables.sql` fari chalavo (re-run safe: nava tables / columns / messages umere).
3. `01` (+ navo sample) → run → `meta_pipeline_run` na counts ane `meta_rejected_record` check.
4. `02` → run → incremental counts check.
5. Run `FAILED` thay to: `SELECT error_code, error_message FROM meta.meta_pipeline_run ORDER BY pipeline_run_id DESC LIMIT 1;`
   (directLoad ma FAILED = kai lakhatu nathi, `last_run_time` khasatu nathi: sudharine fari chalavo).
