# VAT pipeline ma use na thata generic methods (class wise)

Aa project nu framework (core, connector, validation) **badhi pipelines mate generic** che.
VAT return pipeline teno fakt ek bhag j vapre che. Aa file ma e methods ni list che je **framework ma che,
pan VAT return pipeline na run ma kyarey call thata nathi**.

> **Delete karta pehla vancho:** aa methods kharab nathi. Navi pipeline (staging load, SCD2 dimension,
> array lines, bija rules) mate e taiyar padya che. [PIPELINE_GUIDE.md](PIPELINE_GUIDE.md) section 8
> e j methods vapre che. Delete karo to e guide pan badalvi padse.

---

## Kem aa methods use nathi thata? 4 karan

VAT pipeline 4 nirnay le che. Darek nirnay framework no ek aakho rasto bandh kare che:

| VAT pipeline no nirnay | Code ma kya | Etle aa rasto VAT ma kyarey chalto nathi |
|---|---|---|
| `directLoad()` = **true** (aakho run ek transaction) | `VatReturnPipeline.directLoad()` | **Staging table** vado aakho load (section A) |
| Dimensions **SCD1** che (`dim_taxpayer` ma history nathi) | `loadDirect(... DIM_TAXPAYER ...)` | **SCD2** writer (section B) |
| Ek document = ek fact row (array ni lines nathi) | `run()` ma fakt `extractAndValidate` | **Derived rows** (section C) |
| Surrogate keys **SQL ni andar** shodhay che (`KeyLookup`) | `FACT_KEYS` | Spark ma **`addSurrogateKey`** join (section D) |

Section E ane F ma e rules / cleaning steps che je VAT na data ne joita nathi.
Section G ma **sacho dead code** che: kona pan pipeline mathi call thato nathi.

---

## A. Staging vado load (directLoad = false), VAT ma bandh

VAT `directLoad() = true` aape che, etle `DirectPostgresWriter` j vaparay che. Staging no rasto aakho band rahe che.

### `AbstractEtlPipeline`
| Method | Shu kare | Kyare joise |
|---|---|---|
| `load(context, rows, table, keys...)` | Staging table thi UPSERT | `directLoad() = false` vadi navi pipeline |
| `loadDateDimension(context, rows, dateColumn)` | `dim_date` bhare (staging thi) | Same. VAT `loadDateDimensionDirect` vapre che |

### `PostgresWriter` (aakho class VAT ma run nathi thato)
| Method | Shu kare |
|---|---|
| `upsert(...)` | Staging lakhe, merge SQL chalave, staging drop kare |
| `writeToStaging(...)` | `etl_staging.stg_...` table banave |
| `dropStagingTable(...)` | Staging table kadhi nakhe |
| `buildUpsertSql(...)` | `INSERT ... SELECT FROM staging ON CONFLICT ...` SQL banave |

> Class delete na karso. Teno bean `SoftDeleteWriter`, `Scd2Writer` ane `SourceCommitServiceImpl` ma inject thay che.
> `DirectPostgresWriter` pan teni constants `JOB_RUN_ID_COLUMN` ane `LOADED_AT_COLUMN` vapre che.

### `SoftDeleteWriter`
| Method / bhag | Shu kare |
|---|---|
| `applyWithStaging(...)` | Deleted keys staging ma lakhi ne ek UPDATE chalave |
| `stagingSql(...)` | E UPDATE nu SQL (fakt `SoftDeleteWriterTest` ma test thay che) |

VAT ma `apply()` hammesha `applyDirect()` ma j jaay che.

### `SourceCommitServiceImpl`
| Bhag | Shu kare |
|---|---|
| `saveRejectedRecords(...)` no **staging vado bhag** (`saveDirect` pachhi no code) | Rejects staging ma lakhi ne `saveFromStaging` call kare |

### `RejectedRecordRepository`
| Method / constant | Shu kare |
|---|---|
| `saveFromStaging(...)` | Rejects staging table mathi save kare |
| `NEW_ERRORS_FROM_STAGING`, `RECORDS_FROM_STAGING` | E method na SQL |

VAT `saveDirect(...)` vapre che.

### `DwhPipelineServiceImpl`
| Bhag | Shu kare |
|---|---|
| `runAndCommit(...)` no `else` bhag | Transaction vagar `run()` + `commit()` |

### Biji jagya
| Class | Member | Kem use nathi |
|---|---|---|
| `PipelineContext` | `nextStagingTableNumber()` | Fakt `PostgresWriter.writeToStaging` vapre che |
| `ApplicationProperties.Postgres` | `stagingSchema` | Same: fakt staging table nu schema |

---

## B. SCD Type 2 dimension (history), VAT ma bandh

`dim_taxpayer` SCD1 che: navo return aave to row overwrite thay, history rakhati nathi.

### `AbstractEtlPipeline`
| Method | Shu kare |
|---|---|
| `loadScd2(context, rows, spec)` | SCD2 dimension load kare |
| `applyDeletesScd2(context, batch, spec)` | SCD2 table ma current version close kari deleted mark kare |

### `Scd2Writer` (aakho class VAT ma use nathi)
| Member | Shu kare |
|---|---|
| `Scd2Spec` (record) | Table, business key, SCD2 columns, change-time column |
| `load(...)` | 3 SQL ek transaction ma: SCD1 update, juni version close, navi version insert |
| `scd1Columns(...)` | Kaya columns overwrite thay (SCD1) e shodhe |
| `scd1Sql`, `closeSql`, `insertSql` | Te 3 SQL banave |
| `distinct(...)` | `(t.a, t.b) IS DISTINCT FROM (s.a, s.b)` banave |
| `OPEN_END` | Current version no `valid_to` (`9999-12-31`) |

### `SoftDeleteWriter`
| Bhag | Shu kare |
|---|---|
| `directSql(...)` ane `stagingSql(...)` no `scd2 == true` bhag | `is_current = 'N'`, `valid_to = ...` umere |
| `applyDirect(...)` ma `if (scd2)` vado extra parameter | `valid_to` mate biju `deleted_at` |

---

## C. Derived rows (ek document mathi ghani lines), VAT ma bandh

Udaharan: ek invoice document ma `items[]` array hoy ane darek item ek row bane. VAT ma aavu nathi.

| Class | Method / bhag | Shu kare |
|---|---|---|
| `AbstractEtlPipeline` | `validateDerived(...)` | Banaveli lines ne validate kare. Kharab line ekli reject thay |
| `SourceProcessor` | `processDerived(...)` | E kaam nu andar nu implementation |
| `SourceProcessor` | `toBatch(...)` ma `sourceCount = -1` / `derived = true` vado bhag | Derived lines ni ganatri |
| `SourceBatch` | `derived` field | Set thay che, pan kyay vanchatu nathi (section G pan juo) |

---

## D. Spark ma surrogate key join, VAT ma bandh

VAT ma dimension ane fact **ek j transaction** ma lakhay che. Etle Spark navi dimension rows joi shaktu nathi,
ane keys `KeyLookup` thi SQL ni andar shodhay che.

| Class | Method | Shu kare | Kyare joise |
|---|---|---|---|
| `AbstractEtlPipeline` | `addSurrogateKey(...)` | Dimension join kari `*_key` column umere | Dimension pehla commit thai gayu hoy (staging pipeline) |
| `PostgresReader` | `addSurrogateKey(...)` | E j kaam nu implementation | Same |
| `PostgresReader` | `readTable(table)` **columns vagar** | Aakhu table vanche | VAT hammesha column aape che (`"form_id"`) |

---

## E. Data quality rules je VAT vaparatu nathi

### `DataQualityRules`
| Method | Rule | VAT ma kem nathi |
|---|---|---|
| `mustBePositive(column)` | Value > 0 joie | VAT amounts 0 hoi shake. Fakt `DataQualityEngineTest` ma vaparay che |
| `mustNotBeMoreThan(column, max)` | Value <= max | VAT ma upper limit no rule nathi |
| `mustBeBetween(column, min, max)` | min ane max vachche | Rate / percent mate. VAT ma `month` 1–12 `custom` rule thi check thay che |
| `mustBeValidEmail(column)` | Email format | VAT ma email field nathi |
| `EMAIL_REGEX` (constant) | Email pattern | Fakt `mustBeValidEmail` vapre che |

Te uprant `SparkTypes.RATE` fakt `mustBeBetween` ma vaparay che, etle VAT ma te pan use nathi.

---

## F. Cleaning steps je VAT vaparatu nathi

### `DataSanitizer`
| Method | Shu kare | Udaharan |
|---|---|---|
| `toProperCase(columns...)` | Darek shabd no pehlo akshar motu | `"rahul SHAH"` → `"Rahul Shah"` |
| `normalizeIndianPhone(columns...)` | Phone number ne 10 digit kare | `"+91 98765-43210"` → `"9876543210"` |
| `defaultIfNull(column, value)` | NULL hoy to default value | `currency` NULL → `"INR"` |
| `custom(column, cleaner)` | Potanu cleaning | `regexp_replace(c, " ", "")` |

---

## G. Sacho dead code: kona pan pipeline mathi call thato nathi

Section A–F na methods biji pipeline vapri shake. Aa members **aakha project ma kyay vanchata nathi**:

| Class | Member | Note |
|---|---|---|
| `JobMetrics` | `duplicateCount()` (getter) | `addDuplicates()` ganatri kare che, pan getter kyay vanchatu nathi (na log ma, na `meta_pipeline_run` ma) |
| `LineageCollector` | `describe(context)` | Comment kahe che "tests ane logs mate", pan koi test ke log call nathi kartu |
| `MetaAsset` | `displayName()` | Fakt `LineageCollector.describe` vapre che, je pote unused che |
| `SourceBatch` | `derived` field | `toBatch` set kare che, koi vanchatu nathi |

Aa 4 delete kari shakay. Pehla `JobMetrics.duplicateCount()` ne end-of-run log ma umervu saru rahe,
karan ke duplicates ni ganatri mahatvani che.

---

## H. Indirect use: VAT seedhu call nathi kartu, pan code chale che

Aa methods VAT na code ma dekhata nathi, pan biji method mathi call thay che. **Aa use thay che, delete na karso.**

| Class | Method | Kon call kare che |
|---|---|---|
| `SourceDefinition.Builder` | `sourceFilter(json)` | `excludeTestRecords()` (VAT aa vapre che) |
| `SourceDefinition.Builder` | `softDelete(flagField)` | `softDelete()` (VAT default field `isDeleted` vapre che) |
| `EtlPipeline` | `pipelineType()`, `sourceCollections()` | VAT override nathi kartu. Default `DwhPipelineServiceImpl` ma vaparay che |

---

## I. Enum values je VAT na vaparata nathi, pan rakhvana che

Aa values `03_postgres_tables.sql` na `CHECK (...)` constraint ni list sathe match thay che.
Enum ane DB ek sarkha rahe e mate **badlav na karso**.

| Enum | VAT ma use nathi |
|---|---|
| `PipelineType` | `SOURCE_TO_RAW`, `CURATED_TO_AGGREGATED` |
| `AssetType` | `VIEW`, `MATERIALIZED_VIEW`, `API`, `DATASET`, `REPORT` |
| `Platform` | `FILESYSTEM`, `API` |
| `LayerName` | `SOURCE`, `AGGREGATED` |
| `MappingType` | `AGGREGATED`, `CONSTANT`, `CONDITIONAL`, `MASKED`, `HASHED` |

---

## Tunk ma

| Section | Shu | Biji pipeline mate joise? | Delete safe? |
|---|---|---|---|
| A | Staging load | Ha (`directLoad = false`) | Na: guide ane bean wiring tute |
| B | SCD2 | Ha (history vado dimension) | Ha, jo SCD2 kyarey na joie (`Scd2Writer`, `loadScd2`, `applyDeletesScd2`, `PipelineToolkit` ma field) |
| C | Derived rows | Ha (array lines) | Ha, jo array lines kyarey na joie |
| D | `addSurrogateKey` | Ha (staging pipeline) | Ha, `AbstractEtlPipeline` + `PostgresReader` mathi |
| E, F | Rules / cleaning | Ha | Ha (nana methods; `mustBePositive` kadho to `DataQualityEngineTest` pan badlo) |
| G | Dead code | Na | **Ha** |
| H | Indirect | VAT pote vapre che | **Na** |
| I | Enum values | DB constraint | **Na** |

Kai pan delete karya pachhi `mvn clean test` chalavo ane
[BUILD_ORDER_PHASE_WISE.md](BUILD_ORDER_PHASE_WISE.md) ma te file ni line count / row sudharo.
