# vat-return-pipeline: design and ETL walk-through

MongoDB `vat_return_coll` (one document per **filed monthly VAT return**) → PostgreSQL `silver` star schema.
This job writes **directly into the silver tables: no `etl_staging.stg_*` tables**.

| | |
|---|---|
| Pipeline name | `vat-return-pipeline` → `POST /api/admin/dwh/pipelines/vat-return-pipeline/run` |
| Source | MongoDB `bronze_db.vat_return_coll` (default `MONGO_DATABASE`; the name clashes with no other job) |
| Target | `silver`: 6 dimensions + shared `silver.dim_date` + 1 fact |
| Model | **Star schema** |
| Load | Incremental by `updatedAt` watermark, one transaction for the whole run (all-or-nothing) |
| Files | `01_mongo_full_load.js`, `02_mongo_incremental.js`, `03_postgres_tables.sql` (this folder) |

Status: **implemented** (`src/main/java/com/tcs/dmj/dwh/pipeline/vatreturn`), tested end to end with this sample data (section 10).

---

## 1. The source document

One document = one return filed by one taxpayer for one month (`ackNo` = acknowledgement number of the filing).
Shape (shortened):

```text
tin, taxpayerName, taxpayerCategory, taxpayerJurisdiction, taxpayerId     -> who filed
rtnFromDate, rtnToDate, month, year, rtnDueDt                             -> which period
rtnType (Original / Amended), isNilReturn, fileFrom (INET ...), ackNo     -> what kind of filing
taxTypeId (101), formId (12027)                                           -> which form
taxableOutputDataDto { taxableOutputsValue, vatLeveies, zeroRated..., relief..., exemptSup... }   -> sales
purchExpnsData       { local purchases, imported goods / services, total input, tax on purchases } -> inputs
vatComputationData   { total output / input tax, deductible input tax, net output, withholding,
                       credit last month, net payable }                                             -> VAT due
rtnRegisterVatDTO    { rtnStatus (APRV), crtDt (filed at), isLateRtn, isActive, credits b/f c/f,
                       genAcknowledgementVatHeader { ackDate, ackStatus } }                         -> register
penaltyAmount, interest, netTax, delayDays                                                          -> totals
additionalInputData  { stock bought standard / flat rate: value + levies }
updatedAt                                                                       -> ADDED for the ETL (see below)
```

**Not read:** `_class`, `prevRtnRegisterVatDTO`, `taxTypeId`. The form decides the tax type, and
`dim_tax_form` holds both.

### `updatedAt` must be added by the source

The real document has **no top-level change timestamp**. The incremental watermark needs one: the source must
set `updatedAt` (BSON Date) on every insert **and update**, with an index `{updatedAt: 1}`. The sample data adds it.
`rtnRegisterVatDTO.crtDt` is not enough: it is the creation time, so a later status change (PEND → APRV) would
never be read again.

### How the Spark connector reads it (checked)

Every field is read as String. Nested objects arrive as JSON text, so the framework `DataSanitizer.fromJson(...)`
copies each value into its own column. Checked on the sample data:

| Mongo value | String in Spark |
|---|---|
| `formId: NumberLong(12027)` | `"12027"` |
| `isNilReturn: false`, `month: 9` | `"false"`, `"9"` |
| `updatedAt: ISODate(...)` | `"2026-10-14T10:43:38Z"` |
| nested `crtDt: ISODate(...)` inside `rtnRegisterVatDTO` | `fromJson("filedAt", "rtnRegisterVatDTO", "$.crtDt")` → `"2026-10-14T10:43:38Z"` |
| nested `branchId: NumberLong(1)` | `"1"` |

---

## 2. Dimensional model: STAR schema

```text
                   dim_tax_period      dim_taxpayer
                             \           /
    dim_tax_form ------------ fact_vat_return ------------ dim_filing_channel
                             /     |      \
         dim_vat_return_type       |       dim_vat_return_status
                                   |
                          silver.dim_date   (3 roles: filed / due / acknowledgement date)
```

### Tables (7 dimensions incl. the shared date, 1 fact)

| Table | Grain / key | Type | Filled by |
|---|---|---|---|
| `dim_taxpayer` | one row per `tin` | SCD1 (latest name / category / jurisdiction) | the job, from the returns |
| `dim_tax_period` | one row per month, `period_key = yyyyMM` (+ due date) | static once created | the job, from the returns |
| `dim_tax_form` | one row per `form_id` (+ tax type) | reference | **seeded** in `03` (12027, 12028) |
| `dim_vat_return_type` | Original / Amended | reference | **seeded** |
| `dim_vat_return_status` | APRV / PEND / REJT | reference | **seeded** |
| `dim_filing_channel` | INET / OFFC / MOBL | reference | **seeded** |
| `silver.dim_date` | one row per day (shared by all jobs) | conformed | the job (filed, due, ack dates) |
| **`fact_vat_return`** | **one row per filed return (`ack_no`)** | transaction fact | the job, UPSERT on `ack_no` |

**Fact measures (26):** taxable outputs, VAT levies, zero-rated, relief, exempt supplies; local purchases,
imported goods / services, total input, input tax on purchases; total output tax, total / deductible input tax,
net output tax, withholding credit, credit last month, net payable, credit brought / carried forward; penalty,
interest, net tax; 4 stock-bought values.
**Flags:** `is_nil_return`, `is_late_return`, `is_active`, `delay_days`. **Degenerate dimension:** `ack_no`.

### Why STAR and not snowflake?

* Every dimension joins the fact **directly**; there is no dimension → dimension join. Reports such as "VAT payable
  per jurisdiction per month" are one join per dimension.
* The only natural hierarchy, **form → tax type**, is small (2 forms, 1 tax type). It is kept **inside**
  `dim_tax_form` (denormalized) instead of a `dim_tax_type` table. That would be the snowflake.
* The taxpayer's category and jurisdiction are columns of `dim_taxpayer`, not separate tables, for the same reason.

### Why one fact and not three (outputs / inputs / computation)?

All sections of the return have the **same grain** (one filing). Splitting them would only force fact-to-fact joins
on `ack_no`. One wide fact is simpler and faster.

### Why the 4 small reference dimensions are seeded

Their values are a fixed list. Seeding them gives every code a stable key, and the validation rules
(`mustBeOneOf`, `mustExistIn`) reject unknown codes **before** the fact load, so a fact row never misses a key.

---

## 3. ETL process, step by step (sample return `120000000601`)

### Step 1: EXTRACT (incremental)

`$match: { updatedAt: { $gt: <last watermark>, $lte: <run start> }, isTest: { $ne: true } }`.
First run: no watermark, so everything up to now is read: **21 documents** (the test document is skipped).

### Step 2: SANITIZE

`fromJson` flattens the nested objects into columns (`taxableOutputsValue`, `vatLevies`, ..., `rtnStatus`,
`filedAt`, `ackDate`, `creditBroughtForward`, ...), then:

| Rule | Sample | Before → after |
|---|---|---|
| trim + blank → NULL | 611 | `" individual "` → `"individual"` |
| category to standard value | 611 | `"individual"` → `Individual`, `"NON INDIVIDUAL"` → `Non-Individual` |
| return type to standard value | 607, 608 | `"original"` → `Original`, `"A"` → `Amended` |
| upper case codes | 611 | `fileFrom "mobl"` → `MOBL`, `tin`, `rtnStatus` |
| remove thousand separators (all amounts) | 610 | `"2,80,000"` → `280000` |

### Step 3: VALIDATE + DE-DUPLICATE

Section 6 lists all rules. The first run splits the 21 documents into **14 valid / 7 rejected**:

| ackNo | Problem | Error type |
|---|---|---|
| 603 (older copy) | same `ackNo` twice: the copy with the higher `updatedAt` is kept | DUPLICATE |
| 615 | `taxableOutputsValue = "20O000"` (letter O) | CORRUPT |
| 616 | `rtnFromDate = "2026-08-32"` | CORRUPT |
| 617 | `tin = "X12345"` | VALIDATION |
| 618 | nil return, but outputs 50,000 | VALIDATION |
| 619 | `netOutputTax 99,999 ≠ 14,000 - 6,000` | VALIDATION |
| 620 | `formId 99999` not in `dim_tax_form` | ORPHAN |

Reject gate: 6 bad (duplicates not counted) of 21 = 28.6% ≤ 30% → the job goes on.

### Step 4: TRANSFORM (sample `120000000601`, BNSJ, September 2026)

```text
dim_taxpayer      tin=P0000000620  taxpayer_id=110330002299  name=BNSJ  category=Non-Individual  jurisdiction=Q
dim_tax_period    period_key=202609  2026-09-01..2026-09-30  month=9  year=2026  quarter=3  due_date=2026-11-01
dim_date          20261014 (filed + ack date), 20261101 (due date)

fact_vat_return   ack_no=120000000601
   keys           taxpayer_key -> P0000000620,  period_key=202609,  tax_form_key -> 12027,
                  return_type -> Original,  return_status -> APRV,  filing_channel -> INET,
                  filed_date_key=20261014,  due_date_key=20261101,  ack_date_key=20261014
   flags          is_nil_return=false  is_late_return=false  is_active=true  delay_days=-18 (filed 18 days early)
   outputs        taxable_outputs_value=200000.00  vat_levies=40000.00  zero_rated / relief / exempt = 0
   inputs         local_purchases_value=300000.00  total_input_value=300000.00  input_tax_on_purchases=60000.00
   computation    total_output_tax=40000.00  total_input_tax=60000.00  deductible_input_tax=60000.00
                  net_output_tax=-20000.00  withholding=0  credit_last_month=0  net_payable=-20000.00
                  credit_brought_forward=0  credit_carried_forward=-20000.00   (a credit for October)
   totals         penalty_amount=0  interest_amount=0  net_tax=-20000.00
```

Converted on the way: text → `NUMERIC(18,2)`, `"Y"/"N"` and `"true"/"false"` → `BOOLEAN`, dates → date keys
`yyyyMMdd`.

### Step 5: LOAD (direct, no staging, one transaction)

See section 4. Order inside the transaction: `dim_taxpayer` → `dim_tax_period` → `dim_date` → `fact_vat_return` →
rejected records → watermark.

### Step 6: COMMIT + STATUS

COMMIT. Then `meta_pipeline_run.pipeline_status = SUCCESS_WITH_FAILED` (14 loaded, 7 rejected). Any error →
ROLLBACK: nothing in silver or meta changes, `last_run_time` does not move, `pipeline_status = FAILED`.

---

## 4. Loading without staging tables

The other jobs write Spark data into `etl_staging.stg_*` and merge with one SQL. This job writes **directly**:

1. Spark validates and transforms as usual (`cache()` the results).
2. The rows are streamed to the driver with `toLocalIterator()`: **one partition at a time**, never all rows.
3. **One** application connection (Spring `TransactionTemplate` + `JdbcTemplate.batchUpdate`, 1,000 rows per batch):

   ```sql
   -- dimension (UPSERT)
   INSERT INTO silver.dim_taxpayer (tin, taxpayer_id, taxpayer_name, taxpayer_category, jurisdiction_code, job_run_id, loaded_at)
   VALUES (?, ?, ?, ?, ?, ?, now())
   ON CONFLICT (tin) DO UPDATE SET taxpayer_name = EXCLUDED.taxpayer_name, ... ;

   -- fact (UPSERT): surrogate keys are looked up INSIDE the same SQL
   INSERT INTO silver.fact_vat_return (ack_no, taxpayer_key, period_key, tax_form_key, ..., net_tax, job_run_id, loaded_at)
   VALUES (?, (SELECT taxpayer_key FROM silver.dim_taxpayer WHERE tin = ?), ?,
              (SELECT tax_form_key FROM silver.dim_tax_form WHERE form_id = ?), ..., ?, ?, now())
   ON CONFLICT (ack_no) DO UPDATE SET return_status_key = EXCLUDED.return_status_key, ... ;
   ```

4. Rejected records (`meta_rejected_record` + `meta_error_master`) and the watermark go into the **same**
   transaction → COMMIT = everything, ROLLBACK = nothing.

**Why the surrogate keys are looked up in SQL:** a taxpayer inserted in this run is not committed yet, so Spark
(which reads with its own connection) cannot see its `taxpayer_key`. A sub-select on the **same** connection can.

**Volume:** VAT returns arrive once a month per registered taxpayer: tens of thousands of rows per run, with a peak
around the due date. One driver-side transaction handles that well (memory = one partition). If the volume grows to
millions per run, switch this job back to staging plus one final transaction.

---

## 5. Sanitization rules (pipeline specific, `VatReturnSources.sanitizer()`)

| Step | Columns |
|---|---|
| `fromJson` | 5 output, 5 input, 7 computation, 4 stock amounts; `rtnStatus`, `filedAt` (`$.crtDt`), `isLateRtn`, `isActive`, `creditBroughtForward`, `creditCarriedForward`, `ackDate` (`$.genAcknowledgementVatHeader.ackDate`) |
| `trimAllAndBlankToNull` | all |
| `toUpperCase` | `tin`, `fileFrom`, `rtnStatus`, `rtnType`, `taxpayerCategory`, `isLateRtn`, `isActive` |
| `toLowerCase` | `isNilReturn` |
| `mapValues("rtnType")` | `O`, `ORIGINAL` → `Original`; `A`, `AMENDED` → `Amended` |
| `mapValues("taxpayerCategory")` | `INDIVIDUAL` → `Individual`; `NON-INDIVIDUAL`, `NON INDIVIDUAL` → `Non-Individual` |
| `removeThousandSeparators` | all 26 amounts |

## 6. Validation rules (`VatReturnSources.rules(formIds)`)

All 69 messages are seeded in PART 3 of `03_postgres_tables.sql`.

| Type | Rules |
|---|---|
| CORRUPT (34) | `mustBeDecimal` on all 26 amounts; `mustBeDate` `rtnFromDate`, `rtnToDate`, `rtnDueDt`; `mustBeInteger` `month`, `year`, `delayDays`; `mustBeTimestamp` `filedAt`, `ackDate` |
| VALIDATION (33) | `required` (14): `tin`, `taxpayerName`, `taxpayerCategory`, `ackNo`, `rtnFromDate`, `rtnToDate`, `month`, `year`, `rtnType`, `formId`, `fileFrom`, `rtnStatus`, `filedAt`, `netTax` |
| | `tin` like `P0000000001`; `ackNo` 12 digits |
| | `mustBeOneOf`: category, rtnType, fileFrom (INET/OFFC/MOBL), rtnStatus (APRV/PEND/REJT), isNilReturn |
| | `mustNotBeNegative`: outputs, levies, total input, input tax, penalty, interest |
| | `rtnToDate` cannot be in the future (a month can be filed only after it ended) |
| | custom: month 1-12; `rtnToDate` ≥ `rtnFromDate`; month/year = month/year of `rtnFromDate`; a nil return has no taxable outputs; `netOutputTax = totalOutputTax - deductibleInputTax` (± 0.01) |
| ORPHAN (1) | `formId` must exist in `silver.dim_tax_form` |
| DUPLICATE (1) | same `ackNo` twice in one run: the latest `updatedAt` wins |

Reject gate: `maxRejectPercent(30)` (not applied below 10 rows).

---

## 7. Sample data

### `01_mongo_full_load.js`: 22 documents (21 read + 1 test)

| Taxpayer (tin) | Aug 2026 | Sep 2026 | Oct 2026 |
|---|---|---|---|
| BNSJ (P0000000620) | 614 | 601 (credit -20,000) | 602 (uses the credit) · ~~620~~ ORPHAN form |
| Asante Trading Ltd (P0000000731) | 613 | 603 (+ older DUPLICATE copy) | 604 |
| Kwame Boateng (P0000000842), Individual | ~~618~~ nil with outputs | 605 nil | 606 nil |
| Accra Foods Ltd (P0000000953) | ~~615~~ CORRUPT amount | 607 (OFFC, zero-rated 1,50,000) · 608 **Amended** | 612 PEND |
| Volta Logistics Ltd (P0000001064) | 609 **late** 14 days, penalty 500 + interest 1,200 | 610 (`"2,80,000"`) | |
| Ama Owusu (P0000001175), Individual | ~~616~~ CORRUPT date | 611 (MOBL, `" individual "`) | ~~619~~ wrong net |
| Unknown Traders (X12345) | | ~~617~~ bad tin | |

**Expected first run:** read 21 · valid 14 · rejected 7 (1 DUPLICATE, 2 CORRUPT, 3 VALIDATION, 1 ORPHAN)
→ `dim_taxpayer` 6 · `dim_tax_period` 3 · `fact_vat_return` 14 · **SUCCESS_WITH_FAILED**.

### `02_mongo_incremental.js`: changes after the first run

| Change | ackNo | Result in the next run |
|---|---|---|
| UPDATE status PEND → APRV | 612 | fact row updated in place |
| UPDATE taxpayer renamed "Asante Trading Company Ltd" | 604 | `dim_taxpayer` overwritten (SCD1) |
| FIX `netOutputTax` → 8,000.00 | 619 | was rejected, now loaded (its rejected rows stay as history) |
| INSERT Tema Steel Works Ltd, Oct (new taxpayer) | 621 | new dim row + fact row |
| INSERT Volta Logistics, Oct | 622 | new fact row |
| INSERT Kwame, November, filed on 21 Nov | 623 | VALIDATION: `rtnToDate` (30 Nov) cannot be in the future |
| DELETE the bad-tin return | 617 | not visible (deletes are not pushed) |

**Expected incremental run:** read 6 · valid 5 · rejected 1 → `dim_taxpayer` 7 · `fact_vat_return` 17 ·
**SUCCESS_WITH_FAILED**.

### Run order

```powershell
& "C:\Program Files\PostgreSQL\18\bin\psql.exe" -h localhost -p 5433 -U postgres -d silver -f sample-data\vat-return-pipeline\03_postgres_tables.sql
mongosh "mongodb://localhost:27017" sample-data\vat-return-pipeline\01_mongo_full_load.js
curl.exe -X POST "http://localhost:8080/api/admin/dwh/pipelines/vat-return-pipeline/run"
mongosh "mongodb://localhost:27017" sample-data\vat-return-pipeline\02_mongo_incremental.js
curl.exe -X POST "http://localhost:8080/api/admin/dwh/pipelines/vat-return-pipeline/run"
```

---

## 8. Decisions and open points

| # | Point | Decision / question |
|---|---|---|
| 1 | Watermark field | Source must add `updatedAt` (BSON Date, indexed). **To confirm with the source team.** |
| 2 | Time zone | Ghana = UTC, but the Spark session uses `Asia/Kolkata`. Date keys must be computed in the source time zone, not with the session time zone. Note: in the real document `ackDate = 2026-02-03T18:30:00Z`, which is midnight **IST** on 4 Feb, so the source seems to store dates as IST midnight. **To confirm.** |
| 3 | Duplicate fields | Top level and `rtnRegisterVatDTO` both carry period / type / due date. Decision: period, type and due date from the **top level**; status, filed time, credits and ack date from **`rtnRegisterVatDTO`**. |
| 4 | Taxpayer history | `dim_taxpayer` is SCD1 (latest values). If reports need the jurisdiction **at filing time**, change it to SCD2 (more complex without staging) or store `jurisdiction_code` on the fact as well. |
| 5 | Amended returns | Kept as separate fact rows (own `ack_no`). "Latest return per tin + period" is a query or view, not a separate table. |
| 6 | Deletes | Soft delete: `isDeleted: true` + new `updatedAt` -> `fact_vat_return.is_deleted = TRUE` (in the same transaction). A removed document (hard delete) is not propagated. |
| 7 | Volume | Direct load in one transaction is fine for tens of thousands of returns per run; for millions use staging plus one final transaction. |

---

## 9. ETL in one picture

```text
MongoDB vat_return_coll ──(updatedAt > meta_pipeline.last_run_time)──> Spark
   │  sanitize: fromJson (flatten nested objects), trim, upper, map codes, remove separators
   │  validate: CORRUPT / VALIDATION / ORPHAN (dim_tax_form) / DUPLICATE (ackNo)
   │  transform: dim_taxpayer rows, dim_tax_period rows, date keys, fact rows (26 measures)
   ▼
ONE PostgreSQL transaction (application connection, no staging):
   dim_taxpayer UPSERT → dim_tax_period UPSERT → dim_date UPSERT → fact_vat_return UPSERT (keys by sub-select)
   → meta_rejected_record → meta_pipeline.last_run_time
   COMMIT (all) / ROLLBACK (nothing) → meta_pipeline_run.pipeline_status
   successful run → lineage: meta_asset, meta_lineage (automatic), meta_column_lineage (11 declared columns)
```

## 10. Implementation

| Where | What |
|---|---|
| `pipeline/vatreturn/VatReturnSources.java` | `SourceDefinition` (business key `ackNo`, watermark `updatedAt`, `excludeTestRecords()`, `maxRejectPercent(30)`), `sanitizer()` (section 5), `rules(formIds)` (section 6) |
| `pipeline/vatreturn/VatReturnSchemaMapper.java` | `toTyped`, `toDimTaxpayer` (latest return per tin wins), `toDimTaxPeriod`, `toFactVatReturn` |
| `pipeline/vatreturn/VatReturnPipeline.java` | `directLoad() = true`; `run()`: extract + validate -> dim_taxpayer -> dim_tax_period -> dim_date -> fact_vat_return |
| `connector/target/DirectPostgresWriter.java` | **new**: `toLocalIterator` + `JdbcTemplate.batchUpdate` with `INSERT ... ON CONFLICT`, surrogate keys by sub-select (`KeyLookup`), only inside an active transaction |
| `core/EtlPipeline.directLoad()` | **new**, default `false`. `true` = `DwhPipelineServiceImpl` runs `run()` + commit phase in ONE `TransactionTemplate` |
| `repository/RejectedRecordRepository.saveDirect()` | **new**: rejected rows without staging; new messages -> `meta_error_master` first; same transaction |

### What was tested (temporary databases, `reWriteBatchedInserts=true`)

| Run | Result |
|---|---|
| first run (01) | read 21, valid 14, rejected 7 -> dim_taxpayer 6, dim_tax_period 3, fact 14; SUCCESS_WITH_FAILED; no `etl_staging` table created; master unchanged (69: every message was seeded) |
| incremental (02) | read 6, valid 5, rejected 1 -> fact 17, taxpayer 7, Asante renamed, 612 = APRV, 619 loaded |
| **forced failure** (fact insert fails after the dimensions were written) | FAILED. **Rolled back:** the new taxpayer, the rejected row, the new master message and `last_run_time`. The run row stays FAILED with the error message |
| same window again after the fix | read the same 2 documents, both handled; the new message got `error_id` 70 (no gap) |
| partly fixed record (3 errors -> 1) | its one row now has only the remaining error in `error_ids` / `error_messages`, `attempt_count + 1` |
| `sales-pipeline` (staging path, regression) | unchanged: read 94, valid 80, rejected 16 |
