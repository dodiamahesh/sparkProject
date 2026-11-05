# 🧭 Airflow DAG Builder – સંપૂર્ણ User Guide (ગુજરાતીમાં)

> **આ guide કોના માટે છે?** – જે `index.html` (Airflow DAG Builder) વાપરીને **પોતાની જરૂરિયાત મુજબની DAG `.py` file** બનાવવા માંગે છે.
> Python કે Airflow માં નવા હો તો પણ ચાલશે – દરેક option નો **અર્થ, હેતુ અને ક્યારે વાપરવો** તે સમજાવ્યું છે.
>
> Technical શબ્દો (task, retry, cron, XCom…) English માં જ રાખ્યા છે, કારણ કે tool માં અને Airflow UI માં એ જ શબ્દો દેખાય છે.

---

## 📑 અનુક્રમણિકા

**ભાગ A – શરૂઆત**
1. [Tool શું છે અને શું કરે છે](#1-tool-શું-છે-અને-શું-કરે-છે)
2. [Tool કેવી રીતે ખોલવું](#2-tool-કેવી-રીતે-ખોલવું)
3. [Screen ની ઓળખ (layout)](#3-screen-ની-ઓળખ)
4. [5 મિનિટમાં પહેલી DAG – Quick start](#4-5-મિનિટમાં-પહેલી-dag--quick-start)
5. [ઓછામાં ઓછું કેટલું configure કરવું? (Simple vs Complex)](#5-ઓછામાં-ઓછું-કેટલું-configure-કરવું)

**ભાગ B – Top bar (ઉપરની પટ્ટી)**
6. [Template, Layout, Theme, Import/Export, Download](#6-top-bar-ના-બધા-buttons)

**ભાગ C – Configuration ના બધા sections**
7. [⚙ Output settings](#7--output-settings)
8. [🏷 DAG basics](#8--dag-basics)
9. [🕒 Schedule & runs](#9--schedule--runs)
10. [🛡 Default args (all tasks)](#10--default-args-all-tasks)
11. [🔔 Alerts & callbacks](#11--alerts--callbacks)
12. [🎛 Params (trigger form)](#12--params-trigger-form)
13. [🧩 Tasks & dependencies](#13--tasks--dependencies)
14. [દરેક Operator ના options (16 operators)](#14-દરેક-operator-ના-options)

**ભાગ D – Preview**
15. [Code / Graph / Config JSON tabs અને Validation](#15-preview-pane)

**ભાગ E – ઉદાહરણો (Step by step)**
16. [ઉદાહરણ 1 – Simple: રોજ એક shell command](#16-ઉદાહરણ-1--simple-રોજ-એક-shell-command)
17. [ઉદાહરણ 2 – Medium: Python ETL + Params](#17-ઉદાહરણ-2--medium-python-etl--params)
18. [ઉદાહરણ 3 – Complex: Spark pipeline (sensor, setup/teardown, branch, mapping, alerts, asset)](#18-ઉદાહરણ-3--complex-spark-pipeline)

**ભાગ F – Download પછી**
19. [`.py` file ને Airflow માં કેવી રીતે deploy કરવી](#19-py-file-ને-airflow-માં-deploy-કરવી)
20. [Validation messages નો અર્થ અને ઉકેલ](#20-validation-messages-નો-અર્થ-અને-ઉકેલ)
21. [FAQ / Troubleshooting](#21-faq--troubleshooting)
22. [Keyboard shortcuts અને Tips](#22-keyboard-shortcuts-અને-tips)

---

# ભાગ A – શરૂઆત

## 1. Tool શું છે અને શું કરે છે

**Airflow DAG Builder** એ એક **form આધારિત tool** છે:

```
તમે form ભરો  ──►  Tool તરત Python code બનાવે  ──►  Download .py  ──►  Airflow ના dags/ folder માં મૂકો
 (radio, dropdown,        (જમણી બાજુ live preview)
  checkbox, text)
```

- ✅ Python code લખવાની જરૂર નથી (Python task નો "body" જ લખવો પડે, એ પણ template સાથે મળે છે).
- ✅ **Offline** ચાલે – internet, server, database કંઈ જોઈએ નહીં.
- ✅ Airflow **3.x**, **2.x** અથવા **બંને** માટે code બનાવે.
- ✅ દરેક field પર **ⓘ** – mouse લઈ જાઓ એટલે તેનો અર્થ દેખાય.
- ✅ ભૂલો (errors/warnings) તરત બતાવે.
- ✅ તમારું કામ browser માં **આપમેળે save** થાય છે.

---

## 2. Tool કેવી રીતે ખોલવું

1. Folder ખોલો: `LearningDoc/Airflow/dag-builder/`
2. **`index.html` પર double-click** કરો → Chrome / Edge / Firefox માં ખુલશે.
3. બસ! કોઈ install નહીં.

> 💡 **Tip:** Browser માં bookmark કરી દો. કામ એ જ browser માં save રહે છે, એટલે હંમેશા **એ જ browser** વાપરો.

---

## 3. Screen ની ઓળખ

```
┌──────────────────────────────────────────────────────────────────────────────────────────┐
│ ✦ Airflow DAG Builder  [Template ▾][Load] [◧Side|⬓Bottom|☰Config|</>Code] [⇄]            │
│                        [Theme ▾][🎨] (Inline help) [⤒][⤓][↺] [⬇ Download .py]            │  ← (A) Top bar
├───────────────────────────────────────────┬──────────────────────────────────────────────┤
│  ⚙ Output settings                    ▸   │ [Code|Graph|Config JSON] ✓Valid  A− A+ ↩ ⧉   │
│  🏷 DAG basics                        ▸   │ ───────────────────────────────────────────  │
│  🕒 Schedule & runs                   ▸   │  1  """                                      │
│  🛡 Default args (all tasks)          ▸   │  2  my_pipeline - ...                        │
│  🔔 Alerts & callbacks                ▸   │  3  ...                                      │
│  🎛 Params (trigger form)             ▸   │        (live Python code)                    │
│  🧩 Tasks & dependencies              ▾   │                                              │
│     [Operator ▾][＋Add task] (Link)       │                                              │
│     ┌ task card ─────────────────────┐    │                                              │
│     └────────────────────────────────┘    │ 5 tasks · 113 lines · Airflow 3.x · file.py  │
└───────────────────────────────────────────┴──────────────────────────────────────────────┘
        (B) Configuration pane          ↕ divider          (C) Preview pane
```

| ભાગ | કામ |
|-----|-----|
| **(A) Top bar** | Template load, layout, theme, import/export, download |
| **(B) Configuration pane** | 7 sections – અહીં બધું configure કરો |
| **(C) Preview pane** | બનતો Python code, graph, validation |
| **Divider** (વચ્ચેની પટ્ટી) | Mouse થી ખેંચીને બંને pane ની પહોળાઈ બદલો |

**દરેક section નું header** click કરો → ખુલે / બંધ થાય. Header ની જમણી બાજુ **summary** દેખાય (દા.ત. `0 6 * * * · Asia/Kolkata`), એટલે બંધ section માં પણ ખબર પડે કે શું set છે.

---

## 4. 5 મિનિટમાં પહેલી DAG – Quick start

| Step | શું કરવું |
|------|-----------|
| 1 | Top bar માં **Template → "Simple ETL (extract → transform → load)"** પસંદ કરો → **Load** |
| 2 | **🏷 DAG basics → DAG id** માં તમારું નામ લખો, દા.ત. `sales_daily_etl` |
| 3 | **🕒 Schedule & runs → Cron expression** માં સમય, દા.ત. `0 7 * * *` (રોજ સવારે 7) |
| 4 | **🧩 Tasks** માં `extract` card ખોલી Python code તમારી જરૂર મુજબ બદલો |
| 5 | જમણી બાજુ **✓ Valid** (લીલું) છે તે તપાસો |
| 6 | **⬇ Download .py** → `sales_daily_etl.py` download થશે |
| 7 | File ને Airflow ના `dags/` folder માં મૂકો (section 19) |

---

## 5. ઓછામાં ઓછું કેટલું configure કરવું?

### 🟢 Simple DAG – ફક્ત આટલું જરૂરી (Minimum)

| # | Section → Field | કેમ જરૂરી |
|---|-----------------|-----------|
| 1 | DAG basics → **DAG id** | વગર DAG બને જ નહીં, unique હોવું જોઈએ |
| 2 | Schedule → **Schedule type** + value (cron / preset / manual) | ક્યારે ચાલે |
| 3 | Schedule → **Start date** | ક્યારથી ગણતરી શરૂ |
| 4 | Tasks → **ઓછામાં ઓછો 1 task** (task id + operator + command/code) | DAG નું કામ |
| 5 | (2+ tasks હોય તો) → **Depends on** | ક્રમ (કયું પહેલાં, કયું પછી) |

બાકીનું બધું **default** પર છોડી શકો – defaults સલામત રાખ્યા છે (`catchup=Off`, `max_active_runs=1`, `retries=1`, `timeout=60 min`, Airflow 3.x).

### 🟡 Medium DAG – આ પણ કરો (Recommended)

| Field | કેમ |
|-------|-----|
| DAG basics → **Owner, Description, Tags** | UI માં શોધવામાં અને ટીમને સમજવામાં |
| Schedule → **Timezone** | ભારતીય સમય મુજબ ચલાવવા (`Asia/Kolkata`) |
| Default args → **Retries, Retry delay, Task timeout** | ક્ષણિક error માં આપમેળે ફરી પ્રયત્ન; અટકેલા task kill |
| Alerts → **Task failure, DAG run failure** | Fail થાય તો ખબર પડે |
| Params → **env** જેવા input | એક જ DAG dev/qa/prod માટે |

### 🔴 Complex / Production DAG – આ બધું ધ્યાનથી

| Feature | ક્યાં | ક્યારે વાપરવું |
|---------|-------|----------------|
| Sensor | Tasks → `@task.sensor` / FileSensor / ExternalTaskSensor | Input file / બીજા DAG ની રાહ જોવી હોય |
| Branch / ShortCircuit | Tasks → Branch / ShortCircuit | શરત મુજબ રસ્તો બદલવો / skip કરવું |
| Join trigger rule | Task → Trigger rule = `none_failed_min_one_success` | Branch પછી ભેગા થતા task માં **ફરજિયાત** |
| TaskGroup | Task → Task group | 10+ tasks હોય ત્યારે UI સ્વચ્છ રાખવા |
| Dynamic mapping | Task → Map this task | Files/tables ની સંખ્યા રોજ બદલાતી હોય |
| Setup / Teardown | Task → Role | Cluster/temp resource બનાવવું-કાઢવું |
| Assets (Datasets) | Schedule → Dataset / Task → Outlets | એક DAG નો data તૈયાર થાય એટલે બીજો DAG ચાલે |
| Pool / Priority | Task → Advanced | DB/API પર ભાર મર્યાદિત રાખવો |
| Slack/Teams alerts | Alerts → Slack/Teams webhook | Production monitoring |
| Catchup | Schedule → Catchup | જૂની તારીખોનો data ભરવો (backfill) હોય ત્યારે જ |

---

# ભાગ B – Top bar

## 6. Top bar ના બધા buttons

| Button / Control | શું કરે | ક્યારે વાપરવું |
|------------------|---------|----------------|
| **Template ▾ + Load** | તૈયાર pipeline load કરે (8 templates). વર્તમાન config **બદલાઈ જાય** – પણ નીચે આવતા message માં **Undo** button 6 સેકન્ડ મળે. Airflow version setting જળવાઈ રહે. | નવી શરૂઆત કરતી વખતે, સૌથી નજીકનો template લો અને બદલો |
| **◧ Side** | Config ડાબે, Preview જમણે | મોટી screen પર (default) |
| **⬓ Bottom** | Config ઉપર, Preview નીચે | પહોળો code જોવો હોય ત્યારે |
| **☰ Config** | ફક્ત configuration | ધ્યાનથી form ભરવું હોય ત્યારે |
| **</> Code** | ફક્ત preview | Code / graph વાંચવો હોય ત્યારે |
| **⇄** | Config અને Preview ની જગ્યા અદલ-બદલ | Preview ડાબે જોઈએ ત્યારે |
| **Divider ખેંચો** | Pane ની પહોળાઈ/ઊંચાઈ બદલો (keyboard: divider પર Tab, પછી ← →) | |
| **Theme ▾** | 7 themes: 🌙 Dark, ☀️ Light, 🌌 Midnight, 🌲 Forest, 🌅 Sunset, 🧛 Dracula, ◐ High contrast | આંખને ગમે તે; projector પર Light/High contrast |
| **🎨 (color box)** | Accent (મુખ્ય) રંગ પોતાનો પસંદ કરો. Theme બદલો એટલે reset. | |
| **Inline help** | ON કરો → દરેક field ની નીચે તેની સમજૂતી **હંમેશા** દેખાય (hover ની જરૂર નહીં) | શરૂઆતમાં **ON રાખો** – શીખવામાં ખૂબ મદદ |
| **⤒ Import** | પહેલાં export કરેલી `.json` config પાછી load કરે | ટીમ પાસેથી config મળી હોય, બીજા computer પર |
| **⤓ Export** | વર્તમાન config `<dag_id>.config.json` તરીકે save | Backup, ટીમ સાથે share, પછી ફેરફાર કરવા |
| **↺ Reset** | બધું ખાલી (Blank template) – Undo મળે | નવેસરથી શરૂ કરવું હોય |
| **⬇ Download .py** | Python file `<dag_id>.py` download (`Ctrl+S` પણ) | Config પૂરું થાય ત્યારે |

> 📝 નાની screen (1560px થી ઓછી) પર Import/Export/Reset માં ફક્ત icon દેખાય – mouse લઈ જાઓ તો નામ દેખાશે.

### 8 Templates – કયો ક્યારે?

| Template | અંદર શું છે | ક્યારે પસંદ કરવો |
|----------|-------------|-------------------|
| **Blank (start → end)** | ફક્ત 2 Empty tasks | શૂન્યથી બનાવવું હોય |
| **Simple ETL** | start → extract (Python) → transform (Python) → load (Bash) → end, `env` param | Database/API → transform → load જેવા સામાન્ય ETL |
| **Spark ETL + setup/teardown** | FileSensor → create_workspace (setup) → SparkSubmit → delete_workspace (teardown) → notify | Spark job (તમારા Spark project માટે સૌથી ઉપયોગી) |
| **Branching by Param** | env મુજબ load_dev / load_prod, join | dev/qa/prod મુજબ અલગ રસ્તા |
| **Dynamic task mapping** | list_files → process_file (mapped) → summarize → clean_tables (mapped Bash) | Files/tables ની સંખ્યા બદલાતી હોય |
| **Dataset / Asset producer** | Outlet વાળો task | આ DAG નો data બીજા DAG ને trigger કરે |
| **Dataset / Asset triggered consumer** | Asset પર schedule | બીજા DAG નો data આવે ત્યારે ચાલવું |
| **All operators showcase** | 15 tasks, groups, sensors, branch, Spark/Docker/K8s, SQL, mapping, trigger, email | બધું એક સાથે જોવા/શીખવા |

---

# ભાગ C – Configuration ના બધા sections

> દરેક table માં: **Field** = screen પર દેખાતું નામ · **Control** = કેવા પ્રકારનું input · **Default** · **હેતુ / ક્યારે બદલવું** · **Generated code** = Python માં શું બને.

## 7. ⚙ Output settings

**હેતુ:** Python file **કેવી રીતે** બને તે નક્કી કરવું.

| Field | Control | Default | હેતુ / ક્યારે બદલવું | Generated code |
|-------|---------|---------|----------------------|----------------|
| **Airflow version** | Radio: `Airflow 3.x` / `Airflow 2.x` / `Both (auto-detect)` | 3.x | તમારા server પર જે Airflow version હોય તે. ખબર ન હોય તો **Both** – બંને પર ચાલે એવા `try/except` imports બને. (Version જાણવા: `airflow version` command) | 3.x → `from airflow.sdk import ...` · 2.x → `from airflow.decorators import ...` · Both → `try: ... except ImportError: ...` |
| **Explanatory comments** | Toggle | On | Code માં docstring અને સમજૂતી comments ઉમેરે. શીખવા/review માટે On; ટૂંકો code જોઈએ તો Off | `# ── extract · @task ...`, `doc_md=__doc__` |
| **Add dag.test() block** | Toggle | On | File ને સીધી `python my_dag.py` થી ચલાવી local test કરી શકાય | `if __name__ == "__main__": dag.test()` |

---

## 8. 🏷 DAG basics

**હેતુ:** DAG ની ઓળખ – નામ, માલિક, વર્ણન.

| Field | Control | ઉદાહરણ | હેતુ / નિયમ | Generated code |
|-------|---------|---------|--------------|----------------|
| **DAG id** ⭐ (ફરજિયાત) | Text | `sales_daily_etl` | આખા Airflow માં **unique** નામ. ફક્ત અક્ષર, આંકડા, `_` `-` `.` – space નહીં. Download file નું નામ પણ આ જ બને. | `dag_id="sales_daily_etl"` |
| **Owner** | Text | `data_engineering` | જવાબદાર ટીમ/વ્યક્તિ – UI માં દેખાય, filter થાય | `default_args["owner"]` |
| **Description** | Text | `Daily sales ETL from MySQL` | DAG list માં દેખાતું એક-લાઇન વર્ણન | `description="..."` |
| **Tags** | Text (comma) | `etl, daily, finance` | UI માં tags થી DAG શોધવા/filter કરવા | `tags=["etl", "daily", "finance"]` |
| **Documentation (Markdown)** | Textarea | `## આ DAG શું કરે છે…` | Airflow UI ના **Docs** બટનમાં દેખાતું વિગતવાર લખાણ. ખાલી રાખો તો generated docstring વપરાય (comments On હોય તો). | `doc_md="..."` |

---

## 9. 🕒 Schedule & runs

**હેતુ:** DAG **ક્યારે** ચાલે અને **કેટલા run એક સાથે** ચાલે.

### 9.1 Schedule type (Radio) – 5 વિકલ્પ

| વિકલ્પ | અર્થ | ક્યારે વાપરવું | પછી કયા fields દેખાય |
|--------|------|----------------|----------------------|
| **Cron** | ચોક્કસ સમય (દા.ત. રોજ 6:00) | મોટા ભાગના batch jobs | Cron expression, Cron examples |
| **Preset** | ટૂંકા નામ `@daily`, `@hourly`… | સાદા schedule | Preset dropdown |
| **Interval** | દર N મિનિટ/કલાક/દિવસ | "દર 2 કલાકે" જેવું | Every + Unit |
| **Dataset / Asset** | **બીજો DAG data update કરે ત્યારે** | Producer-consumer pipelines | Asset URIs, Run when |
| **Manual only** | ક્યારેય આપમેળે નહીં (`schedule=None`) | ફક્ત UI/API/બીજા DAG થી trigger | – |

### 9.2 Cron (Cron type પસંદ કરો ત્યારે)

| Field | હેતુ |
|-------|------|
| **Cron expression** | 5 ભાગ: `મિનિટ કલાક તારીખ મહિનો વાર` |
| **Cron examples** | Dropdown માંથી પસંદ કરો → expression આપમેળે ભરાય |
| **🕒 લીલું લખાણ** | તમારો cron સાદી ભાષામાં (દા.ત. "At 06:00, every day") – લખતાં-લખતાં બદલાય |

**Cron cheat sheet:**

| Cron | અર્થ |
|------|------|
| `0 6 * * *` | રોજ સવારે 06:00 |
| `30 2 * * *` | રોજ રાત્રે 02:30 |
| `*/15 * * * *` | દર 15 મિનિટે |
| `0 * * * *` | દર કલાકે (મિનિટ 0 પર) |
| `0 6 * * 1-5` | સોમ થી શુક્ર 06:00 |
| `0 0 * * 0` | દર રવિવારે રાત્રે 12 |
| `0 0 1 * *` | દર મહિનાની 1 તારીખે |
| `0 9,18 * * *` | રોજ 09:00 અને 18:00 |

> વાર (day-of-week): `0`=રવિ, `1`=સોમ … `6`=શનિ. સમય **Timezone** field મુજબ ગણાય.

### 9.3 Preset (Preset type પસંદ કરો ત્યારે)

| Preset | અર્થ |
|--------|------|
| `@once` | ફક્ત એક જ વાર |
| `@hourly` | દર કલાકે |
| `@daily` | રોજ રાત્રે 00:00 |
| `@weekly` | રવિવાર 00:00 |
| `@monthly` | મહિનાની 1 તારીખ 00:00 |
| `@quarterly` | દર 3 મહિને |
| `@yearly` | 1 જાન્યુઆરી |
| `@continuous` | એક run પૂરો થાય એટલે તરત બીજો (⚠️ Max active runs = 1 જરૂરી) |

### 9.4 Interval (Interval type પસંદ કરો ત્યારે)

| Field | હેતુ | Generated |
|-------|------|-----------|
| **Every** | આંકડો (દા.ત. 2) | `schedule=timedelta(hours=2)` |
| **Unit** | minutes / hours / days / weeks | |

> Cron અને Interval નો ફરક: Cron = ઘડિયાળના ચોક્કસ સમયે; Interval = start date થી દર N સમયે.

### 9.5 Dataset / Asset (Dataset type પસંદ કરો ત્યારે)

| Field | હેતુ |
|-------|------|
| **Asset / Dataset URIs** | એક line માં એક URI, દા.ત. `s3://my-bucket/sales/daily.parquet`. જે બીજા DAG ના task ના **Outlets** માં **આ જ URI** હોય તે task success થાય ત્યારે આ DAG ચાલે. |
| **Run when** | **ALL** = બધા URIs update થયા પછી · **ANY** = કોઈ એક update થાય ત્યારે (Airflow 2.9+) |

### 9.6 બાકીના Schedule fields (હંમેશા દેખાય)

| Field | Control | Default | હેતુ / ક્યારે બદલવું |
|-------|---------|---------|----------------------|
| **Start date** ⭐ | Date | 2026-01-01 | Scheduler ક્યારથી run ગણે. **ભૂતકાળની ફિક્સ તારીખ** રાખો. |
| **End date** | Date | ખાલી | આ પછી નવા run નહીં. ખાલી = કાયમ. |
| **Timezone** | Dropdown | Asia/Kolkata | Cron/start date કયા timezone માં. ભારત માટે `Asia/Kolkata`. |
| **Catchup (backfill)** | Toggle | Off | **On** = DAG ચાલુ કરો ત્યારે start date થી આજ સુધીના **દરેક ચૂકેલા run** બને (300 દિવસ = 300 runs!). જૂનો data ભરવો હોય ત્યારે જ On. |
| **Max active runs** | Number | 1 | એક સાથે આ DAG ના કેટલા run. 1 = runs એકબીજા પર ન ચડે (સલામત). |
| **Max active tasks** | Number | 16 | આ DAG ના એક સાથે કેટલા tasks ચાલે. |
| **Run timeout (minutes)** | Number | 120 | આખો run આટલી મિનિટથી વધુ ચાલે તો fail. 0 = મર્યાદા નહીં. |
| **Paused when created** | Toggle | On | નવો DAG **paused** આવે – UI માં જાતે ON કરવો પડે (સલામત). Off = deploy થતાં જ ચાલુ. |
| **Render templates as Python objects** | Toggle | Off | On = Jinja નું પરિણામ string ને બદલે list/dict/int બને. Advanced ઉપયોગ માટે. |

---

## 10. 🛡 Default args (all tasks)

**હેતુ:** **દરેક task** પર લાગુ થતા settings – દરેક task માં ફરી લખવાની જરૂર નહીં. કોઈ task માટે અલગ જોઈએ તો task ના **Advanced** માં override કરો.

| Field | Default | હેતુ / ક્યારે બદલવું | Generated |
|-------|---------|----------------------|-----------|
| **Retries** | 1 | Fail થાય તો કેટલી વાર ફરી પ્રયત્ન. API/DB વાળા DAG માં 2–3. | `"retries": 1` |
| **Retry delay (seconds)** | 300 | Retry પહેલાં રાહ (300 = 5 મિનિટ). | `"retry_delay": timedelta(minutes=5)` |
| **Exponential backoff** | Off | On = દરેક retry માં રાહ બમણી (5 → 10 → 20 મિનિટ). Flaky API માટે ઉત્તમ. | `"retry_exponential_backoff": True` |
| **Max retry delay (seconds)** | 1800 | *(Backoff On હોય ત્યારે જ દેખાય)* backoff ની ઉપલી મર્યાદા. | `"max_retry_delay": ...` |
| **Task timeout (minutes)** | 60 | Task આનાથી વધુ ચાલે તો kill + fail. અટકેલો task worker ન રોકે. 0 = મર્યાદા નહીં (ભલામણ નથી). Spark job લાંબો હોય તો વધારો. | `"execution_timeout": ...` |
| **depends_on_past** | Off | On = **ગયા run માં આ task success થયો હોય તો જ** આ run માં ચાલે. ક્રમમાં ચાલવા જરૂરી incremental loads માટે. | `"depends_on_past": True` |
| **Alert e-mails** | ખાલી | Airflow ના built-in email ના મેળવનાર (comma થી). SMTP setup જરૂરી. | `"email": [...]` |
| **E-mail on failure** | Off | Task fail પર email | `"email_on_failure": True` |
| **E-mail on retry** | Off | Retry પર email | `"email_on_retry": True` |

---

## 11. 🔔 Alerts & callbacks

**હેતુ:** કંઈક fail/success થાય ત્યારે **notification** મોકલવું.

| Field | Control | હેતુ |
|-------|---------|------|
| **Notification channel** | Radio: `Off` / `Log only` / `Slack webhook` / `MS Teams webhook` | **Off** = callbacks નહીં · **Log only** = ફક્ત task log માં લખો · **Slack/Teams** = message webhook પર POST |
| **Variable holding webhook URL** | Text | *(Slack/Teams હોય ત્યારે)* Airflow **Variable** નું નામ જેમાં webhook URL છે (દા.ત. `slack_webhook_url`). **URL સીધો code માં ન લખાય** – સલામતી માટે Variable માં. |
| **Notify on** | Chips (એક કરતાં વધુ પસંદ) | `Task failure`, `Task retry`, `Task success`, `DAG run failure`, `DAG run success` |

**Generated code:** `_notify()` function + `on_task_failure()` વગેરે callbacks; task events `default_args` માં અને DAG events `with DAG(...)` માં જોડાય.

**Slack/Teams setup (એક વાર):**
1. Slack/Teams માં **Incoming Webhook** બનાવો → URL copy કરો.
2. Airflow UI → **Admin → Variables → +** → Key: `slack_webhook_url`, Value: URL → Save.
3. Builder માં channel = Slack, variable name = `slack_webhook_url`.

> 💡 **ભલામણ:** ઓછામાં ઓછું `Task failure` અને `DAG run failure` પસંદ રાખો. `Task success` બધા task માટે બહુ messages મોકલે – સામાન્ય રીતે ન લો.

---

## 12. 🎛 Params (trigger form)

**હેતુ:** DAG ના **input**. UI માં **"Trigger DAG w/ config"** દબાવો ત્યારે એક form ખુલે; Airflow value ને validate કરે (ખોટી હોય તો run શરૂ ન થાય). Scheduled run માં **Default** value વપરાય.

**ઉમેરવા:** `＋ Add param` · **કાઢવા:** param box ના ખૂણાનું `✕`

| Field | હેતુ |
|-------|------|
| **Name** | Param નું નામ (identifier – space નહીં). Code માં વાપરવા: Jinja `{{ params.NAME }}`, Python `context["params"]["NAME"]` |
| **Type** | નીચેનું table |
| **Default** | Default value (boolean માટે true/false dropdown) |
| **Allowed values** | *(enum હોય ત્યારે)* comma થી, દા.ત. `dev, qa, prod` → UI માં dropdown |
| **Minimum / Maximum** | *(integer/number હોય ત્યારે)* મર્યાદા |
| **Description** | Form માં field નીચે દેખાતી help |

| Type | UI form માં | ઉદાહરણ ઉપયોગ |
|------|-------------|---------------|
| `string` | Text box | file path, table name |
| `integer` | આંકડો (min/max) | batch size, days back |
| `number` | દશાંશ આંકડો | threshold 0.95 |
| `boolean` | Checkbox | `full_refresh` હા/ના |
| `enum (drop-down)` | Dropdown | `env` = dev/qa/prod |
| `array (list)` | List | tables ની યાદી |

**Generated code:** `params={"env": Param("dev", type="string", enum=["dev","qa","prod"], description="...")}`

---

## 13. 🧩 Tasks & dependencies

**હેતુ:** DAG નું ખરું કામ – tasks ઉમેરવા, તેમનો પ્રકાર (operator) પસંદ કરવો અને ક્રમ (dependency) નક્કી કરવો.

### 13.1 Task ઉમેરવા (Add bar)

| Control | હેતુ |
|---------|------|
| **Operator dropdown** | ઉમેરવાનો operator (4 groups: Control flow, Compute, Sensors, Cross-DAG/notify) |
| **＋ Add task** | પસંદ કરેલો operator ઉમેરો |
| **Link to previous** (toggle) | On = નવો task આપમેળે **છેલ્લા task પછી** આવે (dependency બને). ક્રમસર pipeline માટે On રાખો; parallel tasks માટે Off. |
| **Quick add:** `＋Python` `＋Bash` `＋Spark` `＋Branch` `＋Sensor` `＋Empty` | એક click માં સામાન્ય operators |
| **Expand all / Collapse all** | બધા task cards ખોલો / બંધ કરો |

### 13.2 Task card – header

```
▸ [Python] extract   ▲ setup  [ ] mapped   ← start            [↑][↓][⧉][✕]
```
| ભાગ | અર્થ |
|-----|------|
| `▸` | Click → card ખોલો/બંધ કરો |
| રંગીન badge | Operator નો પ્રકાર. ડાબી કિનારીનો રંગ: **વાદળી**=compute, **પીળો/નારંગી**=sensor, **ગુલાબી**=branch, **ભૂખરો**=control, **જાંબલી**=cross-DAG/notify |
| `extract` | Full task id (group હોય તો `group.task`) |
| `▲ setup` / `▼ teardown` | Role badge |
| `[ ] mapped` | Dynamic mapping ચાલુ છે |
| `← start` | આ task કોના પછી ચાલે છે (upstream) / `root` = કોઈ upstream નહીં |
| `↑` `↓` | List માં ઉપર/નીચે ખસેડો (code માં ક્રમ બદલાય, dependency નહીં) |
| `⧉` | Task ની copy (`_copy` નામ સાથે) |
| `✕` | Delete (Undo મળે). બીજા tasks ની dependency માંથી પણ આપમેળે નીકળી જાય. |

### 13.3 Task card – સામાન્ય fields (દરેક task માં)

| Field | Control | હેતુ |
|-------|---------|------|
| **Task id** ⭐ | Text | DAG માં unique નામ (group ની અંદર). UI, logs અને `xcom_pull(task_ids=...)` માં વપરાય. Box માંથી બહાર click કરો ત્યારે બધે update થાય. |
| **Operator** | Dropdown | Task નો પ્રકાર. ⚠️ બદલો તો operator-specific settings **reset** થાય. |
| **Task group** | Text | Task ને **TaskGroup** (UI box) માં મૂકો. Nested માટે ટપકું: `extract.database`. Full id બને `group.task_id`. ખાલી = group નહીં. |
| **Role** | Radio: `normal` / `setup` / `teardown` | **setup** = resource બનાવે (cluster, temp folder) · **teardown** = સાફ કરે અને **વચ્ચેનું fail થાય તો પણ ચાલે** (finally જેવું). Teardown ના "Depends on" માં તેનો setup task **પસંદ કરવો જરૂરી**. |
| **Info box** (રંગીન) | – | Operator શું કરે છે + જરૂરી `pip install` package |

### 13.4 Dynamic task mapping (ફક્ત Python `@task` અને Bash માં)

**હેતુ:** Run વખતે **દરેક value માટે task ની એક copy** – દા.ત. આજે 3 files = 3 copies, કાલે 50 = 50 copies.

| Field | હેતુ |
|-------|------|
| **Map this task (fan-out)** | Toggle – mapping ચાલુ |
| **Values come from** | **Static list** = તમે અહીં લખો · **Upstream output** = કોઈ Python task જે **list return** કરે તે (run વખતે નક્કી) |
| **Argument name** | *(Python માટે)* function નો argument જેમાં એક value આવે, દા.ત. `file_name`. Python body માં આ નામ વાપરો. |
| **Values** | *(Static)* comma થી અથવા એક line માં એક. **Bash માટે: એક line = એક command.** આંકડા અને True/False આપમેળે type બને. |
| **Upstream task (returns a list)** | *(Upstream output)* કયા Python task નું output. Dependency આપમેળે બને. |
| **Max parallel copies** | એક સાથે વધુમાં વધુ કેટલી copies ચાલે (`max_active_tis_per_dag`) – DB/API પર ભાર ન પડે |

### 13.5 Depends on (upstream) + Trigger rule

| Field | હેતુ |
|-------|------|
| **Depends on** (chips) | આ task **કોના પછી** ચાલે. એક કરતાં વધુ પસંદ કરી શકાય (બધા પૂરા થાય પછી). પસંદ કરેલી chip પર ✓ દેખાય. ⚠️ Loop (A→B→A) ન બનાવો – error આવશે. |
| **Trigger rule** | Upstream ની સ્થિતિ મુજબ task **ક્યારે** ચાલે |

| Trigger rule | ક્યારે ચાલે | ક્યાં વાપરવું |
|--------------|-------------|---------------|
| `all_success` *(default)* | બધા upstream success | સામાન્ય |
| `none_failed_min_one_success` | કોઈ fail નહીં + ઓછામાં ઓછો 1 success | ⭐ **Branch પછીનો join** |
| `all_done` | બધા પૂરા – success, fail કે skip | Cleanup |
| `one_failed` | કોઈ એક fail થાય એટલે | Alert / rollback task |
| `none_failed` | કોઈ fail નહીં (skip ચાલે) | Soft-fail sensor પછી |
| `one_success` | કોઈ એક success થાય એટલે | Race (પહેલું પૂરું તે) |
| `all_failed` | બધા fail | Fallback |
| `one_done` | કોઈ એક પૂરું (success/fail) | |
| `all_skipped` | બધા skip | |
| `none_skipped` | કોઈ skip નહીં | |
| `always` | હંમેશા | Debug |

### 13.6 Advanced (▸ Advanced click કરો)

| Field | હેતુ | ખાલી રાખો તો |
|-------|------|-------------|
| **Retries (override)** | ફક્ત આ task ના retries | Default args વપરાય |
| **Retry delay sec (override)** | ફક્ત આ task નો retry delay | Default |
| **Timeout min (override)** | ફક્ત આ task નો timeout (દા.ત. Spark માટે 180) | Default |
| **Pool** | Shared resource limit (Airflow UI → Admin → Pools માં બનાવો, દા.ત. `mysql_pool` 3 slots) | `default_pool` |
| **Priority weight** | Slots ઓછા હોય ત્યારે મોટો આંકડો પહેલાં | 1 |
| **Outlets – Asset URIs** | Task success થાય ત્યારે આ Asset "updated" ગણાય → તે Asset પર schedule થયેલા DAG ચાલે | – |
| **Task documentation** | Task ના detail page પર Markdown | – |

---

## 14. દરેક Operator ના options

> ⭐ = ફરજિયાત/મહત્વનું. `{{ ds }}` જેવું **Jinja** (run વખતે value બને) જ્યાં લખ્યું છે ત્યાં વાપરી શકાય.
>
> **Key=value fields** (env, conf, headers…): **એક line માં એક**, દા.ત.
> ```
> RUN_ENV={{ params.env }}
> JAVA_HOME=/usr/lib/jvm/java-17
> ```

### 🔹 Control flow

#### EmptyOperator
કંઈ કરતું નથી. **Start / end marker** અથવા branches ભેગા કરવા **join** તરીકે. કોઈ option નથી.

#### LatestOnlyOperator
Backfill/catchup ના **જૂના run માં downstream skip** કરે; ફક્ત સૌથી નવા run માં ચલાવે. Report/email ને latest_only પછી મૂકો જેથી 30 દિવસના backfill માં 30 emails ન જાય. કોઈ option નથી.

#### @task.branch – Branch
**હેતુ:** run વખતે **કયો રસ્તો** લેવો તે નક્કી કરે; બાકીના direct downstream **skip**.

| Field | હેતુ |
|-------|------|
| **Decide by** | `Param value (no code)` – code વગર, param ની value મુજબ · `Custom Python code` – પોતાનો logic |
| **Param name** | કયો param જોવો (Params section માં હોવો જોઈએ), દા.ત. `env` |
| **Default target** | કોઈ rule match ન થાય ત્યારે કયો task (ખાલી = પહેલો downstream) |
| **Rules** | એક line માં એક: `value => task_id`. ઘણા: `qa => load_dev, load_prod` |
| **Function body** *(Custom)* | task_id (string) અથવા list `return` કરો. Group ની અંદરના task માટે full id `group.task` |

નીચે **"Downstream tasks you can return:"** માં માન્ય task ids દેખાય છે.

⚠️ **પહેલાં branch બનાવો, પછી target tasks ના "Depends on" માં branch પસંદ કરો**, અને join task નો trigger rule `none_failed_min_one_success` રાખો.

#### @task.short_circuit
**હેતુ:** શરત **False** હોય તો downstream skip ("data નથી તો આગળ ન જાઓ").

| Field | હેતુ |
|-------|------|
| **Condition** | `return True` = આગળ જાઓ, `return False` = skip |
| **Skip ALL downstream** | On = નીચેના **બધા** tasks skip (trigger rule અવગણીને) · Off = ફક્ત સીધા downstream skip; cleanup (`all_done`) જેવા tasks ચાલે |

### 🔹 Compute

#### @task – TaskFlow Python ⭐ (સૌથી વધુ વપરાતો)

| Field | હેતુ |
|-------|------|
| **Function body (Python)** | ફક્ત function નો **અંદરનો ભાગ** લખો – `def ...(**context):` line આપમેળે બને. |
| **multiple_outputs** | Dict return કરો તો દરેક key અલગ XCom બને |

**Body માં શું વાપરી શકાય:**
```python
context["ds"]                      # run ની તારીખ "2026-11-05"
context["params"]["env"]           # param value
context["ti"].xcom_pull(task_ids="extract")   # બીજા task નું return
log.info("message %s", value)      # log લખવો (log તૈયાર છે)
return value                       # આ value XCom માં જાય (નાનો data જ)
```
Body માં **Tab** દબાવો તો 4 spaces આવે. Indentation (spaces) Python માં મહત્વનું છે.

#### PythonOperator (classic)
જૂની/classic રીત. Module-level function `<task>_callable(**context)` બને.

| Field | હેતુ |
|-------|------|
| **Callable body** | Function નો અંદરનો ભાગ |
| **op_kwargs** | `KEY=value` – function ને arguments (Jinja ચાલે), body માં `context["KEY"]` |

#### BashOperator ⭐

| Field | હેતુ |
|-------|------|
| **Bash command** ⭐ | Shell command (multi-line ચાલે). દા.ત. `python /opt/jobs/load.py --date {{ ds }}` |
| **Environment variables** | `KEY=value` – command માટે env vars |
| **Keep worker env (append_env)** | On = worker ના PATH, JAVA_HOME વગેરે પણ રાખો (સામાન્ય રીતે On) |
| **Working directory (cwd)** | Command કયા folder માં ચાલે |

#### SparkSubmitOperator ⭐ (Spark project માટે)
📦 `pip install apache-airflow-providers-apache-spark` · Airflow Connection `spark_default` જોઈએ (host: `local[*]` / `yarn` / `spark://host:7077`).

| Field | હેતુ | ઉદાહરણ |
|-------|------|---------|
| **Application** ⭐ | PySpark `.py` અથવા `.jar` નો path | `/opt/spark-apps/etl_job.py` |
| **Spark connection id** | Spark master વાળી connection | `spark_default` |
| **Spark app name** | Spark UI/YARN માં નામ | `etl_{{ ds_nodash }}` |
| **Main class (JAR only)** | Scala/Java job ની main class | `com.company.Main` |
| **Application arguments** | Job ને arguments (space થી; quotes માં space વાળી value) | `--run-date {{ ds }} --env prod` |
| **Spark conf** | `key=value` | `spark.sql.shuffle.partitions=200` |
| **Executor memory / Driver memory** | Memory | `4g` / `2g` |
| **Num executors / Executor cores** | Resources | `4` / `2` |
| **Packages** | Maven coordinates (comma) | Kafka/Delta connectors |
| **Verbose** | spark-submit debug log | |

#### SQLExecuteQueryOperator
📦 `apache-airflow-providers-common-sql` + તમારા DB નો provider (postgres/mysql…)

| Field | હેતુ |
|-------|------|
| **Connection id** | DB ની Airflow connection, દા.ત. `postgres_default` |
| **SQL** | Query/queries (`;` થી અલગ). Jinja: `WHERE dt = '{{ ds }}'` |
| **Autocommit** | દરેક statement પછી commit |

#### HttpOperator (REST API)
📦 `apache-airflow-providers-http`

| Field | હેતુ |
|-------|------|
| **HTTP connection id** | Base URL વાળી connection |
| **Endpoint** | Path, દા.ત. `api/v1/refresh` |
| **Method** | GET / POST / PUT / DELETE |
| **Body / data** | POST/PUT નો body (JSON string) |
| **Headers** | `Key=value` |
| **Log response** | Response log માં લખો |

#### DockerOperator
📦 `apache-airflow-providers-docker`

| Field | હેતુ |
|-------|------|
| **Image** | Docker image:tag |
| **Command** | Container માં ચાલતો command |
| **Docker URL** | Docker daemon address (Linux: `unix://var/run/docker.sock`) |
| **Auto remove** | `never` / `success` / `force` – container ક્યારે delete |
| **Environment** | `KEY=value` |
| **Mount tmp dir** | Host temp folder mount (Docker Desktop પર Off રાખો) |

#### KubernetesPodOperator
📦 `apache-airflow-providers-cncf-kubernetes`

| Field | હેતુ |
|-------|------|
| **Pod name / Namespace / Image** | Pod ની ઓળખ અને image |
| **Entrypoint (cmds) / Arguments** | Space થી અલગ (list બને) |
| **Stream logs** | Pod નો output Airflow log માં |
| **On finish** | `delete_pod` / `delete_succeeded_pod` / `keep_pod` |

### 🔹 Sensors (રાહ જોનારા)

**બધા sensors ના સામાન્ય options:**

| Field | Default | હેતુ |
|-------|---------|------|
| **Poke interval (sec)** | 60 | કેટલી વારે તપાસ |
| **Timeout (sec)** | 3600 | આટલી રાહ પછી હાર |
| **Mode** | reschedule | `poke` = worker slot આખો સમય રોકે (ટૂંકી રાહ) · `reschedule` = તપાસ વચ્ચે slot છોડે (લાંબી રાહ માટે ⭐) |
| **Soft fail** | Off | On = timeout પર **fail ને બદલે skip** (DAG લાલ ન થાય) |

| Sensor | ખાસ fields | ક્યારે |
|--------|-----------|--------|
| **@task.sensor (Python)** | **Condition** – `True` return = તૈયાર | કોઈ પણ custom શરત (API status, DB row count…) |
| **FileSensor** | **File path** (Jinja/glob `*.csv` ચાલે), **Filesystem connection** (`fs_default`) | Input file / `_SUCCESS` marker ની રાહ |
| **ExternalTaskSensor** | **External DAG id**, **External task id** (ખાલી = આખો DAG), **Time offset (minutes)** | બીજો DAG પૂરો થાય તેની રાહ (બંનેની logical date મેળ ખાવી જોઈએ; અલગ સમયે ચાલે તો offset) |

### 🔹 Cross-DAG / Notify

#### TriggerDagRunOperator

| Field | હેતુ |
|-------|------|
| **DAG id to trigger** | કયો DAG ચાલુ કરવો |
| **conf (JSON)** | બીજા DAG ને મોકલાતો data – **માન્ય JSON** હોવો જોઈએ (`{"key": "value"}`, double quotes). બીજા DAG માં `context["dag_run"].conf` થી વાંચો. |
| **Wait for completion** | On = triggered DAG પૂરો થાય ત્યાં સુધી રાહ |
| **Check interval (sec)** | *(Wait On હોય ત્યારે)* કેટલી વારે state તપાસવી |

#### EmailOperator
📦 Airflow 3 માં `apache-airflow-providers-smtp` · SMTP setup જરૂરી

| Field | હેતુ |
|-------|------|
| **To** | Comma થી emails |
| **Subject** | વિષય (Jinja ચાલે) |
| **HTML body** | HTML મેસેજ |

---

# ભાગ D – Preview

## 15. Preview pane

### Tabs
| Tab | શું દેખાય | ઉપયોગ |
|-----|-----------|--------|
| **Code** | Generated Python – રંગીન syntax, line numbers | ખરેખર શું બનશે તે વાંચો |
| **Graph** | Tasks અને તીરો નો diagram (Airflow ના Graph view જેવો) | ક્રમ સાચો છે કે નહીં તપાસો. **Task પર click** → config માં એ task ખૂલે. |
| **Config JSON** | Export થતો JSON | Debug / share |

**Graph legend:** box ની કિનારીનો રંગ = operator પ્રકાર · **લીલી dashed line** = setup → teardown · **જાંબલી dotted line** = mapping (output પરથી) · લાલ ⚠ = cycle (loop) છે.

### Toolbar
| Button | કામ |
|--------|-----|
| **Status pill** (`✓ Valid` / `⚠ 2 warnings` / `✕ 1 error`) | Click → નીચે messages ની યાદી ખોલો/બંધ કરો |
| **A− / A+** | Code font નાનો/મોટો |
| **↩ Wrap** | લાંબી lines વાળો |
| **⧉ Copy** | આખો code (અથવા JSON tab માં JSON) clipboard માં |

**Status bar (નીચે):** tasks ની સંખ્યા · code lines · Airflow version · file નું નામ · "autosaved".

### Message ના રંગ
| પ્રકાર | અર્થ | Download? |
|--------|------|-----------|
| 🔴 **ERROR** | આ સાથે DAG **ચાલશે નહીં** / Airflow માં import error | Download થાય, પણ message ચેતવે – **પહેલાં સુધારો** |
| 🟠 **WARN** | ચાલશે, પણ કદાચ તમે ધાર્યું તેમ નહીં (દા.ત. join skip થશે) | તપાસીને નિર્ણય કરો |
| 🔵 **INFO** | માહિતી (દા.ત. કયું pip package જોઈએ) | વાંધો નહીં |

---

# ભાગ E – ઉદાહરણો (Step by step)

## 16. ઉદાહરણ 1 – Simple: રોજ એક shell command

**જરૂરિયાત:** રોજ સવારે 8 વાગ્યે `/opt/scripts/cleanup.sh` ચલાવવી.

| Step | ક્યાં | શું કરવું |
|------|-------|-----------|
| 1 | Top bar | Template = **Blank** → Load |
| 2 | DAG basics | DAG id = `daily_cleanup`, Owner = તમારું નામ |
| 3 | Schedule | Type = **Cron**, expression = `0 8 * * *` (લીલું: "At 08:00, every day") |
| 4 | Tasks | `start` અને `end` બંને છે. `＋ Bash` click કરો (Link to previous **Off** કરો) |
| 5 | નવો Bash card | Task id = `run_cleanup`, Bash command = `bash /opt/scripts/cleanup.sh` |
| 6 | `run_cleanup` → Depends on | `start` ✓ |
| 7 | `end` → Depends on | `start` ✗ કાઢો, `run_cleanup` ✓ |
| 8 | Preview → Graph | `start → run_cleanup → end` દેખાય |
| 9 | ⬇ Download .py | `daily_cleanup.py` |

> 💡 `.sh` થી પૂરો થતો command – tool આપમેળે છેલ્લે space ઉમેરે છે (Airflow ની જાણીતી તકલીફ ટાળવા).

---

## 17. ઉદાહરણ 2 – Medium: Python ETL + Params

**જરૂરિયાત:** રોજ 6:30 વાગ્યે API માંથી data લાવો → transform → env (dev/prod) મુજબ load. Fail થાય તો Slack પર message.

| Step | ક્યાં | શું કરવું |
|------|-------|-----------|
| 1 | Top bar | Template = **Simple ETL** → Load |
| 2 | Output | Airflow version = તમારું version |
| 3 | DAG basics | DAG id = `api_sales_etl`, Description, Tags = `etl, api` |
| 4 | Schedule | Cron = `30 6 * * *`, Timezone = `Asia/Kolkata` |
| 5 | Default args | Retries = `3`, Retry delay = `120`, Exponential backoff = On, Task timeout = `30` |
| 6 | Alerts | Channel = **Slack webhook**, Variable = `slack_webhook_url`, Notify on = Task failure ✓, DAG run failure ✓ |
| 7 | Params | `env` પહેલેથી છે (enum: dev, qa, prod). `＋ Add param` → Name = `days_back`, Type = integer, Default = `1`, Min = `1`, Max = `30` |
| 8 | Tasks → `extract` | Body માં તમારો API code: `import requests` … `return rows` (નાનો data; મોટો હોય તો file માં લખી path return કરો) |
| 9 | Tasks → `transform` | `rows = context["ti"].xcom_pull(task_ids="extract")` … |
| 10 | Tasks → `load` | Bash command: `python /opt/jobs/load.py --env {{ params.env }} --date {{ ds }}` |
| 11 | Preview | ✓ Valid → Download |
| 12 | Airflow UI | Variable `slack_webhook_url` બનાવો (section 11) |

---

## 18. ઉદાહરણ 3 – Complex: Spark pipeline

**જરૂરિયાત:**
1. રોજ રાત્રે 2:30 → input folder માં `_SUCCESS` file આવે તેની રાહ (2 કલાક સુધી).
2. Temp workspace બનાવો (setup).
3. Region ની list (north, south, east, west) – **દરેક region માટે અલગ Spark job** (mapping).
4. Workspace ગમે તે થાય delete (teardown).
5. `mode` param: `full` હોય તો full refresh task, `incremental` હોય તો incremental task (branch).
6. પછી join → data ready Asset update → reporting DAG આપમેળે ચાલે.
7. Fail થાય તો Teams alert.

| Step | ક્યાં | શું કરવું |
|------|-------|-----------|
| 1 | Top bar | Template = **Spark ETL + setup / teardown** → Load (sensor, setup, spark, teardown તૈયાર મળે) |
| 2 | DAG basics | DAG id = `regional_sales_spark`, Tags = `spark, sales` |
| 3 | Schedule | Cron = `30 2 * * *`, Max active runs = `1`, Run timeout = `300` |
| 4 | Default args | Retries = `2`, Task timeout = `120` |
| 5 | Alerts | Channel = **MS Teams webhook**, Variable = `teams_webhook_url`, Task failure ✓, DAG run failure ✓ |
| 6 | Params | Name = `mode`, Type = **enum**, Allowed = `full, incremental`, Default = `incremental` |
| 7 | `wait_for_input` (FileSensor) | File path = `/data/input/{{ ds }}/_SUCCESS`, Timeout = `7200`, Mode = reschedule |
| 8 | `create_workspace` | Role = **setup** (પહેલેથી) |
| 9 | `＋ Python` → `list_regions` | Body (દરેક region માટે એક spark-submit command ની list બનાવે – regions ની યાદી run વખતે DB/config માંથી પણ લાવી શકાય): <br>`regions = ["north", "south", "east", "west"]`<br>`ds = context["ds"]`<br>`return [f"spark-submit /opt/spark-apps/etl_job.py --region {r} --date {ds}" for r in regions]`<br>Depends on = `create_workspace` |
| 10 | `spark_job` | Operator = **Bash** માં બદલો *(આ tool માં mapping Python અને Bash માટે છે)*, Map this task = On, Values come from = **Upstream output**, Upstream task = `list_regions`, Max parallel copies = `2`, Depends on = `wait_for_input`, `create_workspace`, `list_regions` → run વખતે 4 copies (`spark_job[0..3]`) બનશે |
| 10-B | *(વિકલ્પ)* | Regions ફિક્સ હોય તો step 9 ન કરો; Values come from = **Static list** અને Values માં એક line માં એક command લખો |
| 11 | `delete_workspace` | Role = **teardown**, Depends on = `spark_job` ✓ અને `create_workspace` ✓ (**setup પસંદ કરવો જરૂરી**) |
| 12 | `＋ Branch` → `choose_mode` | Decide by = Param value, Param name = `mode`, Depends on = `spark_job` |
| 13 | `＋ Bash` → `full_refresh` | Command: `python /opt/jobs/refresh.py --full`, Depends on = `choose_mode` |
| 14 | `＋ Bash` → `incremental_load` | Command: `python /opt/jobs/refresh.py --incremental`, Depends on = `choose_mode` |
| 15 | `choose_mode` પાછું ખોલો | Rules: <br>`full => full_refresh`<br>`incremental => incremental_load`<br>Default target = `incremental_load` |
| 16 | `＋ Empty` → `join` | Depends on = `full_refresh`, `incremental_load` · **Trigger rule = `none_failed_min_one_success`** ⭐ |
| 17 | `join` → Advanced | Outlets = `s3://lake/sales/regional_ready` |
| 18 | Preview → Graph | ક્રમ તપાસો; લીલી dashed line (setup→teardown) દેખાય |
| 19 | Preview | ✓ Valid (INFO માં pip packages) → Download |
| 20 | Reporting DAG | Builder માં નવું: Template = **Dataset triggered consumer** → Asset URI = `s3://lake/sales/regional_ready` → Download |

> ✅ આમ એક જ DAG માં **sensor + setup/teardown + mapping + branch + join + asset + alerts** – production level pipeline.

---

# ભાગ F – Download પછી

## 19. `.py` file ને Airflow માં deploy કરવી

| Step | શું કરવું | Command / જગ્યા |
|------|-----------|------------------|
| 1 | જરૂરી packages install કરો (file ના ઉપરના docstring માં `Install:` અને Preview ના INFO messages માં યાદી) | `pip install apache-airflow-providers-apache-spark` |
| 2 | File ને **dags folder** માં copy કરો | સામાન્ય રીતે `~/airflow/dags/` અથવા Docker માં `./dags/` |
| 3 | File માં syntax error નથી તે તપાસો | `python my_dag.py` (error ન આવે તો સારું) |
| 4 | Airflow ને DAG દેખાય છે? | `airflow dags list \| grep my_dag` અથવા UI માં 30–60 સેકન્ડ રાહ |
| 5 | Import error તપાસો | `airflow dags list-import-errors` અથવા UI ના ઉપર લાલ banner |
| 6 | જરૂરી **Connections / Variables** બનાવો | UI → Admin → Connections / Variables (spark_default, postgres_default, slack_webhook_url…) |
| 7 | Local test (optional) | `python my_dag.py` (dag.test() block On હોય તો) અથવા `airflow dags test my_dag 2026-11-05` |
| 8 | UI માં DAG **Unpause** (toggle ON) | Paused when created = On હતું એટલે |
| 9 | પહેલી વાર manually trigger | ▶ → Trigger DAG (w/ config) |
| 10 | Graph અને Logs જુઓ | |

> 🪟 **Windows નોંધ:** Airflow Windows પર સીધું ચાલતું નથી – **Docker** (`docker compose`) અથવા **WSL** માં ચલાવો, અને file ને તેના `dags/` folder માં મૂકો.

---

## 20. Validation messages નો અર્થ અને ઉકેલ

| Message (ટૂંકમાં) | અર્થ | ઉકેલ |
|-------------------|------|------|
| DAG id is required… | DAG id ખાલી/ખોટા અક્ષર | ફક્ત `a-z A-Z 0-9 _ . -` |
| Cron expression must have 5 fields | Cron ખોટો | 5 ભાગ space થી: `0 6 * * *` |
| Dataset / Asset schedule needs at least one URI | Dataset type પણ URI નથી | URI લખો |
| @continuous requires Max active runs = 1 | | Max active runs = 1 |
| End date is before the start date | | તારીખ સુધારો |
| Catchup is ON… *(info)* | જૂના બધા run બનશે | જરૂર ન હોય તો Off |
| Duplicate task id | બે task નું એક નામ (એક જ group માં) | નામ બદલો |
| Task id … is invalid | ખોટા અક્ષર/space | સુધારો |
| Circular dependency | Loop (A→B→A) | Depends on માંથી loop તોડો |
| Branch … has no downstream tasks | Branch પછી કોઈ task નથી | Target tasks ના Depends on માં branch પસંદ કરો |
| Branch … reads Param … not defined | Param name ખોટું / બનાવ્યું નથી | Params માં બનાવો |
| Branch target … is not a task id / not directly downstream | Rules માં ખોટું task નામ | સાચું નામ અથવા dependency ઉમેરો |
| "X" joins branches of "Y" but uses all_success | Join skip થઈ જશે | Trigger rule = `none_failed_min_one_success` |
| Teardown … has no setup task as upstream | Teardown નો setup જોડ્યો નથી | Depends on માં setup task પસંદ કરો |
| choose the upstream task whose output is mapped | Mapping source પસંદ નથી | Upstream task dropdown પસંદ કરો |
| add at least one value to map over | Static mapping ખાલી | Values લખો |
| conf is not valid JSON | Trigger conf ખોટો JSON | Double quotes `"`, છેલ્લે comma નહીં |
| Param … default is not one of the allowed values | Enum default યાદીમાં નથી | Default સુધારો |
| "E-mail on failure" is on but no e-mail | | Email લખો અથવા Off |
| Set the Variable name that holds the webhook URL | | Variable નામ લખો |
| … needs: pip install … *(info)* | Provider package જોઈએ | Server પર install કરો |

---

## 21. FAQ / Troubleshooting

**Q. મારું કામ ક્યાં save થાય છે? બીજા દિવસે મળશે?**
A. Browser ના localStorage માં **આપમેળે** save. એ જ browser માં ખોલો તો મળશે. Browser data/cache clear કરો તો જશે – એટલે મહત્વના config **⤓ Export** કરી રાખો.

**Q. ભૂલથી task delete / template load થઈ ગયું!**
A. નીચે આવતા message માં **Undo** (6 સેકન્ડ) દબાવો.

**Q. Airflow UI માં DAG દેખાતો નથી.**
A. (1) file `dags/` folder માં છે? (2) `airflow dags list-import-errors` – મોટે ભાગે **provider package install નથી** (દા.ત. Spark). (3) DAG id બીજા DAG જેવું તો નથી? (4) 1 મિનિટ રાહ જુઓ.

**Q. Branch પછીનો task હંમેશા skip થાય છે.**
A. Join task નો Trigger rule `none_failed_min_one_success` કરો.

**Q. DAG ચાલુ કર્યો અને સેંકડો run બની ગયા!**
A. Catchup On હતું. Off કરો, DAG ફરી deploy કરો; વધારાના runs UI માંથી delete કરો.

**Q. Schedule ખોટા સમયે ચાલે છે.**
A. Timezone તપાસો. UI માં સમય UTC માં દેખાઈ શકે – Airflow UI ના ઉપર જમણે timezone બદલો.

**Q. Airflow 2 કે 3 – ખબર નથી.**
A. Server પર `airflow version`. ખબર ન હોય તો Output settings → **Both (auto-detect)**.

**Q. XCom માં મોટો data મોકલી શકું?**
A. ના. DataFrame/મોટી file S3/HDFS/disk પર લખો અને XCom માં ફક્ત **path** return કરો.

**Q. Spark operator માં mapping કેમ નથી?**
A. આ tool માં mapping ફક્ત Python `@task` અને Bash માટે છે. ઘણા Spark jobs માટે Bash + `spark-submit` mapping વાપરો (ઉદાહરણ 3), અથવા Python task માંથી list return કરીને Bash ને map કરો.

**Q. Password / API key ક્યાં લખવી?**
A. **ક્યારેય code/builder માં નહીં.** Airflow **Connections** (DB/API) અથવા **Variables** માં રાખો, અને code માં ફક્ત તેનું નામ વાપરો.

**Q. Generated code હાથથી બદલી શકું?**
A. હા, `.py` સામાન્ય Python છે. પણ પછી builder માં ફેરફાર કરી ફરી download કરશો તો હાથથી કરેલા ફેરફાર જશે – એટલે શક્ય હોય ત્યાં builder માં જ ફેરફાર કરો અને config **Export** કરી રાખો.

---

## 22. Keyboard shortcuts અને Tips

| Shortcut / Tip | કામ |
|----------------|-----|
| `Ctrl + S` | .py download |
| Code box માં `Tab` | 4 spaces (indentation) |
| Divider પર Tab પછી `← → ↑ ↓` | Pane નું માપ બદલો |
| Graph માં task પર click | Config માં તે task ખોલીને ત્યાં લઈ જાય |
| Section header click | ખોલો/બંધ – નકામા sections બંધ રાખો |
| **Inline help ON** | શરૂઆતમાં ON રાખો – દરેક field નો અર્થ સામે જ |
| Template થી શરૂ કરો | શૂન્યથી બનાવવા કરતાં નજીકનો template લઈ બદલવો ઝડપી અને ઓછી ભૂલવાળું |
| Download પહેલાં Graph tab | ક્રમ (dependency) સાચો છે તે આંખે જોઈ લો |
| દરેક DAG માટે Export | `.config.json` ને `.py` ની બાજુમાં git માં રાખો – પછી ફેરફાર સરળ |
| Task ids અર્થપૂર્ણ રાખો | `task_1` ને બદલે `extract_orders` – logs/alerts માં તરત સમજાય |
| Retries + Timeout હંમેશા | Production DAG માં ક્યારેય 0 timeout ન રાખો |

---

> 🎯 **સારાંશ:** Template પસંદ કરો → DAG id + Schedule + Start date → Tasks અને Depends on → (જરૂર મુજબ) Params, Retries, Alerts, Branch, Sensor, Mapping → Preview માં ✓ Valid અને Graph તપાસો → Download → `dags/` folder → Unpause → Trigger. બસ! 🚀
