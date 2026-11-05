# 📘 `airflow_all_scenarios_dag.py` – દરેક લાઇનની સંપૂર્ણ સમજૂતી (ગુજરાતીમાં)

> **આ document કોના માટે છે?** – જે Airflow શીખી રહ્યા છે અને આ file ની **દરેક લાઇન શું કરે છે અને શા માટે લખી છે** તે સમજવા માંગે છે.
>
> **કેવી રીતે વાંચવું?** – ડાબી બાજુ file ખોલો, જમણી બાજુ આ document. દરેક section ની શરૂઆતમાં **Line number** આપેલા છે (જેમ કે `Lines 242–268`). Technical શબ્દો (task, DAG, XCom, retry…) જાણી જોઈને English માં જ રાખ્યા છે, કારણ કે office / documentation / interview માં એ જ શબ્દો વપરાય છે.

---

## 📑 અનુક્રમણિકા (Index)

0. [પહેલાં સમજવા જેવા 5 મૂળભૂત concept](#0-પહેલાં-સમજવા-જેવા-5-મૂળભૂત-concept)
1. [Lines 1–54 · Module docstring](#1-lines-154--module-docstring)
2. [Lines 56–64 · Python ના સામાન્ય imports](#2-lines-5664--python-ના-સામાન્ય-imports)
3. [Lines 66–121 · Airflow 2 અને 3 બંનેમાં ચાલે એવા imports](#3-lines-66121--airflow-2-અને-3-બંનેમાં-ચાલે-એવા-imports)
4. [Lines 123–137 · Logger અને Constants](#4-lines-123137--logger-અને-constants)
5. [Lines 143–155 · `get_variable()` helper](#5-lines-143155--get_variable-helper-s08)
6. [Lines 161–188 · Callbacks](#6-lines-161188--callbacks-s04)
7. [Lines 194–207 · Custom Operator](#7-lines-194207--custom-operator-s25)
8. [Lines 213–220 · Classic Python function](#8-lines-213220--classic-python-function-s09)
9. [Lines 226–237 · `default_args`](#9-lines-226237--default_args-s02)
10. [Lines 242–268 · MAIN DAG ની વ્યાખ્યા](#10-lines-242268--main-dag-ની-વ્યાખ્યા-s01--s03)
11. [Line 273 · `start` task](#11-line-273--start-task-s05)
12. [Lines 278–294 · BashOperator + Jinja](#12-lines-278294--bashoperator--jinja-s06)
13. [Lines 299–318 · TaskFlow `read_config`](#13-lines-299318--taskflow-read_config-s07-s08)
14. [Lines 323–327 · Classic PythonOperator](#14-lines-323327--classic-pythonoperator-s09)
15. [Lines 332–353 · TaskGroup + Nested TaskGroup](#15-lines-332353--taskgroup--nested-taskgroup-s10)
16. [Lines 358–398 · Dynamic Task Mapping](#16-lines-358398--dynamic-task-mapping-s11-s12)
17. [Lines 403–420 · `@task_group` decorator](#17-lines-403420--task_group-decorator-s13)
18. [Lines 425–451 · Branching + Labels + Join](#18-lines-425451--branching--labels--join-s14-s15)
19. [Lines 456–483 · ShortCircuit → Dataset → Trigger child](#19-lines-456483--shortcircuit--dataset--trigger-child-s16-s17-s18)
20. [Lines 488–534 · Setup / Teardown + Spark](#20-lines-488534--setup--teardown--spark-s19-s20)
21. [Lines 539–591 · Sensor, Retry, Fail/Skip, Alert](#21-lines-539591--sensor-retry-failskip-alert-s21s23)
22. [Lines 596–601 · LatestOnlyOperator](#22-lines-596601--latestonlyoperator-s24)
23. [Lines 606–614 · Custom operator નો ઉપયોગ + Mapped Bash](#23-lines-606614--custom-operator-નો-ઉપયોગ--mapped-bash-s25-s11)
24. [Lines 619–636 · Cleanup, End અને બધી dependencies](#24-lines-619636--cleanup-end-અને-બધી-dependencies-s15-s27)
25. [Lines 642–668 · CHILD DAG](#25-lines-642668--child-dag-s18)
26. [Lines 674–695 · CONSUMER DAG](#26-lines-674695--consumer-dag-s17)
27. [Lines 702–703 · `dag.test()`](#27-lines-702703--dagtest-s28)
28. [આખો flow એક નજરમાં (Graph)](#28-આખો-flow-એક-નજરમાં)
29. [અલગ-અલગ રીતે trigger કરો તો શું થશે?](#29-અલગ-અલગ-રીતે-trigger-કરો-તો-શું-થશે)
30. [Trigger rules – cheat sheet](#30-trigger-rules--cheat-sheet)
31. [સામાન્ય ભૂલો (Common mistakes)](#31-સામાન્ય-ભૂલો-common-mistakes)
32. [શબ્દકોશ (Glossary)](#32-શબ્દકોશ-glossary)

---

## 0. પહેલાં સમજવા જેવા 5 મૂળભૂત concept

આ 5 વાત સમજાઈ જશે તો આખી file સરળ લાગશે.

### (1) DAG એટલે શું?
**DAG = Directed Acyclic Graph** – એટલે કે tasks નો એવો નકશો જેમાં
- **Directed** → દરેક તીર (arrow) એક જ દિશામાં જાય છે (A પછી B),
- **Acyclic** → ક્યારેય ગોળ-ગોળ loop ન બને (A → B → A ન ચાલે).

DAG પોતે કોઈ કામ કરતો નથી; એ ફક્ત કહે છે કે **કયું કામ, ક્યારે, કયા ક્રમમાં** થશે.

### (2) Task, Operator અને Task Instance
| શબ્દ | અર્થ | ઉદાહરણ |
|------|------|---------|
| **Operator** | કામનો *પ્રકાર* (template) | `BashOperator` = shell command ચલાવવાનો પ્રકાર |
| **Task** | DAG માં મૂકેલો operator | `print_run_info = BashOperator(...)` |
| **Task Instance (TI)** | કોઈ ચોક્કસ તારીખના run માં એ task નું એક execution | 5-Jan ના run નો `print_run_info` |

### (3) Parse time vs Run time ⚠️ (સૌથી મહત્વનું)
Airflow નો **scheduler / DAG processor આ file ને દર ~30 સેકન્ડે ફરી-ફરી વાંચે (parse કરે) છે.**
- File ના **top-level** (કોઈ function ની બહાર) લખેલો code → **દરેક parse વખતે** ચાલે છે.
- Task ના function ની **અંદર** લખેલો code → **ફક્ત task run થાય ત્યારે** ચાલે છે.

👉 એટલે જ DB call, API call, `Variable.get()` જેવી ભારે વસ્તુઓ **હંમેશા task ની અંદર** લખવી. આ file માં આ નિયમ બધે પાળ્યો છે.

### (4) XCom
**XCom = Cross-Communication** – એક task થી બીજા task ને **નાનો data** (id, count, path, નાની list) મોકલવાની રીત. Airflow તેને metadata DB માં સાચવે છે. **મોટો data (DataFrame, GB ની file) XCom માં ન મૂકવો** – એ S3/HDFS માં મૂકો અને XCom માં ફક્ત path મોકલો.

### (5) Context
Task run થાય ત્યારે Airflow તેને એક **dictionary** આપે છે જેને `context` કહેવાય. તેમાં run ની બધી માહિતી હોય છે:

| Key | અર્થ |
|-----|------|
| `ds` | logical date `YYYY-MM-DD` (દા.ત. `2026-11-05`) |
| `ds_nodash` | `20261105` |
| `logical_date` | run ની તારીખ (datetime object) |
| `run_id` | આ run નું unique નામ |
| `params` | DAG params ની values |
| `ti` / `task_instance` | ચાલુ task instance (XCom push/pull માટે) |
| `dag_run` | આખા DAG run નો object (`dag_run.conf` વગેરે) |

---

## 1. Lines 1–54 · Module docstring

```python
"""
# Airflow – All Scenarios in ONE DAG file (learning reference)
...
"""
```

| Line(s) | શું છે | શા માટે |
|---------|--------|---------|
| 1 અને 54 | `"""` … `"""` | Python નો **docstring** – file ની શરૂઆતમાં લખેલું લાંબું string. Python તેને `__doc__` variable માં રાખે છે. |
| 2 | Title | File નો હેતુ – "બધા scenario એક જ file માં". |
| 4 | Airflow 2.7+ અને 3.x | આ file બંને version પર ચાલે એવી રીતે લખી છે (આગળ section 3 માં જોઈશું કેવી રીતે). |
| 6–12 | 3 DAG નું table | એક જ `.py` file માં **એક કરતાં વધુ DAG** બનાવી શકાય – અહીં main, child અને consumer એમ 3 DAG છે. |
| 14–45 | Scenario list `S01`…`S28` | દરેક feature ને એક નંબર આપ્યો છે. File માં `[S07]` search કરો તો સીધા એ feature ના code પર પહોંચો. |
| 47–53 | "How to play with it" | DAG ને અલગ-અલગ config સાથે trigger કરીને શું શું જોઈ શકાય તેની યાદી. |

**આ docstring નો ખાસ ઉપયોગ:** Line 245 પર `doc_md=__doc__` લખ્યું છે → એટલે આ આખું લખાણ **Airflow UI માં DAG ના "Docs" પેજ પર Markdown તરીકે દેખાય છે.** એટલે જ અહીં Markdown table (`| ... |`) અને heading (`#`) વાપર્યા છે.

---

## 2. Lines 56–64 · Python ના સામાન્ય imports

```python
from __future__ import annotations

import logging
import os
import shutil
import tempfile
from datetime import timedelta

import pendulum
```

| Line | Code | અર્થ અને શા માટે |
|------|------|-------------------|
| 56 | `from __future__ import annotations` | Type hints (`key: str`, `-> None`) ને તરત evaluate ન કરે, string તરીકે રાખે. ફાયદો: જૂના Python version પર પણ નવી type-hint syntax ચાલે અને import થોડું ઝડપી થાય. **Airflow ના official examples માં પણ આ line હોય છે.** |
| 58 | `import logging` | Log લખવા માટે. Airflow task માં `print()` ને બદલે `log.info()` વાપરવું વધુ સારું – log level (INFO/WARNING/ERROR) મળે અને UI ના log માં સરસ દેખાય. |
| 59 | `import os` | Path બનાવવા (`os.path.join`), folder બનાવવા (`os.makedirs`), file છે કે નહીં તપાસવા (`os.path.exists`). |
| 60 | `import shutil` | આખું folder delete કરવા (`shutil.rmtree`) – teardown task માં વપરાય છે. |
| 61 | `import tempfile` | OS નું temp folder શોધવા (`/tmp` Linux પર, `C:\Users\...\Temp` Windows પર). એટલે demo files કોઈ પણ machine પર બને. |
| 62 | `from datetime import timedelta` | **સમયગાળો** (duration) દર્શાવવા: `timedelta(seconds=30)`, `timedelta(hours=2)`. Retry delay, timeout વગેરેમાં વપરાય. |
| 64 | `import pendulum` | **Timezone-aware** તારીખ માટે. Airflow અંદરથી pendulum જ વાપરે છે. સાદું `datetime(2026,1,1)` timezone વગરનું હોય – એનાથી schedule ખોટા સમયે ચાલી શકે. |

> 💡 Blank lines (57, 63, 65): PEP-8 style – standard library imports, third-party imports અને Airflow imports ને અલગ group માં રાખવા.

---

## 3. Lines 66–121 · Airflow 2 અને 3 બંનેમાં ચાલે એવા imports

### સમસ્યા શું છે?
Airflow 3 માં ઘણી વસ્તુઓની **import location બદલાઈ** ગઈ:

| વસ્તુ | Airflow 2.x | Airflow 3.x |
|-------|-------------|-------------|
| `DAG`, `task`, `Param`, `Variable`… | `airflow`, `airflow.decorators`, `airflow.models…` | **`airflow.sdk`** (Task SDK) |
| BashOperator, PythonOperator… | `airflow.operators.*` | **`airflow.providers.standard.*`** |
| Dataset | `airflow.datasets.Dataset` | **`airflow.sdk.Asset`** (નામ જ બદલાયું) |

### ઉકેલ: `try / except ImportError`

```python
try:  # ---------- Airflow 3.x (Task SDK) ----------
    from airflow.sdk import (DAG, BaseHook, BaseOperator, Label, Param, TaskGroup,
                             Variable, chain, dag, get_current_context, task, task_group)
    AIRFLOW_3 = True
except ImportError:  # ---------- Airflow 2.x ----------
    from airflow import DAG
    ...
    AIRFLOW_3 = False
```

| Line(s) | સમજૂતી |
|---------|--------|
| 69 | `try:` – પહેલાં Airflow 3 વાળો import **પ્રયત્ન** કરો. |
| 70–83 | `airflow.sdk` માંથી બધું એક સાથે import. કૌંસ `( )` વાપરવાથી લાંબી line ને ઘણી lines માં તોડી શકાય. |
| 85 | `AIRFLOW_3 = True` – પછીથી જરૂર પડે તો ખબર રહે કે કયું version ચાલે છે (flag). |
| 86 | `except ImportError:` – જો `airflow.sdk` ન મળે (એટલે Airflow 2 છે) તો આ block ચાલે. |
| 87–95 | એ જ વસ્તુઓ Airflow 2 ની જૂની જગ્યાએથી. |
| 97 | `AIRFLOW_3 = False` |

**દરેક imported નામ શા માટે જોઈએ છે?**

| નામ | ઉપયોગ |
|-----|-------|
| `DAG` | Main DAG `with DAG(...)` બનાવવા |
| `dag` (નાનો d) | `@dag` decorator – child અને consumer DAG માટે |
| `task` | `@task` decorator – Python function ને task બનાવવા |
| `task_group` | `@task_group` decorator |
| `TaskGroup` | `with TaskGroup(...)` – tasks ને box માં group કરવા |
| `Param` | DAG params (UI form) |
| `Variable` | Airflow Variables વાંચવા |
| `BaseHook` | Connection (`demo_conn`) વાંચવા |
| `BaseOperator` | પોતાનો custom operator બનાવવા |
| `Label` | Graph માં તીર પર લખાણ (edge label) |
| `chain` | ઘણા tasks ને એક સાથે ક્રમમાં જોડવા |
| `get_current_context` | Function ની અંદર context મેળવવા |

### Lines 99–106 · Dataset / Asset

```python
try:  # Airflow 3.x renamed Dataset → Asset
    from airflow.sdk import Asset
    HAS_ASSET = True
except ImportError:  # Airflow 2.4+
    from airflow.datasets import Dataset
    HAS_ASSET = False
```
આ import ને **અલગ try** માં રાખ્યો છે – જેથી ઉપરનો import ગમે તે રીતે થાય, Asset/Dataset નો નિર્ણય સ્વતંત્ર રીતે થાય. `HAS_ASSET` flag line 134 પર વપરાય છે.

### Lines 108–119 · Operators

```python
try:  # Airflow 3.x – core operators live in the "standard" provider
    from airflow.providers.standard.operators.bash import BashOperator
    ...
except ImportError:  # Airflow 2.x
    from airflow.operators.bash import BashOperator
    ...
```
| Operator | કામ |
|----------|-----|
| `BashOperator` | Shell command ચલાવે |
| `EmptyOperator` | કંઈ કરતો નથી – start/end/join marker |
| `LatestOnlyOperator` | ફક્ત સૌથી નવા run માં જ આગળ જવા દે |
| `PythonOperator` | Classic રીતે Python function ચલાવે |
| `TriggerDagRunOperator` | બીજા DAG ને ચાલુ (trigger) કરે |

### Line 121 · Exceptions
```python
from airflow.exceptions import AirflowFailException, AirflowSkipException
```
| Exception | raise કરો તો શું થાય |
|-----------|----------------------|
| `AirflowFailException` | Task **તરત FAIL** – retries હોય તો પણ retry **નહીં** થાય |
| `AirflowSkipException` | Task **SKIPPED** (ગુલાબી) – error નથી, ફક્ત "આજે કામ નથી" |

---

## 4. Lines 123–137 · Logger અને Constants

```python
log = logging.getLogger(__name__)
```
**Line 123:** આ module માટે logger બનાવ્યો. `__name__` = module નું નામ, એટલે log માં ખબર પડે કે message કઈ file માંથી આવ્યો. આખી file માં `log.info(...)`, `log.error(...)` આ જ logger વાપરે છે.

```python
LOCAL_TZ = "Asia/Kolkata"
DEMO_DIR = os.path.join(tempfile.gettempdir(), "airflow_demo")
FLAG_FILE = os.path.join(DEMO_DIR, "ready.flag")
SUMMARY_FILE = os.path.join(DEMO_DIR, "sales_summary.csv")
```

| Line | Constant | અર્થ |
|------|----------|------|
| 128 | `LOCAL_TZ` | ભારતનો timezone. Schedule `0 6 * * *` એટલે **ભારતીય સમય મુજબ સવારે 6 વાગ્યે** (UTC મુજબ નહીં). |
| 129 | `DEMO_DIR` | Temp folder ની અંદર `airflow_demo` folder નો path. |
| 130 | `FLAG_FILE` | Sensor જેની રાહ જુએ છે તે file (`ready.flag`). |
| 131 | `SUMMARY_FILE` | `publish_summary` task જે CSV લખે છે. |

> 💡 **Constants CAPITAL અક્ષરમાં** – Python convention: "આ value બદલાવાની નથી". એક જ જગ્યાએ લખવાથી, બદલવું હોય તો એક જ જગ્યાએ બદલવું પડે.
>
> ⚠️ આ top-level code છે, એટલે દરેક parse વખતે ચાલે – પણ એ ફક્ત string બનાવે છે (DB/network નથી), એટલે સસ્તું અને સલામત છે.

```python
if HAS_ASSET:
    SALES_DATASET = Asset(name="sales_summary", uri=f"file://{SUMMARY_FILE}")
else:
    SALES_DATASET = Dataset(f"file://{SUMMARY_FILE}")
```
**Lines 133–137 [S17]:** Dataset / Asset એ **data નો "logical pointer"** છે – એક નામ/URI જે કહે છે "આ data". Airflow ખરેખર file ખોલીને તપાસતું નથી; ફક્ત એટલું યાદ રાખે છે કે "કયા task એ આ Asset update કર્યું". Airflow 3 માં `name` અને `uri` બંને આપી શકાય, Airflow 2 માં ફક્ત URI.

---

## 5. Lines 143–155 · `get_variable()` helper [S08]

```python
def get_variable(key: str, default=None):
    try:
        try:
            return Variable.get(key, default=default)  # Airflow 3 signature
        except TypeError:
            return Variable.get(key, default_var=default)  # Airflow 2 signature
    except Exception:
        return default
```

| Line | સમજૂતી |
|------|--------|
| 143 | Function: Variable નું નામ (`key`) અને ન મળે તો default value. |
| 144–148 | Docstring – **મહત્વનો નિયમ**: Variable ને top-level પર ન વાંચો, કારણ કે દરેક parse (~30 સેકન્ડ) પર DB hit થાય. |
| 151 | Airflow 3 માં parameter નું નામ `default` છે. |
| 152–153 | Airflow 2 માં નામ `default_var` હતું. ખોટું નામ આપીએ તો Python `TypeError` આપે → એ પકડીને બીજી રીત try કરીએ. |
| 154–155 | DB ન મળે / Variable ન હોય / કોઈ પણ error → crash થવાને બદલે default પાછું આપો. `# noqa: BLE001` = linter ને કહેવું કે "બધા exception પકડવા એ જાણી જોઈને કર્યું છે". |

> **Variable એટલે શું?** Airflow UI → *Admin → Variables* માં રાખેલી key-value settings (દા.ત. `s3_bucket = my-bucket`). Code બદલ્યા વગર setting બદલી શકાય.

---

## 6. Lines 161–188 · Callbacks [S04]

**Callback** = એવું function જે Airflow **આપમેળે** બોલાવે છે જ્યારે કોઈ ઘટના બને (task fail થાય, success થાય, retry થાય…). દરેક callback ને `context` dictionary મળે છે.

```python
def on_task_failure(context):
    ti = context["task_instance"]
    log.error("TASK FAILED  dag=%s task=%s run=%s error=%s",
              ti.dag_id, ti.task_id, context["run_id"], context.get("exception"))
```

| Line(s) | Function | ક્યારે ચાલે | શું કરે |
|---------|----------|-------------|---------|
| 161–167 | `on_task_failure` | કોઈ task **fail** થાય (બધા retry પછી) | DAG id, task id, run id અને error log કરે. Line 167: real project માં અહીં Slack/Teams/email મોકલો. |
| 170–172 | `on_task_retry` | Task **retry** થાય | કેટલામો પ્રયત્ન છે (`try_number`) તે log કરે. |
| 175–176 | `on_task_success` | Task **success** થાય | Success log. |
| 179–180 | `on_task_execute` | Task **શરૂ થાય તે પહેલાં** | "Starting" log. |
| 183–184 | `on_dag_success` | **આખો DAG run** success | Run id log. |
| 187–188 | `on_dag_failure` | **આખો DAG run** fail | Run id અને કારણ (`reason`) log. |

**મુદ્દા:**
- `log.error("... %s", value)` – `%s` placeholder વાપરવું f-string કરતાં વધુ સારું: log level બંધ હોય તો string બનાવવાનો ખર્ચ બચે.
- `context.get("exception")` – `[...]` ને બદલે `.get()` કારણ કે key ન હોય તો error ન આવે, `None` મળે.
- આ callbacks ક્યાં જોડાયા છે? → `default_args` (line 235–236), DAG (line 255–256), અને અમુક tasks પર (line 273, 293, 558).

---

## 7. Lines 194–207 · Custom Operator [S25]

```python
class GreetOperator(BaseOperator):
    template_fields = ("name",)
    ui_color = "#ffe5b4"

    def __init__(self, name: str, **kwargs):
        super().__init__(**kwargs)
        self.name = name

    def execute(self, context):
        message = f"Hello {self.name}! run_id={context['run_id']}"
        self.log.info(message)
        return message
```

| Line | સમજૂતી |
|------|--------|
| 194 | `BaseOperator` માંથી inherit કરીને **પોતાનો operator** બનાવ્યો. બધા Airflow operators (Bash, Python…) આ જ રીતે બનેલા છે. |
| 195 | Docstring. |
| 197 | `template_fields = ("name",)` – **ખૂબ મહત્વનું.** Airflow ને કહે છે કે `name` field માં Jinja (`{{ ... }}`) હોય તો **run વખતે render** કરજે. આ ન લખો તો `"{{ params.owner_name }}"` એ જ text તરીકે છપાય. ⚠️ Tuple માં એક જ item હોય તો **comma `,` જરૂરી** – `("name",)`. |
| 198 | `ui_color` – Graph view માં આ task નો box રંગ (આછો નારંગી). |
| 200 | Constructor: `name` આપણો પોતાનો argument; `**kwargs` એટલે `task_id`, `retries` જેવા બીજા બધા arguments. |
| 201 | `super().__init__(**kwargs)` – parent `BaseOperator` ને બાકીના arguments આપો. **આ ભૂલી જાઓ તો task_id વગેરે set નહીં થાય.** |
| 202 | `self.name = name` – value object માં સાચવો. |
| 204 | `execute(self, context)` – **task run થાય ત્યારે Airflow આ જ method બોલાવે છે.** અહીં ખરું કામ થાય. |
| 205–206 | Message બનાવો અને log કરો (`self.log` = operator નો પોતાનો logger). |
| 207 | `return` કરેલી value **આપમેળે XCom માં** (`key="return_value"`) save થાય. |

> 💡 **ક્યારે custom operator બનાવવો?** જ્યારે એક જ પ્રકારનું કામ ઘણા DAG માં વારંવાર થતું હોય (દા.ત. "company ના API માંથી data લાવો") – એક વાર લખો, બધે વાપરો.

---

## 8. Lines 213–220 · Classic Python function [S09]

```python
def classic_python_fn(run_date: str, env: str, **context):
    ti = context["ti"]
    batch_size = ti.xcom_pull(task_ids="read_config", key="batch_size")
    log.info("Classic PythonOperator  run_date=%s env=%s batch_size=%s", run_date, env, batch_size)
    ti.xcom_push(key="classic_message", value=f"processed {batch_size} rows for {run_date}")
    return "classic_done"
```

| Line | સમજૂતી |
|------|--------|
| 213 | `run_date` અને `env` → `op_kwargs` (line 326) માંથી આવે છે. `**context` → Airflow બાકીનો context (ti, ds, params…) આપે છે. |
| 214 | `ti` = Task Instance – XCom માટે જરૂરી. |
| 216 | **XCom pull:** `read_config` task એ મોકલેલી `batch_size` value વાંચો. `read_config` માં `multiple_outputs=True` છે એટલે દરેક key અલગ XCom છે – એટલે `key="batch_size"` આપીને એક જ value લઈ શકાય. |
| 217 | Values log કરો. |
| 219 | **XCom push (manual):** પોતાની custom key `classic_message` સાથે value મોકલો. બીજો task `xcom_pull(task_ids="classic_python_task", key="classic_message")` થી વાંચી શકે. |
| 220 | `return` → આપમેળે `key="return_value"` તરીકે XCom માં. |

> 📝 આ function **DAG ની બહાર** (module level) લખ્યું છે – classic PythonOperator માં આવું જ કરાય. TaskFlow (`@task`) માં function DAG ની અંદર લખાય છે.

---

## 9. Lines 226–237 · `default_args` [S02]

`default_args` = **એક dictionary જે DAG ના દરેક task પર આપમેળે લાગુ થાય છે.** દરેક task માં વારંવાર `retries=1` લખવાની જરૂર નહીં. કોઈ task પોતાની value આપે તો તે override થાય (દા.ત. `flaky_task` માં `retries=3`).

| Line | Key | Value | અર્થ અને શા માટે |
|------|-----|-------|-------------------|
| 227 | `owner` | `"data_engineering"` | Task નો માલિક – UI માં દેખાય, filter કરી શકાય. |
| 228 | `retries` | `1` | Fail થાય તો **1 વાર ફરી** પ્રયત્ન. Network/DB ની ક્ષણિક (temporary) તકલીફમાં બહુ ઉપયોગી. |
| 229 | `retry_delay` | 30 સેકન્ડ | Retry પહેલાં કેટલી રાહ જોવી. |
| 230 | `retry_exponential_backoff` | `True` | દરેક retry વખતે રાહ **બમણી** થતી જાય (30s → 60s → 120s…). API ને વારંવાર મારો ન થાય. |
| 231 | `max_retry_delay` | 10 મિનિટ | Backoff ગમે તેટલું વધે, 10 મિનિટથી વધુ રાહ નહીં. |
| 232 | `execution_timeout` | 30 મિનિટ | Task 30 મિનિટથી વધુ ચાલે તો **kill કરીને fail.** અટકી ગયેલા (hang) task worker slot કાયમ ન રોકે. |
| 233 | `email_on_failure` | `False` | Fail પર email ન મોકલો (SMTP setup ન હોય તો error આવે). |
| 234 | `email_on_retry` | `False` | Retry પર email નહીં. |
| 235 | `on_failure_callback` | `on_task_failure` | દરેક task fail થાય ત્યારે section 6 નું function ચાલે. |
| 236 | `on_retry_callback` | `on_task_retry` | દરેક retry વખતે. |

---

## 10. Lines 242–268 · MAIN DAG ની વ્યાખ્યા [S01] [S03]

```python
with DAG(
    dag_id="airflow_all_scenarios",
    ...
) as main_dag:
```
**`with DAG(...) as main_dag:`** – Python નો *context manager*. આ block ની **અંદર બનાવેલો દરેક task આપમેળે આ DAG નો ભાગ** બને છે (દરેક task માં `dag=...` લખવાની જરૂર નથી). `main_dag` variable પછી line 703 પર `.test()` માટે વપરાય છે.

| Line | Argument | અર્થ અને શા માટે |
|------|----------|-------------------|
| 243 | `dag_id` | DAG નું **unique** નામ. આખા Airflow માં બે DAG નું એક નામ ન ચાલે. |
| 244 | `description` | DAG list માં દેખાતું ટૂંકું વર્ણન. |
| 245 | `doc_md=__doc__` | Section 1 નો docstring UI ના **Docs** માં બતાવો. |
| 247 | `schedule="0 6 * * *"` | **Cron**: મિનિટ=0, કલાક=6, બાકી બધું `*` → **રોજ સવારે 06:00**. બીજા વિકલ્પ: `"@daily"`, `timedelta(hours=1)`, `None` (ફક્ત manual), `[dataset]` (data આવે ત્યારે). |
| 248 | `start_date` | 1-Jan-2026, ભારતીય timezone. Scheduler આ તારીખથી run ગણવાનું શરૂ કરે. ⚠️ **ક્યારેય `datetime.now()` ન લખો** – દર parse પર બદલાય અને schedule ગૂંચવાય. |
| 249 | `catchup=False` | DAG ચાલુ કરો ત્યારે start_date થી આજ સુધીના **ચૂકી ગયેલા બધા દિવસના run ન બનાવો** – ફક્ત નવીનતમ. `True` કરો તો 300+ જૂના run એક સાથે બને! |
| 250 | `max_active_runs=1` | એક સમયે આ DAG નો **ફક્ત 1 run** ચાલે – runs એકબીજા પર ન ચડે. |
| 251 | `max_active_tasks=16` | આ DAG ના એક સાથે વધુમાં વધુ 16 tasks ચાલે. |
| 252 | `dagrun_timeout` | આખો run 2 કલાકથી વધુ ચાલે તો fail. |
| 253 | `default_args` | Section 9 ના defaults. |
| 254 | `tags` | UI માં filter કરવા માટે labels. |
| 255–256 | DAG callbacks | આખો run success/fail થાય ત્યારે. |
| 258–267 | `params` | નીચે જુઓ 👇 |

### Params (Lines 258–267) [S03]
**Params = DAG ના input.** UI માં **"Trigger DAG w/ config"** દબાવો ત્યારે એક form દેખાય છે; Airflow value ને **type અને range મુજબ validate** કરે છે, ખોટી હોય તો run શરૂ જ ન થાય.

| Line | Param | Default | Validation | ક્યાં વપરાય |
|------|-------|---------|------------|--------------|
| 259 | `env` | `"dev"` | ફક્ત `dev`/`qa`/`prod` (dropdown) | Branch (line 427), Bash env var |
| 260 | `batch_size` | `3` | integer, 1 થી 10 | Extract કેટલી rows બનાવે |
| 261 | `simulate_flaky` | `True` | boolean (checkbox) | `flaky_task` પહેલી વાર fail થાય |
| 262 | `simulate_failure` | `False` | boolean | `hard_fail_check` ને fail કરાવવા |
| 263 | `sensor_auto_pass` | `True` | boolean | Sensor તરત pass થાય |
| 264 | `run_spark` | `False` | boolean | ખરેખર `spark-submit` ચલાવવું કે નહીં |
| 265 | `spark_app` | path | string | Spark job ની file |
| 266 | `owner_name` | `"Data Team"` | string | Custom operator માં નામ |

**વાપરવાની બે રીત:** Jinja માં `{{ params.env }}`, Python માં `context["params"]["env"]`.

---

## 11. Line 273 · `start` task [S05]

```python
start = EmptyOperator(task_id="start", on_execute_callback=on_task_execute)
```
- `EmptyOperator` **કોઈ કામ નથી કરતો** – તરત success.
- **શા માટે?** Graph માં સ્પષ્ટ શરૂઆત (start) બતાવવા, અને ઘણા tasks ને એક જ બિંદુથી શરૂ કરવા.
- `on_execute_callback` – task શરૂ થાય તે પહેલાં `on_task_execute` (line 179) ચાલે – callback નો ડેમો.

---

## 12. Lines 278–294 · BashOperator + Jinja [S06]

```python
print_run_info = BashOperator(
    task_id="print_run_info",
    bash_command="""
        echo "ds            = {{ ds }}"
        ...
    """,
    env={"RUN_ENV": "{{ params.env }}"},
    append_env=True,
    depends_on_past=True,
    on_success_callback=on_task_success,
)
```

| Line | સમજૂતી |
|------|--------|
| 279 | `task_id` – DAG ની અંદર unique નામ. |
| 280–289 | `bash_command` – ત્રણ quote `"""` વાળું multi-line string. **`{{ ... }}` = Jinja template** – run વખતે Airflow તેની જગ્યાએ સાચી value મૂકે. |
| 281 | `{{ ds }}` → `2026-11-05` |
| 282 | `{{ ds_nodash }}` → `20261105` (file names માં ઉપયોગી) |
| 283 | `{{ macros.ds_add(ds, -1) }}` → ગઈકાલની તારીખ. `if ds else 'n/a'` – Airflow 3 માં manual run માં `ds` ખાલી હોઈ શકે, ત્યારે error ન આવે. |
| 284 | `{{ run_id }}` → run નું નામ (દા.ત. `scheduled__2026-11-05T00:30:00+00:00`). |
| 285 | Data interval – આ run કયા સમયગાળાનો data process કરે છે. |
| 286 | `{{ params.env }}` → Param ની value. |
| 287 | `{{ var.value.get('demo_variable', 'not-set') }}` → Variable વાંચો; ન હોય તો `not-set`. **Jinja માં Variable વાંચવું સલામત છે** કારણ કે render ફક્ત run વખતે થાય, parse વખતે નહીં. |
| 288 | `$RUN_ENV` – સામાન્ય shell environment variable (નીચે line 290 માં set કર્યું). |
| 290 | `env={...}` – command માટે environment variable; value પણ Jinja થી. |
| 291 | `append_env=True` – worker ના પહેલેથી રહેલા env vars (PATH, JAVA_HOME…) **પણ રાખો**. `False` હોય તો ફક્ત `RUN_ENV` જ રહે અને `echo` જેવા command પણ ન મળે એવું બને. |
| 292 | `depends_on_past=True` – **ગયા run માં આ જ task success થયો હોય તો જ આ run માં ચાલે.** Incremental load (ક્રમમાં જ ચાલવું જોઈએ) માટે ઉપયોગી. |
| 293 | Success પર callback. |

---

## 13. Lines 299–318 · TaskFlow `read_config` [S07] [S08]

```python
@task(multiple_outputs=True)
def read_config(**context):
    params = context["params"]
    source_system = get_variable("demo_variable", default="default_source")
    try:
        conn = BaseHook.get_connection("demo_conn")
        conn_host = conn.host
    except Exception:
        conn_host = "localhost"
    return {"env": ..., "batch_size": ..., "source_system": ..., "conn_host": ...}

cfg = read_config()
```

| Line | સમજૂતી |
|------|--------|
| 299 | `@task` – **TaskFlow API**: સામાન્ય Python function ને Airflow task બનાવે. Classic PythonOperator કરતાં ઓછો code અને XCom આપમેળે. `multiple_outputs=True` → return કરેલી dict ની **દરેક key અલગ XCom** બને. |
| 300 | `**context` લખવાથી Airflow આપમેળે context આપે. Task id = function નું નામ (`read_config`). |
| 301 | Params મેળવો. |
| 302 | Section 5 નું helper – Variable **task ની અંદર** વાંચ્યું (સાચી રીત). |
| 304–308 | **Connection** વાંચો. Connection = DB/API નું host, user, password – *Admin → Connections* માં સલામત રીતે રાખેલું. ન મળે તો `localhost`. **Password ક્યારેય code માં ન લખો – connection માં રાખો.** |
| 311–316 | Dictionary return → 4 XCom: `env`, `batch_size`, `source_system`, `conn_host`. |
| 318 | `cfg = read_config()` – function **"બોલાવવું"** અહીં ખરેખર run નથી કરતું! એ task બનાવે છે અને `cfg` એક **XComArg** (ભવિષ્યની value નો placeholder) છે. `cfg["batch_size"]` લખો તો "read_config ની batch_size XCom" નો reference મળે અને **dependency આપમેળે બને.** |

---

## 14. Lines 323–327 · Classic PythonOperator [S09]

```python
classic_task = PythonOperator(
    task_id="classic_python_task",
    python_callable=classic_python_fn,
    op_kwargs={"run_date": "{{ ds }}", "env": "{{ params.env }}"},
)
```
| Line | સમજૂતી |
|------|--------|
| 324 | Task id. |
| 325 | `python_callable` – કયું function ચલાવવું (section 8). **કૌંસ `()` વગર** – function આપવાનું છે, બોલાવવાનું નથી. |
| 326 | `op_kwargs` – function ને arguments. `op_kwargs` **templated field** છે એટલે `{{ ds }}` run વખતે તારીખ બની જાય. |

> **TaskFlow vs Classic – ક્યારે શું?** નવો code → `@task` (સરળ). જૂના project, અથવા એક જ function ને અલગ arguments સાથે ઘણા tasks માં વાપરવું હોય → PythonOperator. બંને આ file માં બતાવ્યા છે જેથી બંને ઓળખી શકો.

---

## 15. Lines 332–353 · TaskGroup + Nested TaskGroup [S10]

```python
with TaskGroup(group_id="extract", tooltip="Extract from all sources") as extract_tg:
    @task
    def extract_api(batch_size: int): ...
    with TaskGroup(group_id="database") as db_tg:
        @task
        def connect_db(host: str): ...
        @task
        def query_db(host: str, batch_size: int): ...
        db_rows = query_db(connect_db(cfg["conn_host"]), cfg["batch_size"])
    api_rows = extract_api(cfg["batch_size"])
```

**TaskGroup** = Graph માં tasks ને એક **ખોલ-બંધ થઈ શકે એવા box** માં મૂકવા. 50 tasks વાળો DAG પણ સ્વચ્છ દેખાય.

| Line | સમજૂતી |
|------|--------|
| 332 | `extract` નામનું group. `tooltip` – UI માં hover કરો ત્યારે દેખાય. |
| 334–338 | `extract_api` – API માંથી data લાવવાનું *simulation*: `batch_size` જેટલી rows ની list બનાવે. Task id બને છે **`extract.extract_api`** (group નું નામ આગળ લાગે). |
| 336 | List comprehension: `i = 1..batch_size` માટે `{"id", "source", "amount"}` dict. |
| 340 | **Nested group** – group ની અંદર group. Id: `extract.database`. |
| 342–345 | `connect_db` – DB સાથે જોડાવાનું simulation, host પાછો આપે. |
| 347–349 | `query_db` – DB rows નું simulation (id 101, 102…). |
| 351 | **Data dependency:** `connect_db(...)` નું output સીધું `query_db` ને આપ્યું → Airflow આપમેળે `connect_db >> query_db` dependency બનાવે. `cfg[...]` આપ્યું એટલે `read_config >> ...` dependency પણ આપમેળે. |
| 353 | `api_rows` – extract_api નું output (પછી transform માં વપરાય). |

---

## 16. Lines 358–398 · Dynamic Task Mapping [S11] [S12]

**Dynamic Task Mapping** = **Run વખતે** નક્કી થાય કે task ની કેટલી copies બનાવવી. દા.ત. આજે 3 files આવી → 3 copies, કાલે 100 → 100 copies. Code બદલવાની જરૂર નહીં. (Spark ના `map` જેવો જ વિચાર.)

| Line | Function | કામ |
|------|----------|-----|
| 360–364 | `validate(api, db)` | બંને list ભેગી કરી, `amount >= 0` વાળી rows જ રાખે. |
| 366–371 | `@task(...)` settings | **[S26]** `max_active_tis_per_dag=4` → mapped copies માંથી એક સમયે ફક્ત 4 ચાલે (DB/API પર ભાર ન પડે). `pool="default_pool"` → pool = resource limit (Admin → Pools). `priority_weight=5` → slot ઓછા હોય ત્યારે વધુ priority. `weight_rule="downstream"` → priority ગણતી વખતે downstream tasks નું weight પણ ઉમેરાય. |
| 372–374 | `process_record(record, multiplier)` | **દરેક record માટે એક અલગ task instance** – amount × multiplier. |
| 376–380 | `aggregate(values)` | **Reduce step [S12]**: બધી mapped copies ના results નો સરવાળો. `values` એક lazy list છે. |
| 382–385 | `build_partition_kwargs(rows)` | દરેક source (api, db@localhost) માટે `{"partition": ..., "row_count": ...}` ની list બનાવે. |
| 387–389 | `write_partition(partition, row_count)` | એક partition લખવાનું simulation. |

```python
validated = validate(api_rows, db_rows)                                   # line 391
processed = process_record.partial(multiplier=2).expand(record=validated) # line 394
total = aggregate(processed)                                              # line 395
write_partition.expand_kwargs(build_partition_kwargs(validated))          # line 398
```

| Line | સમજૂતી |
|------|--------|
| 391 | Validate ચલાવો. |
| 394 | **`.partial(multiplier=2)`** = બધી copies માટે **સરખો** argument. **`.expand(record=validated)`** = list ની **દરેક item માટે એક copy**. batch_size=3 હોય તો 6 records → `process_record` ની 6 copies (UI માં `[0]`…`[5]`). |
| 395 | Fan-in: બધી copies પૂરી થાય પછી `aggregate` એક વાર ચાલે. |
| 398 | **`.expand_kwargs(list_of_dicts)`** = દરેક dict ના **ઘણા arguments** એક copy ને. અહીં 2 partitions → 2 copies. |

```
validate ──► process_record[0..5] ──► aggregate      (fan-out → fan-in)
        └──► build_partition_kwargs ──► write_partition[0..1]
```

---

## 17. Lines 403–420 · `@task_group` decorator [S13]

```python
@task_group(group_id="quality_checks")
def quality_checks(total_amount):
    @task
    def check_not_negative(value: int):
        if value < 0:
            raise AirflowFailException(f"Total is negative: {value}")
        return True
    @task
    def check_threshold(value: int, threshold: int = 100_000):
        if value > threshold:
            log.warning(...)
        return value <= threshold
    check_not_negative(total_amount)
    check_threshold(total_amount)

qc = quality_checks(total)
```

| Line | સમજૂતી |
|------|--------|
| 403 | TaskGroup બનાવવાની **decorator રીત** (section 15 માં `with TaskGroup` રીત હતી). ફાયદો: function ની જેમ **argument** લઈ શકે અને **ફરી-ફરી વાપરી** શકાય. |
| 405–409 | **Data quality check**: total negative હોય તો `AirflowFailException` → retry વગર તરત fail (ખોટો data આગળ ન જાય). |
| 411–415 | Threshold check: 1,00,000 થી વધુ હોય તો ફક્ત warning (fail નહીં). `100_000` – Python માં `_` થી મોટા આંકડા વાંચવામાં સરળ. |
| 417–418 | બંને checks **parallel** ચાલે (એકબીજા પર આધાર નથી). |
| 420 | Group બોલાવો; `total` (aggregate નું output) આપ્યું → dependency આપમેળે. `qc` એટલે TaskGroup object. |

---

## 18. Lines 425–451 · Branching + Labels + Join [S14] [S15]

```python
@task.branch
def choose_load_path(**context):
    env = context["params"]["env"]
    if env == "prod":
        return "load_prod"
    if env == "qa":
        return ["load_dev", "load_prod"]
    return "load_dev"
```

| Line | સમજૂતી |
|------|--------|
| 425 | `@task.branch` – **if/else** જેવું: function જે **task_id return કરે તે જ downstream task ચાલે**, બાકીના **SKIP**. |
| 427 | Param `env` વાંચો. |
| 428–429 | `prod` → ફક્ત `load_prod`. |
| 430–431 | `qa` → **list** return → **બંને** branches ચાલે. |
| 432 | બાકી (dev) → `load_dev`. |
| 434–440 | બે load tasks – ખરેખર warehouse માં load કરવાનું simulation. |
| 442–444 | Tasks બનાવો; `total` આપ્યું એટલે `aggregate >> load_*` dependency પણ આપમેળે. |

```python
join = EmptyOperator(task_id="join", trigger_rule="none_failed_min_one_success")
```
**Line 447 – ખૂબ મહત્વની:** Default trigger rule `all_success` = "બધા upstream **success** હોય તો જ ચાલુ". Branch પછી એક branch **skipped** હોય છે → `all_success` હોય તો `join` પણ skip થઈ જાય અને આખો બાકીનો DAG અટકી જાય! **`none_failed_min_one_success`** = "કોઈ fail નથી અને ઓછામાં ઓછો એક success છે" → join ચાલે. 👉 **Branch પછીના join માં હંમેશા આ rule વાપરો.**

```python
qc >> branch
branch >> Label("env = dev / qa") >> load_dev_task >> join
branch >> Label("env = prod / qa") >> load_prod_task >> join
```
| Line | સમજૂતી |
|------|--------|
| 449 | `>>` = **"પછી"** (dependency). Quality checks પૂરા થાય પછી branch. |
| 450–451 | `Label(...)` – Graph view માં **તીર પર લખાણ** દેખાય – કઈ સ્થિતિમાં કયો રસ્તો, તે સ્પષ્ટ થાય. |

---

## 19. Lines 456–483 · ShortCircuit → Dataset → Trigger child [S16] [S17] [S18]

### ShortCircuit (Lines 456–461)
```python
@task.short_circuit(ignore_downstream_trigger_rules=False)
def has_data(total_amount: int):
    return total_amount > 0
```
- Function **`False`** return કરે → downstream tasks **skip** ("data જ નથી તો publish શા માટે?").
- `True` → સામાન્ય રીતે આગળ.
- **`ignore_downstream_trigger_rules=False`**: ફક્ત **સીધા** downstream (publish) skip થાય; આગળના tasks (`cleanup`, `end`) **પોતાના trigger rule** મુજબ ચાલે. Default `True` હોય તો **બધા** downstream skip થઈ જાત – `end` પણ!

> **Branch vs ShortCircuit:** Branch = "કયો રસ્તો?" (ઘણા વિકલ્પ). ShortCircuit = "આગળ જવું કે નહીં?" (હા/ના).

### Dataset producer (Lines 463–469)
```python
@task(outlets=[SALES_DATASET])
def publish_summary(total_amount: int, **context):
    os.makedirs(DEMO_DIR, exist_ok=True)
    with open(SUMMARY_FILE, "w", encoding="utf-8") as f:
        f.write("run_id,total\n")
        f.write(f"{context['run_id']},{total_amount}\n")
```
| Line | સમજૂતી |
|------|--------|
| 463 | **`outlets=[SALES_DATASET]`** – task success થાય ત્યારે Airflow નોંધે "SALES_DATASET update થયું" → જે DAG આ dataset પર schedule છે (section 26 નો consumer) તે **આપમેળે ચાલુ** થાય. |
| 465 | Folder બનાવો; `exist_ok=True` = હોય તો error નહીં. |
| 466 | File લખવા ખોલો. `with` = કામ પૂરું થાય એટલે file આપમેળે બંધ. `encoding="utf-8"` = Windows/Linux બંને પર સરખું. |
| 467–468 | CSV header અને એક row. |
| 469 | Log. |

> 💡 **Dataset scheduling નો ફાયદો:** "રોજ 6 વાગ્યે" ને બદલે "**data તૈયાર થાય ત્યારે**" ચલાવવું. Producer મોડો પડે તો consumer પણ આપમેળે રાહ જુએ.

### TriggerDagRunOperator (Lines 471–480)
```python
trigger_child = TriggerDagRunOperator(
    task_id="trigger_child_dag",
    trigger_dag_id="airflow_all_scenarios_child",
    conf={"source_run_id": "{{ run_id }}",
          "total": "{{ ti.xcom_pull(task_ids='transform.aggregate') }}",
          "env": "{{ params.env }}"},
    wait_for_completion=False,
)
```
| Line | સમજૂતી |
|------|--------|
| 473 | કયો DAG ચાલુ કરવો (section 25). |
| 474–478 | **`conf`** = child DAG ને મોકલાતો data. Templated છે એટલે run id, aggregate નું total (XCom), env – બધું run વખતે ભરાય. ધ્યાન: task id `transform.aggregate` (group prefix સાથે). |
| 479 | `False` = child ને ચાલુ કરીને તરત આગળ. `True` = child પૂરો થાય ત્યાં સુધી રાહ (ત્યારે `deferrable=True` વાપરવું જેથી worker slot ન રોકાય). |

```python
publish = publish_summary(total)
join >> has_data(total) >> publish >> trigger_child
```
**Lines 482–483:** ક્રમ: `join → has_data → publish_summary → trigger_child_dag`.

---

## 20. Lines 488–534 · Setup / Teardown + Spark [S19] [S20]

**Setup/Teardown** = Python ના `try/finally` જેવું:
- **Setup** = resource તૈયાર કરો (Spark/EMR/Databricks cluster, temp folder).
- **Teardown** = resource સાફ કરો – **વચ્ચેનું કામ fail થાય તો પણ teardown ચાલે જ.** નહીં તો cluster ચાલુ રહી જાય અને **cloud bill** વધે!

| Line | સમજૂતી |
|------|--------|
| 488 | `spark` નામનું TaskGroup. |
| 490–496 | **`create_workspace` (setup):** run id માંથી `:` અને `+` કાઢીને (folder name માં ન ચાલે) safe folder name બનાવે, folder બનાવે, path return કરે. Line 492 – real life માં અહીં cluster બનાવો. |
| 498–502 | **`delete_workspace` (teardown):** folder delete. `ignore_errors=True` = folder ન હોય તો પણ error નહીં. |
| 504–515 | **`spark_job`** – નીચે વિગત 👇 |
| 517–528 | Comment માં **SparkSubmitOperator** નું ઉદાહરણ – provider install કર્યા પછી production માં આ વાપરો. |

### Spark job નો Jinja `{% if %}` (Lines 506–514)
```python
bash_command=(
    "{% if params.run_spark %}"
    "spark-submit --master local[*] --name airflow_demo_{{ ds_nodash }} "
    "{{ params.spark_app }} --run-date {{ ds }}"
    "{% else %}"
    "echo 'run_spark=False → skipping spark-submit. "
    "workspace={{ ti.xcom_pull(task_ids=\"spark.create_workspace\") }}'"
    "{% endif %}"
),
```
- કૌંસ `( )` ની અંદર બાજુ-બાજુમાં લખેલા strings Python **આપમેળે જોડી** દે છે – લાંબો command વાંચવામાં સરળ.
- `{% if %}…{% else %}…{% endif %}` = **Jinja નું if/else** – `{{ }}` value માટે, `{% %}` logic માટે.
- `run_spark=True` → ખરેખર `spark-submit` (local mode, app name માં તારીખ, `--run-date` argument).
- `run_spark=False` (default) → ફક્ત `echo` – Spark install ન હોય તો પણ demo ચાલે.
- `\"` – Python string ની અંદર double-quote escape.

### Wiring (Lines 530–534)
```python
workspace = create_workspace()
teardown = delete_workspace(workspace).as_teardown(setups=workspace)
workspace >> spark_job >> teardown
join >> spark_tg
```
| Line | સમજૂતી |
|------|--------|
| 530 | Setup task બનાવો. |
| 531 | `delete_workspace(workspace)` – path XCom થી મળે. **`.as_teardown(setups=workspace)`** – Airflow ને કહે છે "આ teardown છે, તેનો setup `workspace` છે" → વચ્ચેનું fail થાય તો પણ આ ચાલશે, અને teardown નું fail થવું DAG ને લાલ નહીં કરે. |
| 532 | ક્રમ: setup → spark_job → teardown. |
| 534 | આખું group `join` પછી. |

---

## 21. Lines 539–591 · Sensor, Retry, Fail/Skip, Alert [S21]–[S23]

### Sensor (Lines 539–551) [S21]
**Sensor** = **કોઈ શરત પૂરી થાય ત્યાં સુધી રાહ જોતો** task (file આવી? બીજો DAG પૂરો થયો? API ready?).

```python
@task.sensor(poke_interval=10, timeout=60, mode="reschedule", soft_fail=True)
def wait_for_flag_file(**context):
    if context["params"]["sensor_auto_pass"]:
        return True
    return os.path.exists(FLAG_FILE)
```
| Argument / Line | અર્થ |
|-----------------|------|
| `poke_interval=10` | દર 10 સેકન્ડે શરત તપાસો. |
| `timeout=60` | 60 સેકન્ડમાં શરત પૂરી ન થાય તો હાર માનો. |
| `mode="reschedule"` | તપાસ વચ્ચે **worker slot છોડી દો**. `"poke"` હોય તો આખો સમય slot રોકાય – લાંબી રાહ માં worker ખૂટી પડે. |
| `soft_fail=True` | Timeout પર **FAILED ને બદલે SKIPPED**. |
| 546–547 | Param `sensor_auto_pass=True` (default) → તરત `True` → demo અટકે નહીં. |
| 549 | `True` return = શરત પૂરી → sensor success. `False` = ફરી 10 સેકન્ડ પછી. |
| 550–551 | બીજા sensors ની યાદી. **Deferrable** sensors માટે `airflow triggerer` process ચાલુ હોવી જોઈએ. |

### Flaky task – Retry demo (Lines 553–564) [S22]
```python
@task(trigger_rule="none_failed", retries=3, retry_delay=timedelta(seconds=5),
      retry_exponential_backoff=True, on_retry_callback=on_task_retry)
def flaky_task(**context):
    ti = context["ti"]
    if context["params"]["simulate_flaky"] and ti.try_number < 2:
        raise RuntimeError(...)
    log.info("Succeeded on try %s", ti.try_number)
```
| Line | સમજૂતી |
|------|--------|
| 554 | `none_failed` – sensor soft-fail થઈને **skip** થાય તો પણ આ task ચાલે (skip ને failure ગણતું નથી). |
| 555–558 | આ task માટે `default_args` **override**: 3 retries, 5 સેકન્ડ, backoff, retry callback. |
| 562 | `try_number` = કેટલામો પ્રયત્ન (પહેલો = 1). પહેલા પ્રયત્નમાં → error. |
| 563 | `RuntimeError` → Airflow retry કરે (UI માં **પીળો** "up_for_retry"). |
| 564 | બીજા પ્રયત્નમાં success. 👉 **Real life:** network blip, DB lock જેવી ક્ષણિક તકલીફ retry થી આપમેળે ઉકેલાય. |

### Hard fail (Lines 566–570) [S23]
```python
@task(retries=5)
def hard_fail_check(**context):
    if context["params"]["simulate_failure"]:
        raise AirflowFailException("...")
```
`retries=5` હોવા છતાં **`AirflowFailException` retry ને અવગણે** અને તરત fail. **ક્યારે વાપરવું?** ખોટો config, ખરાબ data – જ્યાં retry થી કંઈ સુધરવાનું નથી; ખોટી રાહ અને resource બચે.

### Weekday-only (Lines 572–577) [S23]
```python
run_dt = context.get("logical_date") or pendulum.now(LOCAL_TZ)
if run_dt.isoweekday() >= 6:
    raise AirflowSkipException("Weekend → skipping this task")
```
- `logical_date` ન હોય (Airflow 3 manual run) તો હાલનો સમય.
- `isoweekday()`: સોમ=1 … શનિ=6, રવિ=7 → શનિ-રવિ **skip** (error નહીં, ફક્ત ગુલાબી skipped).

### Alert on failure (Lines 579–582) [S15]
```python
@task(trigger_rule="one_failed")
def alert_on_failure(): ...
```
**`one_failed`** = upstream માંથી **કોઈ એક પણ fail** થાય તો જ ચાલે. બધું બરાબર હોય તો skip. Alert / rollback / cleanup માટે ઉત્તમ.

### Wiring (Lines 584–591)
```python
sensor = wait_for_flag_file()
flaky = flaky_task()
hard_fail = hard_fail_check()
weekday = weekday_only_task()

join >> sensor >> flaky >> hard_fail
join >> weekday
[flaky, hard_fail] >> alert_on_failure()
```
- `[a, b] >> c` = a **અને** b બંને પછી c (list વાપરીને ઘણા tasks એક સાથે જોડાય).

---

## 22. Lines 596–601 · LatestOnlyOperator [S24]

```python
latest_only = LatestOnlyOperator(task_id="latest_only")
send_daily_report = BashOperator(task_id="send_daily_report", bash_command='echo "..."')
```
- **Backfill/catchup** માં જૂની તારીખોના run ચાલે ત્યારે `latest_only` પછીના tasks **skip** થાય; ફક્ત **સૌથી નવા** run માં ચાલે.
- **શા માટે?** 30 દિવસનો backfill કરો ત્યારે મેનેજરને 30 report emails ન જવા જોઈએ – data બધો process થાય, પણ report ફક્ત એક વાર.
- (Manual trigger પર latest_only skip કરતું નથી.)

---

## 23. Lines 606–614 · Custom operator નો ઉપયોગ + Mapped Bash [S25] [S11]

```python
greet = GreetOperator(task_id="greet", name="{{ params.owner_name }}")
```
**Line 606:** Section 7 નો custom operator વાપર્યો. `name` template field છે એટલે log માં `Hello Data Team!` આવે.

```python
mapped_bash = BashOperator.partial(task_id="mapped_bash").expand(
    bash_command=['echo "cleaning table: orders"', 'echo "cleaning table: customers"', 'echo "cleaning table: products"']
)
```
**Lines 608–614:** Mapping ફક્ત `@task` માટે નથી – **classic operator** પણ map થાય. `.partial(task_id=...)` = બધી copies માટે સરખું, `.expand(bash_command=[...])` = 3 commands → **3 copies** (`mapped_bash[0]`, `[1]`, `[2]`).

---

## 24. Lines 619–636 · Cleanup, End અને બધી dependencies [S15] [S27]

```python
cleanup = BashOperator(task_id="cleanup", bash_command='echo "..."', trigger_rule="all_done")
end = EmptyOperator(task_id="end", trigger_rule="none_failed_min_one_success")
```
| Line | Task | Trigger rule | અર્થ |
|------|------|--------------|------|
| 619–623 | `cleanup` | **`all_done`** | upstream success, fail કે skip – **ગમે તે થાય**, બધા પૂરા થાય એટલે ચાલે. Temp files સાફ કરવા આદર્શ. |
| 626 | `end` | `none_failed_min_one_success` | કોઈ fail ન હોય તો green. `hard_fail_check` fail થાય તો `end` "upstream_failed" થાય → **DAG run લાલ** (એ જ જોઈએ છે – failure છુપાવવી નથી). |

```python
chain(start, print_run_info, cfg, classic_task, extract_tg)   # line 629
start >> latest_only >> send_daily_report                       # line 630
join >> [greet, mapped_bash]                                    # line 631

leaves = [trigger_child, spark_tg, hard_fail, weekday, send_daily_report, greet, mapped_bash]  # 633
leaves >> cleanup                                               # 634
leaves >> end                                                   # 635
cleanup >> end                                                  # 636
```
| Line | સમજૂતી |
|------|--------|
| 629 | **`chain(a, b, c, d, e)`** = `a >> b >> c >> d >> e` – લાંબી સાંકળ ટૂંકમાં. TaskGroup (`extract_tg`) પણ સાંકળમાં ચાલે. |
| 630 | `start` પછી latest_only પછી report (બીજી parallel શાખા). |
| 631 | `join` પછી બંને parallel. |
| 633 | **Leaves** = જે tasks ની પાછળ બીજું કંઈ નથી (શાખાના છેડા). |
| 634 | બધા છેડા પૂરા થાય પછી `cleanup`. |
| 635–636 | બધા છેડા **અને** cleanup પછી `end`. |

---

## 25. Lines 642–668 · CHILD DAG [S18]

```python
@dag(
    dag_id="airflow_all_scenarios_child",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz=LOCAL_TZ),
    catchup=False,
    tags=["learning", "all-scenarios"],
    default_args={"owner": "data_engineering", "retries": 0},
)
def all_scenarios_child():
    @task
    def read_trigger_conf():
        context = get_current_context()
        conf = context["dag_run"].conf or {}
        log.info(...)
        return conf
    read_trigger_conf()

all_scenarios_child()
```

| Line | સમજૂતી |
|------|--------|
| 642 | **`@dag` decorator** – DAG બનાવવાની બીજી રીત (`with DAG` ને બદલે). Function ની અંદરના tasks આ DAG ના બને. |
| 644 | `schedule=None` – **પોતાની જાતે ક્યારેય નહીં ચાલે**; ફક્ત trigger (main DAG, UI કે API) થી. |
| 648 | આ DAG માટે અલગ default_args – retries 0. |
| 653 | `get_current_context()` – `**context` parameter વગર પણ context મેળવવાની રીત. |
| 654 | **`dag_run.conf`** – main DAG ના `TriggerDagRunOperator` એ મોકલેલી dictionary. Manual run માં conf ન હોય તો `None` → `or {}` થી ખાલી dict, જેથી `.get()` પર error ન આવે. |
| 655–656 | મળેલી values log. |
| 659 | Task બનાવો. |
| 661–665 | Comment: **ExternalTaskSensor** – બીજા DAG ના task ની રાહ જોવાની રીત (બંનેની logical date સરખી હોવી જોઈએ). |
| 668 | ⚠️ **`all_scenarios_child()` બોલાવવું ફરજિયાત!** `@dag` function ને બોલાવો ત્યારે જ DAG object બને અને Airflow ને દેખાય. ભૂલી જાઓ તો DAG UI માં **દેખાશે જ નહીં** – બહુ સામાન્ય ભૂલ. |

---

## 26. Lines 674–695 · CONSUMER DAG [S17]

```python
@dag(dag_id="airflow_all_scenarios_dataset_consumer",
     schedule=[SALES_DATASET], ...)
def all_scenarios_dataset_consumer():
    @task
    def consume_summary():
        context = get_current_context()
        events = context.get("triggering_asset_events") or context.get("triggering_dataset_events")
        log.info(...)
        if os.path.exists(SUMMARY_FILE):
            with open(SUMMARY_FILE, encoding="utf-8") as f:
                log.info("Summary file content:\n%s", f.read())
    consume_summary()

all_scenarios_dataset_consumer()
```

| Line | સમજૂતી |
|------|--------|
| 676 | **`schedule=[SALES_DATASET]`** – સમય પર નહીં, **data update પર** schedule. `publish_summary` (line 463) success થાય એટલે આ DAG આપમેળે ચાલે. List માં ઘણા datasets આપો તો **બધા** update થાય પછી ચાલે. |
| 686 | કયા dataset event થી trigger થયું તે માહિતી. Airflow 3 માં key `triggering_asset_events`, Airflow 2 માં `triggering_dataset_events` → `or` થી બંને સંભાળ્યા. |
| 688–690 | File હોય તો તેની content log કરો. |
| 692, 695 | Task બનાવો અને DAG function બોલાવો (section 25 જેવું જ). |

```
MAIN DAG: publish_summary ──(outlets)──► SALES_DATASET ──(schedule)──► CONSUMER DAG
```

---

## 27. Lines 702–703 · `dag.test()` [S28]

```python
if __name__ == "__main__":
    main_dag.test(run_conf={"env": "qa"})
```
| Line | સમજૂતી |
|------|--------|
| 702 | `__name__ == "__main__"` – file ને **સીધી** `python airflow_all_scenarios_dag.py` થી ચલાવો ત્યારે જ સાચું. Airflow scheduler import કરે ત્યારે ખોટું → આ code scheduler માં **નહીં ચાલે**. |
| 703 | **`dag.test()`** – scheduler/webserver વગર આખો DAG **એક જ process માં** ચલાવે અને log terminal માં બતાવે. Debugging માટે ઉત્તમ (VS Code breakpoint પણ લાગે). `run_conf={"env": "qa"}` = params override → બંને branches ચાલશે. |

> શરત: Airflow install હોય અને એક વાર `airflow db migrate` કર્યું હોય.

---

## 28. આખો flow એક નજરમાં

```
start ─┬─► print_run_info ─► read_config ─► classic_python_task ─► [extract group]
       │                                                            ├─ extract_api
       │                                                            └─ [database] connect_db ─► query_db
       │                                                                        │
       │                     [transform group] validate ─► process_record[0..n] ─► aggregate
       │                                            └─► build_partition_kwargs ─► write_partition[0..n]
       │                                                                        │
       │                     [quality_checks] check_not_negative, check_threshold
       │                                                                        │
       │                     choose_load_path ──(dev/qa)──► load_dev ──┐
       │                                      └─(prod/qa)─► load_prod ─┴─► join
       │                                                                    │
       │      ┌────────────────────┬──────────────────────┬────────────────┼──────────────┬──────────────┐
       │      ▼                    ▼                      ▼                ▼              ▼              ▼
       │  has_data ─► publish   [spark group]       wait_for_flag   weekday_only      greet       mapped_bash[0..2]
       │   (short     _summary   create_workspace     _file (sensor)    _task
       │   circuit)     │         ─► spark_job            │
       │                ▼         ─► delete_workspace     ▼
       │        trigger_child_dag   (teardown)        flaky_task ─► hard_fail_check
       │                                                   └────────┬──────┘
       │                                                            ▼
       │                                                    alert_on_failure (one_failed)
       │
       └─► latest_only ─► send_daily_report

   બધા છેડા (leaves) ─► cleanup (all_done) ─► end (none_failed_min_one_success)
```

---

## 29. અલગ-અલગ રીતે trigger કરો તો શું થશે?

UI → DAG → **▶ Trigger DAG w/ config** → JSON આપો:

| Config | પરિણામ |
|--------|---------|
| *(કંઈ નહીં – default)* | બધું **green**. `flaky_task` એક વાર પીળો (retry) પછી green. `load_dev` ચાલે, `load_prod` skip. `alert_on_failure` skip. |
| `{"env": "prod"}` | `load_prod` ચાલે, `load_dev` skip. |
| `{"env": "qa"}` | **બંને** load tasks ચાલે. |
| `{"simulate_failure": true}` | `hard_fail_check` તરત **લાલ** (retry વગર) → `alert_on_failure` ચાલે → `end` upstream_failed → **DAG run લાલ**. `cleanup` તો પણ ચાલે (all_done). |
| `{"simulate_flaky": false}` | `flaky_task` પહેલા પ્રયત્નમાં જ success. |
| `{"batch_size": 0}` | **Run શરૂ જ નહીં થાય** – Param validation (minimum=1) error. |
| `{"batch_size": 10}` | 20 records → `process_record` ની 20 copies (એક સમયે 4). |
| `{"sensor_auto_pass": false}` | Sensor 60 સેકન્ડ flag file ની રાહ જુએ → ન મળે તો **skipped** → `flaky_task` તો પણ ચાલે (none_failed). |
| `{"run_spark": true}` | ખરેખર `spark-submit` ચાલે (Spark install અને `spark_app` file હોવી જોઈએ). |

દરેક successful run પછી: **consumer DAG** (dataset) અને **child DAG** (trigger) બંને આપમેળે ચાલે.

---

## 30. Trigger rules – cheat sheet

| Trigger rule | ક્યારે ચાલે | ઉપયોગ |
|--------------|-------------|--------|
| `all_success` *(default)* | બધા upstream success | સામાન્ય ક્રમ |
| `all_failed` | બધા upstream fail | Fallback |
| `all_done` | બધા પૂરા (કોઈ પણ state) | **Cleanup** (line 622) |
| `one_success` | એક પણ success થાય એટલે તરત | Race – પહેલું પૂરું થાય તે |
| `one_failed` | એક પણ fail થાય એટલે તરત | **Alert / rollback** (line 580) |
| `none_failed` | કોઈ fail નહીં (skip ચાલે) | Sensor soft-fail પછી (line 554) |
| `none_failed_min_one_success` | કોઈ fail નહીં + ઓછામાં ઓછો 1 success | **Branch પછી join** (line 447, 626) |
| `none_skipped` | કોઈ skip નહીં | |
| `always` | હંમેશા | Debug |

---

## 31. સામાન્ય ભૂલો (Common mistakes)

| ❌ ભૂલ | ✅ સાચી રીત | આ file માં ક્યાં |
|--------|-------------|------------------|
| Top-level પર `Variable.get()` / DB call | Task ની અંદર, અથવા Jinja `{{ var.value.x }}` | Lines 143–155, 287, 302 |
| `start_date=datetime.now()` | Fixed તારીખ + timezone | Line 248 |
| Branch પછી join માં default `all_success` | `none_failed_min_one_success` | Line 447 |
| `catchup=True` ભૂલથી → સેંકડો જૂના run | `catchup=False` (જરૂર હોય ત્યારે જ True) | Line 249 |
| Custom operator માં `template_fields` ભૂલવું → Jinja render ન થાય | `template_fields = ("name",)` (comma સાથે) | Line 197 |
| `@dag` function ને બોલાવવાનું ભૂલવું → DAG દેખાય નહીં | `all_scenarios_child()` | Lines 668, 695 |
| Cluster delete કરવાનું સામાન્ય task → fail પર cluster ચાલુ રહી જાય | Teardown (`as_teardown`) | Line 531 |
| લાંબી રાહ માટે sensor `mode="poke"` → worker ખૂટે | `mode="reschedule"` અથવા deferrable | Line 542 |
| XCom માં મોટો DataFrame | Storage માં લખો, XCom માં ફક્ત path | Section 0 (4) |
| Password code માં | Airflow Connection | Lines 304–308 |
| ક્યારેય સુધરે નહીં એવી error પર retries | `AirflowFailException` | Lines 566–570 |

---

## 32. શબ્દકોશ (Glossary)

| શબ્દ | ગુજરાતીમાં અર્થ |
|------|-----------------|
| **DAG** | Tasks નો દિશાવાળો, loop વગરનો નકશો |
| **Task** | DAG માં કામનો એક એકમ |
| **Operator** | Task નો પ્રકાર (Bash, Python, Spark…) |
| **Task Instance** | કોઈ ચોક્કસ run માં task નું એક execution |
| **DAG Run** | DAG નું એક આખું execution (દા.ત. 5-Nov નું) |
| **Scheduler** | DAG files વાંચીને નક્કી કરે કે ક્યારે શું ચલાવવું |
| **Worker** | ખરેખર task ચલાવનાર process / machine |
| **Triggerer** | Deferrable tasks ની રાહ async રીતે જોનાર process |
| **XCom** | Tasks વચ્ચે નાનો data મોકલવાની રીત |
| **Context** | Run ની માહિતીવાળી dictionary |
| **Jinja / Template** | `{{ ds }}` જેવું placeholder જે run વખતે value બને |
| **Macro** | Jinja માં વાપરી શકાતા helper (`macros.ds_add`) |
| **Logical date (`ds`)** | Run કઈ તારીખના data માટે છે |
| **Data interval** | Run જે સમયગાળાનો data process કરે |
| **Catchup / Backfill** | જૂની ચૂકી ગયેલી તારીખોના run ચલાવવા |
| **Param** | DAG નો validated input |
| **Variable** | UI માં રાખેલી global key-value setting |
| **Connection** | DB/API ના host, user, password સલામત રીતે |
| **Pool** | એક સાથે કેટલા tasks કોઈ resource વાપરે તેની મર્યાદા |
| **Trigger rule** | Upstream ની સ્થિતિ મુજબ task ક્યારે ચાલે તેનો નિયમ |
| **Branch** | Run વખતે રસ્તો પસંદ કરવો |
| **Short circuit** | શરત ખોટી તો આગળનું બધું skip |
| **Sensor** | શરત પૂરી થાય ત્યાં સુધી રાહ જોતો task |
| **Dynamic Task Mapping** | Run વખતે data મુજબ task ની copies બનાવવી |
| **TaskGroup** | Tasks નો UI box |
| **Setup / Teardown** | Resource બનાવવું / હંમેશા સાફ કરવું |
| **Dataset / Asset** | Data નો logical pointer – data-aware scheduling માટે |
| **Callback** | ઘટના બને ત્યારે આપમેળે ચાલતું function |
| **Retry / Backoff** | ફરી પ્રયત્ન / દરેક વખતે વધુ રાહ |
| **Idempotent** | ગમે તેટલી વાર ચલાવો, પરિણામ સરખું – સારા Airflow task નો ગુણ |

---

> 🎯 **આગળનું પગલું:** આ file ને Airflow ના `dags/` folder માં મૂકો, UI માં દરેક config (section 29) સાથે trigger કરો, અને **Graph view + Logs** જોતાં-જોતાં આ document ફરી વાંચો – દરેક concept હાથોહાથ સમજાઈ જશે.
