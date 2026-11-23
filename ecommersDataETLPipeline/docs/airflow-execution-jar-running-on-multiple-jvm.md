# Airflow: ek saathe ghani jobs, ane darek job nu `exit`

**Prashna:** Airflow ma 3 alag jobs ek j samaye chale che, ane darek ni finish time alag che. Je job pehli puri thay e
`System.exit` kare che. To shu e JVM ke jar ne bandh kari ne biji jobs ne asar karshe?

**Jawab: na.** `exit` fakt **e j job ni potani JVM** bandh kare che. Biji jobs ne koi asar nathi thati.

---

## 1. Darek job = alag process (alag JVM)

Airflow darek task mate **navo process** chalave che:

```bash
java -jar /opt/dmj-dwh/dmj-dwh-1.0.0.jar --spring.profiles.active=prod --run-pipeline=sales-pipeline
```

3 jobs ek saathe chale to worker par **3 alag JVM** bane che. Badhi e j jar file vanche che, pan ek-bija thi
sampurna alag che:

```text
                 Airflow worker (ek machine)
 ┌──────────────────────────────────────────────────────────────────────────┐
 │                                                                          │
 │  JVM 1 (PID 1001)          JVM 2 (PID 1002)          JVM 3 (PID 1003)    │
 │  --run-pipeline=           --run-pipeline=           --run-pipeline=     │
 │    sales-pipeline            payment-pipeline          vat-return-pipeline│
 │  ┌──────────────────┐      ┌──────────────────┐      ┌──────────────────┐│
 │  │ potani memory    │      │ potani memory    │      │ potani memory    ││
 │  │ potanu Spark     │      │ potanu Spark     │      │ potanu Spark     ││
 │  │ potana DB conn.  │      │ potana DB conn.  │      │ potana DB conn.  ││
 │  │ lock: sales      │      │ lock: payment    │      │ lock: vat-return ││
 │  └──────────────────┘      └──────────────────┘      └──────────────────┘│
 │           │                         │                         │          │
 │           └──────── badha FAKT VANCHE che ────────────────────┘          │
 │                     /opt/dmj-dwh/dmj-dwh-1.0.0.jar                       │
 └──────────────────────────────────────────────────────────────────────────┘
```

## 2. Pehli job puri thay tyare shu thay

```text
samay ──────────────────────────────────────────────────────────────────►

JVM 1  sales       ████████████ exit 0  ← fakt JVM 1 bandh
JVM 2  payment     ██████████████████████████ exit 0  ← chalti rahi, pachhi potani rite bandh
JVM 3  vat-return  ████████████████████████████████████████ exit 0
```

JVM 1 nu `System.exit(0)`:

| Shu bandh thay | Shu NATHI thatu |
|---|---|
| JVM 1 ni memory, enu SparkSession, ena DB connections | JVM 2 ane JVM 3 chalti rahe che |
| JVM 1 no pipeline lock (sales) chhuto thay | payment ane vat-return na lock ane connections jem che tem rahe |
| Airflow ne exit code `0` male, sales nu task SUCCESS | jar file ne kai nathi thatu (e fakt vanchay che, lock nathi thati) |

Mukhya vaat: **`System.exit` operating system nu process-level kaam che.** Ek process biji process ni memory ke
threads ne adi pan nathi shakti.

## 3. Kai vastu share NATHI thati

| Vastu | Kem alag che |
|---|---|
| Memory (heap) | darek JVM ni potani `-Xmx` memory |
| Spark | darek JVM potanu `local[*]` SparkSession banave che. Spark UI band che, ane driver port random che. |
| PostgreSQL connections | darek JVM no potano Hikari pool, ane lock mate ek alag connection |
| Pipeline lock | **pipeline dith** PostgreSQL advisory lock: sales no lock payment ne nathi rokto |
| MongoDB | darek JVM fakt vanche che, kai lakhtu nathi |

Fakt **e j pipeline** be jagya e ek saathe chale to bijo process rokay che: exit `1` ane "already running".
Aa rite be process ek j table / watermark ma ek saathe nathi lakhta.

## 4. Kharekhar test karyu che

| Test | Parinam |
|---|---|
| Be JVM ek saathe (same pipeline) | biji JVM exit 1 "already running", pehli JVM sachi puri thai (21 records, exit 0) |
| REST service + Airflow batch ek saathe | batch exit 1, REST ni run ne koi asar nahi |
| Chalti JVM ne kill kari | lock jate chhuto thayo. Aavti run e juni run ne FAILED / INTERRUPTED kari ane potanu kaam karyu |

## 5. Dhyan rakhvanu: CPU ane RAM share thay che

Process alag che, pan **machine ek j che**. 3 JVM ek j worker par chale to:

- **RAM:** darek ne `-Xmx4g` (YAML `java_options`) che, etle 3 jobs = aashare **12 GB+** (heap + Spark + JVM overhead).
  Worker ma etli RAM na hoy to OS koi process ne mari nakhe (OOM). E job FAILED thay, ane aavti run e j data
  fari vanche che.
- **CPU:** `local[*]` = Spark darek job ma **badha cores** vapre che. 3 jobs ek saathe chale to ek-bija ne dhimi kare.

### Upay 1: darek job ne ochha cores aapo

`airflow/dags/dmj_dwh_pipelines.yaml` ma:

```yaml
  sales-pipeline:
    schedule: "0 2 * * *"
    env:
      SPARK_MASTER: "local[2]"        # aa job fakt 2 cores vaparshe
```

### Upay 2: ek saathe ketli jobs chale e limit karo (Airflow pool)

1. Airflow UI → **Admin → Pools** → navo pool `dmj_dwh`, slots = **2** (worker ni RAM / 4 GB mujab).
2. YAML na `defaults` ma comment kadhi nakho:
   ```yaml
   defaults:
     pool: dmj_dwh
   ```
3. Have ek saathe vadhu ma vadhu 2 dmj-dwh jobs chalshe. Treeji job slot khali thay tya sudhi **queue** ma raah
   jue che. E fail nathi thati.

Ek pipeline mate alag pool joie to e pipeline ma `pool: <naam>` lakho.

### Upay 3: memory pipeline pramane

Nani pipeline ne ochhi memory aapo:

```yaml
  payment-pipeline:
    java_options: "-Xms512m -Xmx2g"
```

## 6. Saransh

- Darek Airflow job **alag JVM** che. `exit` fakt potani JVM bandh kare che, ane biji jobs ne koi asar nathi.
- Jar file fakt vanchay che, ane game etli jobs e ek j jar ek saathe chalavi shake.
- Lock **pipeline dith** che: alag pipelines parallel chale, ane same pipeline be var ek saathe nahi chale.
- Ek j machine par **CPU / RAM** share thay che: `SPARK_MASTER`, `java_options` ane Airflow **pool** thi limit karo.
