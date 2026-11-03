# Production Server Setup – Spark Cluster (1 Master + 2 Workers) + Airflow + MongoDB + EDB PAS (ગુજરાતી માર્ગદર્શિકા)

> **આ document શું છે?**
> Local machine પર બનાવેલો Spark cluster હવે **real Linux servers** પર કેવી રીતે લઈ જવો,
> ETL JAR ક્યાં મૂકવો, Airflow તેને કેવી રીતે schedule કરશે, અને **દરેક server વચ્ચે કયા port પર કોણ કોની સાથે વાત કરે છે** –
> આ બધું step-by-step, example સાથે. છેલ્લે **"chain માં ક્યાં gap હતા"** તેનું અલગ section છે.
>
> આ document `sparkETLPipelineDemo` project (Spring Boot 3.5 + Spark 3.5.x + Java 17 + Mongo Spark Connector 10.x) ના આધારે લખાયું છે.
> તમારા real project માં class/property ના નામ અલગ હોય તો એ પ્રમાણે બદલી લેવા.

---

## અનુક્રમણિકા

0. [એક નજરમાં – Final Architecture](#0-એક-નજરમાં--final-architecture)
1. [સૌથી મહત્વનો નિર્ણય – JAR કેવી રીતે ચાલશે?](#1-સૌથી-મહત્વનો-નિર્ણય--jar-કેવી-રીતે-ચાલશે)
2. [Servers, IP અને Role (Example)](#2-servers-ip-અને-role-example)
3. [Ports – કોણ કોની સાથે કયા port પર વાત કરે છે](#3-ports--કોણ-કોની-સાથે-કયા-port-પર-વાત-કરે-છે)
4. [એક Job run નો આખો Flow (Chain)](#4-એક-job-run-નો-આખો-flow-chain)
5. [Phase 0 – શરૂ કરતા પહેલા માહિતી ભેગી કરો](#5-phase-0--શરૂ-કરતા-પહેલા-માહિતી-ભેગી-કરો)
6. [Phase 1 – OS, hosts, firewall (S1, S2, S3)](#6-phase-1--os-hosts-firewall-s1-s2-s3)
7. [Phase 2 – Java + Spark install, Master/Worker ચાલુ કરો](#7-phase-2--java--spark-install-masterworker-ચાલુ-કરો)
8. [Phase 3 – MongoDB અને EDB PAS ને Spark nodes માટે ખોલો](#8-phase-3--mongodb-અને-edb-pas-ને-spark-nodes-માટે-ખોલો)
9. [Phase 4 – Code માં જરૂરી ફેરફાર (Batch Runner + Executor jars)](#9-phase-4--code-માં-જરૂરી-ફેરફાર-batch-runner--executor-jars)
10. [Phase 5 – JAR deploy કરો (ક્યાં અને કેવી રીતે)](#10-phase-5--jar-deploy-કરો-ક્યાં-અને-કેવી-રીતે)
11. [Phase 6 – Airflow server setup](#11-phase-6--airflow-server-setup)
12. [Phase 7 – Airflow DAG: multiple services, multiple jobs, dependency](#12-phase-7--airflow-dag-multiple-services-multiple-jobs-dependency)
13. [Resource sizing – Spark અને MongoDB એક જ server પર](#13-resource-sizing--spark-અને-mongodb-એક-જ-server-પર)
14. [Monitoring, Logs, Cleanup](#14-monitoring-logs-cleanup)
15. [Chain માં જે Gap હતા – અને તેનો ઉકેલ](#15-chain-માં-જે-gap-હતા--અને-તેનો-ઉકેલ)
16. [Troubleshooting](#16-troubleshooting)
17. [Alternative: Spring Boot ને REST service તરીકે રાખવો (Mode B)](#17-alternative-spring-boot-ને-rest-service-તરીકે-રાખવો-mode-b)
18. [Final Checklist](#18-final-checklist)

---

## 0. એક નજરમાં – Final Architecture

```
                         ┌──────────────────────────────┐
     Admin PC  ───────►  │  S4: airflow-server          │
  (browser 8080)         │  Airflow (scheduler, UI,     │
                         │  dag-processor, triggerer)   │
                         │  + Airflow metadata Postgres │
                         └──────────────┬───────────────┘
                                        │  SSH (port 22)  ← Airflow ફક્ત આ એક port થી Spark બાજુ જાય છે
                                        ▼
┌───────────────────────────────────────────────────────────────────────────────────────┐
│ S1: spark-master  (10.10.1.11)                                                        │
│   • Spark Master daemon        (7077 RPC, 8080 UI)                                    │
│   • ETL JAR અહીં deploy થાય   (/opt/etl/...)  → દરેક job = એક Driver JVM             │
│   • MongoDB (27017)                                                                   │
└───────┬───────────────────────────────────────────────────────────────┬───────────────┘
        │ 7077 / 7078 / 7100-7116 / 7200-7216                           │
        ▼                                                               ▼
┌─────────────────────────────┐                         ┌─────────────────────────────┐
│ S2: spark-worker-1          │◄──── 7200-7216 ────────►│ S3: spark-worker-2          │
│  Spark Worker (7078, 8081)  │      (shuffle)          │  Spark Worker (7078, 8081)  │
│  Executors ચાલે છે અહીં    │                         │  Executors ચાલે છે અહીં    │
│  MongoDB (27017)            │                         │  MongoDB (27017)            │
└──────────────┬──────────────┘                         └──────────────┬──────────────┘
               │  Executors સીધા DB સાથે વાત કરે છે (Driver મારફતે નહિ!)     │
               ├──────────── 27017 ──► MongoDB (S1/S2/S3)  ◄────────────┤
               └──────────── 5444  ──► S5: EDB PAS (silver_db) ◄────────┘
```

**ત્રણ વાક્યમાં આખું setup:**

1. **Spark**: S1 = Master + "edge node" (JAR અહીં રહે, Driver અહીં ચાલે), S2/S3 = Workers (Executors અહીં ચાલે).
2. **Airflow**: અલગ server (S4) પરથી **SSH** કરીને S1 પર `run-etl.sh <service> <pipeline> <loadType>` ચલાવે છે. Script નો **exit code** = Airflow task નું SUCCESS/FAILED.
3. **Data**: Executors (S2/S3) **સીધા** MongoDB (27017) માંથી વાંચે છે અને **સીધા** EDB PAS (5444) માં લખે છે. એટલે DB access ફક્ત S1 ને નહિ, **ત્રણેય Spark servers ને** જોઈએ.

---

## 1. સૌથી મહત્વનો નિર્ણય – JAR કેવી રીતે ચાલશે?

તમારો project **Spring Boot app છે જેની અંદર SparkSession બને છે** (`SparkConfig.java`).
Local પર તે `master: local[*]` સાથે ચાલે છે અને REST API (`POST /api/etl/pipelines/{name}/run`) થી job trigger થાય છે.

Cluster પર લઈ જવા માટે 3 રસ્તા છે:

| રસ્તો | કેવી રીતે | ફાયદો | તકલીફ |
|---|---|---|---|
| **A. Batch mode via SSH (✅ Recommended)** | Airflow → SSH → S1 પર `java -jar app.jar --pipeline=X` → job પૂરો → JVM exit | દરેક job અલગ Spark application, resources job પૂરો થતા છૂટા, exit code સીધો Airflow ને મળે, Airflow server પર Spark/Java install નહિ કરવું પડે, ફક્ત **port 22** | Code માં નાનો "Batch Runner" class ઉમેરવો પડે (section 9) |
| B. REST service mode | Spring Boot app S1 પર હંમેશા ચાલુ, Airflow HTTP call કરે + status poll કરે | Existing REST API એમ જ વપરાય | SparkSession હંમેશા cluster ના cores પકડી રાખે, app restart = ચાલુ jobs ગુમ, polling logic લખવું પડે (section 17) |
| C. `spark-submit` / SparkSubmitOperator | Airflow server પરથી spark-submit | Standard Spark રસ્તો | Spring Boot fat jar (`BOOT-INF/`) spark-submit સાથે સારી રીતે ચાલતી નથી (classpath conflict), Airflow server પર Spark + Java install કરવું પડે, client mode માં Workers ને Airflow server સુધી ports ખોલવા પડે |

**આ document Mode A પ્રમાણે છે.** Mode B નો ટૂંકો setup section 17 માં છે.

### `spark-submit` કેમ નહિ? (ટૂંકમાં સમજ)

- `spark-submit` પોતાના `$SPARK_HOME/jars` (Jackson 2.15, વગેરે) classpath પર પહેલા મૂકે છે. Spring Boot 3.5 ને નવા Jackson જોઈએ → `NoSuchMethodError` જેવી તકલીફો.
- Spring Boot fat jar માં તમારા classes `BOOT-INF/classes/` માં હોય છે, Spark executors ત્યાંથી class load કરી શકતા નથી.
- **`java -jar` થી ચલાવવું પૂરતું છે**: Driver = તમારી Spring Boot JVM, તે `spark://10.10.1.11:7077` સાથે connect થાય છે – આ પણ સાચો "client mode" છે. ફક્ત executors ને જોઈતા jars આપણે `spark.jars` થી અલગ આપીશું (section 9.2).

---

## 2. Servers, IP અને Role (Example)

> નીચેના IP **ફક્ત example** છે. તમારા real IP મૂકી દેજો. આખા document માં આ જ IP વપર્યા છે.

| Server | Hostname | IP (example) | શું ચાલશે |
|---|---|---|---|
| **S1** | `spark-master` | `10.10.1.11` | Spark Master, ETL JAR + Driver JVMs (edge node), MongoDB, (optional) Spark History Server |
| **S2** | `spark-worker-1` | `10.10.1.12` | Spark Worker → Executors, MongoDB |
| **S3** | `spark-worker-2` | `10.10.1.13` | Spark Worker → Executors, MongoDB |
| **S4** | `airflow-server` | `10.10.1.20` | Airflow 3.x (LocalExecutor) + Airflow નો પોતાનો metadata PostgreSQL |
| **S5** | `edb-server` | `10.10.1.30` | EDB Postgres Advanced Server (PAS) – `silver_db`, port **5444** (EDB default; તમારે 5432 હોય તો એ) |
| Admin PC | – | `10.10.2.50` | તમારું laptop: SSH, Spark UI, Airflow UI જોવા |

> **નોંધ – EDB PAS ક્યાં છે?** તમે કહ્યું નથી કે EDB કયા server પર છે. અહીં અલગ server (S5) માન્યું છે.
> જો EDB પણ S1/S2/S3 માંથી કોઈ એક પર હોય, તો ports એ જ રહેશે, ફક્ત IP બદલાશે અને RAM sizing (section 13) ફરી ગણવી પડશે.

### Master પર Worker કેમ નથી રાખ્યો?

S1 પર પહેલેથી MongoDB + Spark Master + દરેક job નો Driver JVM ચાલશે. ત્યાં Worker પણ મૂકો તો RAM ની ખેંચ પડશે.
પાછળથી RAM વધારે હોય તો S1 પર પણ worker ચાલુ કરી શકાય (section 7.6).

---

## 3. Ports – કોણ કોની સાથે કયા port પર વાત કરે છે

### 3.1 Spark ના ports – પહેલા સમજો કે કયું port શેના માટે

Spark by default ઘણા ports **random** રાખે છે. Limited-port environment માં random ports ચાલે નહિ, એટલે આપણે **બધા fix** કરીએ છીએ:

| Port | Setting | કોણ listen કરે | કોણ connect કરે | શા માટે |
|---|---|---|---|---|
| **7077** | `SPARK_MASTER_PORT` | Master (S1) | Workers (S2,S3), Driver (S1) | Worker register થાય, Driver application register કરે |
| **8080** | `SPARK_MASTER_WEBUI_PORT` | Master (S1) | Admin PC | Master UI – કેટલા workers alive, કઈ apps ચાલે છે |
| **7078** | `SPARK_WORKER_PORT` | Worker (S2,S3) | Master (S1) | Master worker ને કહે "executor શરૂ કર" |
| **8081** | `SPARK_WORKER_WEBUI_PORT` | Worker (S2,S3) | Admin PC | Worker UI – **executor ના stdout/stderr logs** અહીંથી દેખાય |
| **7100–7116** | `spark.driver.port` (+ `spark.port.maxRetries=16`) | Driver JVM (S1) | Executors (S2,S3), Master | Executors driver પાસેથી tasks લે, results મોકલે, **jars download કરે** |
| **7200–7216** | `spark.blockManager.port` | Driver (S1) **અને** દરેક Executor (S2,S3) | એકબીજા સાથે | Shuffle data, broadcast, cached blocks ની આપ-લે |
| 18080 | History Server (optional) | S1 | Admin PC | પૂરા થયેલા jobs ની history |
| 4040 | Driver UI | – | – | આ project માં `spark.ui.enabled=false` છે, એટલે **જરૂર નથી** |

**Range (7100–7116) કેમ?** S1 પર એક સાથે 3 jobs ચાલે તો 3 Driver JVM હશે. પહેલો 7100 લેશે, બીજો 7101, ત્રીજો 7102...
Spark port busy હોય તો આપોઆપ +1 try કરે છે (`spark.port.maxRetries` સુધી). એ જ રીતે એક worker પર 2 executors હોય તો 7200, 7201.

### 3.2 આખી Port Matrix (Network/Firewall team ને આ table આપી દો)

| # | Source | → Destination | Port (TCP) | કારણ |
|---|---|---|---|---|
| 1 | S2, S3 | S1 | **7077** | Worker → Master registration |
| 2 | S1 | S2, S3 | **7078** | Master → Worker (executor launch) |
| 3 | S2, S3 | S1 | **7100–7116** | Executor → Driver RPC + jar download |
| 4 | S1, S2, S3 | S1, S2, S3 (બધા એકબીજાને) | **7200–7216** | Block manager / shuffle |
| 5 | S1, S2, S3 | S1, S2, S3 (બધા એકબીજાને) | **27017** | Spark (driver+executors) → MongoDB; replica set members વચ્ચે replication |
| 6 | S1, S2, S3 | S5 | **5444** | Driver (Flyway, audit, merge SQL) + Executors (JDBC write) → EDB PAS |
| 7 | S4 (Airflow) | S1 | **22** | Airflow SSHOperator → `run-etl.sh` |
| 8 | Admin PC | S4 | **8080** | Airflow UI |
| 9 | Admin PC | S1 | **8080** (+ 18080 optional) | Spark Master UI (+ History) |
| 10 | Admin PC | S2, S3 | **8081** | Worker UI / executor logs |
| 11 | Admin PC / CI | S1–S5 | **22** | SSH, JAR deploy (scp) |
| 12 | S4 | SMTP server | 25/587 (optional) | Airflow failure email |

**ધ્યાન આપો – કયા ports જરૂરી નથી:**

- ❌ Airflow (S4) → Spark ports (7077 વગેરે) – **નહિ જોઈએ**. Airflow ફક્ત SSH કરે છે.
- ❌ Airflow (S4) → MongoDB / EDB – **નહિ જોઈએ** (optional: EDB audit table પરથી SQL check કરવો હોય તો 5444).
- ❌ Internet પરથી 7077 / 8080 – **ક્યારેય ખોલવા નહિ**. Spark standalone માં default authentication નથી; 7077 સુધી પહોંચી શકે એ કોઈ પણ code ચલાવી શકે.

> **સૌથી સરળ અને સલામત rule**: જો security team મંજૂરી આપે તો **S1, S2, S3 વચ્ચે બધા TCP ports ખુલ્લા** (trusted cluster subnet) અને બહારથી ફક્ત 22/8080/8081.
> મંજૂરી ન મળે તો ઉપરની matrix exact ports સાથે વાપરો.

---

## 4. એક Job run નો આખો Flow (Chain)

ઉદાહરણ: રોજ રાત્રે 02:00 વાગ્યે `payment-service` નો `payment-pipeline` INCREMENTAL load.

```
 ①  S4 Airflow Scheduler: 02:00 થયા → DAG run બનાવે → task "payment_pipeline" queue કરે
 ②  S4 LocalExecutor: SSHOperator ચલાવે
        ssh spark@10.10.1.11  "/opt/etl/bin/run-etl.sh payment-service payment-pipeline INCREMENTAL <run_id>"
                                                                                    (S4 → S1 : 22)
 ③  S1 run-etl.sh: secrets.env load કરે → `java -jar /opt/etl/apps/payment-service/current/app.jar --etl.runner.enabled=true ...`
 ④  S1 Driver JVM (Spring Boot) શરૂ થાય:
        • Flyway → EDB PAS માં tables/migrations check                         (S1 → S5 : 5444)
        • SparkSession → master spark://10.10.1.11:7077 સાથે register          (S1 → S1 : 7077)
 ⑤  S1 Master: "payment-pipeline app ને 4 cores, 3g/executor જોઈએ" →
        Worker S2 અને S3 ને executor launch કરવાનું કહે                       (S1 → S2,S3 : 7078)
 ⑥  S2/S3 Workers: Executor JVM શરૂ કરે → Executor Driver સાથે connect થાય   (S2,S3 → S1 : 7100)
        • Executor driver પાસેથી spark.jars download કરે
          (app-classes.jar, mongo-spark-connector, mongodb-driver, postgresql)  (S2,S3 → S1 : 7100)
 ⑦  Driver: etl_job_run row બનાવે (status RUNNING)                             (S1 → S5 : 5444)
 ⑧  Executors: MongoDB માંથી processedFlag = 'N' વાળા documents વાંચે        (S2,S3 → Mongo : 27017)
 ⑨  Executors: validate / clean / dedupe / join → shuffle                       (S2 ↔ S3 ↔ S1 : 7200-7216)
 ⑩  Executors: staging tables માં JDBC batch write                              (S2,S3 → S5 : 5444)
 ⑪  Driver: staging → final star-schema tables માં UPSERT (SQL)                (S1 → S5 : 5444)
 ⑫  Executors: MongoDB માં processedFlag = 'Y' / 'E' update (foreachPartition)  (S2,S3 → Mongo : 27017)
 ⑬  Driver: etl_job_run row = SUCCESS / SUCCESS_WITH_REJECTS / FAILED + counts (S1 → S5 : 5444)
 ⑭  Driver: SparkSession.stop() → Master executors બંધ કરાવે → cores ફરી free
 ⑮  JVM exit code:  0 = SUCCESS / SUCCESS_WITH_REJECTS,  1 = FAILED,  2 = crash/config error
 ⑯  SSH session exit code → Airflow:  0 = task success → downstream tasks શરૂ
                                       ≠0 = task failed → retry (2 વાર) → પછી alert
```

**આ chain માંથી એક પણ કડી તૂટે તો શું દેખાશે** – તેની વિગત section 16 (Troubleshooting) માં છે.

---

## 5. Phase 0 – શરૂ કરતા પહેલા માહિતી ભેગી કરો

Setup શરૂ કરતા પહેલા આ જવાબો તૈયાર રાખો (ઘણી વાર setup અહીં જ અટકે છે):

| પ્રશ્ન | કેવી રીતે ચકાસવું | કેમ જરૂરી |
|---|---|---|
| OS કયું છે? (Ubuntu / RHEL / Rocky) | `cat /etc/os-release` | Package/firewall commands અલગ |
| દરેક server ના CPU cores, RAM, disk | `nproc`, `free -g`, `df -h` | Worker cores/memory sizing |
| MongoDB **replica set** છે કે 3 અલગ standalone? | `mongosh --eval "rs.status()"` | Mongo URI કેવી બનશે |
| Replica set ના member hostnames શું છે? | `mongosh --eval "rs.conf().members.map(m=>m.host)"` | Executors ને આ નામ resolve થવા જોઈએ |
| Mongo authentication ચાલુ છે? | `/etc/mongod.conf` માં `security.authorization` | ETL user બનાવવો પડશે |
| Data કયા database/collections માં છે? | `show dbs` | `MONGO_DATABASE` |
| EDB PAS નું host, port, version, database | EDB DBA પાસેથી | JDBC URL, pg_hba entry |
| Servers ને internet access છે? | `curl -I https://archive.apache.org` | નથી → laptop પર download કરી `scp` |
| Project નું Spark version | `pom.xml` માં `<spark.version>` (હાલ `3.5.9`) | **Cluster માં exact આ જ version install કરવું** |
| Java version | `pom.xml` માં `<java.version>` (17) | બધા servers પર Java 17 |

---

## 6. Phase 1 – OS, hosts, firewall (S1, S2, S3)

### 6.1 `/etc/hosts` – પાંચેય servers પર એક સરખું

```bash
sudo tee -a /etc/hosts <<'EOF'
10.10.1.11  spark-master
10.10.1.12  spark-worker-1
10.10.1.13  spark-worker-2
10.10.1.20  airflow-server
10.10.1.30  edb-server
EOF
```

> ⚠️ **Ubuntu trap**: Ubuntu default `/etc/hosts` માં `127.0.1.1  spark-worker-1` જેવી line હોય છે.
> તેના કારણે Worker master ને પોતાનું address `127.0.1.1` કહે છે અને પછી executors connect થઈ શકતા નથી.
> ઉપાય: એ line comment કરો **અથવા** `spark-env.sh` માં `SPARK_LOCAL_IP` set કરો (section 7.3 માં કર્યું છે).

જો MongoDB replica set ના members hostnames થી configure થયા હોય (દા.ત. `mongo1:27017`), તો **તે નામ પણ** ત્રણેય Spark servers ના `/etc/hosts` માં ઉમેરો.

### 6.2 Time sync

```bash
# Ubuntu
sudo apt install -y chrony && sudo systemctl enable --now chrony
# RHEL/Rocky
sudo dnf install -y chrony && sudo systemctl enable --now chronyd
```
બધા servers નો સમય સરખો નહિ હોય તો Airflow schedule, logs, અને Mongo replication માં ગૂંચવણ થાય.

### 6.3 OS user

```bash
# S1, S2, S3 પર
sudo useradd -m -s /bin/bash spark
sudo mkdir -p /opt/etl /data/spark/{work,tmp} /var/log/spark /opt/spark-events
sudo chown -R spark:spark /opt/etl /data/spark /var/log/spark /opt/spark-events
```
સરળતા માટે Spark daemons અને ETL jobs બંને `spark` user થી ચાલશે. (Security માટે અલગ `etl` user રાખી શકાય, પણ ત્યારે directory permissions ખાસ ધ્યાનથી આપવા.)

### 6.4 Firewall – Ubuntu (`ufw`) example

**S1 (spark-master) પર:**
```bash
# Workers થી
for ip in 10.10.1.12 10.10.1.13; do
  sudo ufw allow from $ip to any port 7077       proto tcp   # master RPC
  sudo ufw allow from $ip to any port 7100:7116  proto tcp   # driver RPC
  sudo ufw allow from $ip to any port 7200:7216  proto tcp   # block manager
  sudo ufw allow from $ip to any port 27017      proto tcp   # mongo
done
sudo ufw allow from 10.10.1.20 to any port 22    proto tcp   # Airflow SSH
sudo ufw allow from 10.10.2.50 to any port 22    proto tcp   # Admin SSH
sudo ufw allow from 10.10.2.50 to any port 8080  proto tcp   # Spark Master UI
sudo ufw allow from 10.10.2.50 to any port 18080 proto tcp   # History (optional)
sudo ufw enable
```

**S2 અને S3 (workers) પર:**
```bash
sudo ufw allow from 10.10.1.11 to any port 7078 proto tcp          # master → worker
for ip in 10.10.1.11 10.10.1.12 10.10.1.13; do
  sudo ufw allow from $ip to any port 7200:7216 proto tcp          # shuffle
  sudo ufw allow from $ip to any port 27017     proto tcp          # mongo
done
sudo ufw allow from 10.10.2.50 to any port 22   proto tcp
sudo ufw allow from 10.10.2.50 to any port 8081 proto tcp          # worker UI / logs
sudo ufw enable
```
(Server પોતાને localhost થી connect કરે તે firewall માં block થતું નથી.)

### 6.5 Firewall – RHEL/Rocky (`firewalld`) example (S1)

```bash
sudo firewall-cmd --permanent --new-zone=sparkcluster
sudo firewall-cmd --permanent --zone=sparkcluster --add-source=10.10.1.12
sudo firewall-cmd --permanent --zone=sparkcluster --add-source=10.10.1.13
for p in 7077/tcp 7100-7116/tcp 7200-7216/tcp 27017/tcp; do
  sudo firewall-cmd --permanent --zone=sparkcluster --add-port=$p
done
sudo firewall-cmd --permanent --new-zone=airflow
sudo firewall-cmd --permanent --zone=airflow --add-source=10.10.1.20
sudo firewall-cmd --permanent --zone=airflow --add-service=ssh
sudo firewall-cmd --reload
```
S2/S3 માટે એ જ pattern, ports section 3.2 પ્રમાણે.

### 6.6 Firewall ચકાસો – listener વગર port test કેમ કરવું?

Driver ports (7100…) પર job ચાલતો ન હોય ત્યારે કોઈ listen કરતું નથી, એટલે `nc` fail થશે ભલે firewall ખુલ્લું હોય.
ચકાસવા માટે થોડી વાર માટે dummy listener ચાલુ કરો:

```bash
# S1 પર (terminal 1)
nc -l 7100
# S2 પર (terminal 2)
nc -zv 10.10.1.11 7100     # "succeeded" / "Connected" આવવું જોઈએ
```

---

## 7. Phase 2 – Java + Spark install, Master/Worker ચાલુ કરો

### 7.1 Java 17 (S1, S2, S3)

```bash
# Ubuntu
sudo apt update && sudo apt install -y openjdk-17-jdk-headless
# RHEL/Rocky
sudo dnf install -y java-17-openjdk-headless
java -version          # 17.x દેખાવું જોઈએ
```
> Airflow server (S4) પર Java/Spark **જરૂર નથી** (SSH mode).

### 7.2 Spark install (S1, S2, S3 – ત્રણેય પર એક સરખું)

```bash
SPARK_VERSION=3.5.9          # ⚠️ pom.xml ના <spark.version> જેટલું જ
cd /opt
sudo wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz
sudo tar -xzf spark-${SPARK_VERSION}-bin-hadoop3.tgz
sudo ln -sfn /opt/spark-${SPARK_VERSION}-bin-hadoop3 /opt/spark
sudo chown -R spark:spark /opt/spark-${SPARK_VERSION}-bin-hadoop3
```
Internet ન હોય તો: laptop પર download → `scp spark-3.5.9-bin-hadoop3.tgz spark@10.10.1.11:/tmp/` → ત્યાંથી extract.

> **Version કેમ match કરવું?** Driver ના Spark jars તમારી fat jar માં (3.5.9) છે અને Executors worker ના `/opt/spark/jars` (install કરેલું version) વાપરે છે.
> બંને અલગ હોય તો `InvalidClassException ... serialVersionUID` જેવી errors આવે. Scala version પણ 2.12 જ (default Spark 3.5 build 2.12 છે).

### 7.3 `spark-env.sh`

**S1 – `/opt/spark/conf/spark-env.sh`**
```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64      # RHEL: /usr/lib/jvm/jre-17
export SPARK_LOCAL_IP=10.10.1.11
export SPARK_MASTER_HOST=10.10.1.11
export SPARK_MASTER_PORT=7077
export SPARK_MASTER_WEBUI_PORT=8080
export SPARK_DAEMON_MEMORY=1g
export SPARK_LOG_DIR=/var/log/spark
```

**S2 – `/opt/spark/conf/spark-env.sh`** (S3 પર ફક્ત IP `10.10.1.13`)
```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export SPARK_LOCAL_IP=10.10.1.12
export SPARK_WORKER_PORT=7078
export SPARK_WORKER_WEBUI_PORT=8081
export SPARK_WORKER_CORES=6            # section 13 પ્રમાણે
export SPARK_WORKER_MEMORY=8g          # section 13 પ્રમાણે (MongoDB માટે RAM છોડો!)
export SPARK_WORKER_DIR=/data/spark/work
export SPARK_LOCAL_DIRS=/data/spark/tmp          # shuffle spill
export SPARK_LOG_DIR=/var/log/spark
export SPARK_DAEMON_MEMORY=1g
# પૂરી થયેલી apps ના jars/logs 3 દિવસ પછી આપોઆપ delete (નહીં તો disk ભરાઈ જશે)
export SPARK_WORKER_OPTS="-Dspark.worker.cleanup.enabled=true -Dspark.worker.cleanup.interval=1800 -Dspark.worker.cleanup.appDataTtl=259200"
```

```bash
sudo chown spark:spark /opt/spark/conf/spark-env.sh && sudo chmod +x /opt/spark/conf/spark-env.sh
```

### 7.4 systemd service (reboot પછી આપોઆપ ચાલુ)

**S1 – `/etc/systemd/system/spark-master.service`**
```ini
[Unit]
Description=Apache Spark Master
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=spark
Environment=SPARK_NO_DAEMONIZE=true
ExecStart=/opt/spark/sbin/start-master.sh
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
```

**S2, S3 – `/etc/systemd/system/spark-worker.service`**
```ini
[Unit]
Description=Apache Spark Worker
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=spark
Environment=SPARK_NO_DAEMONIZE=true
ExecStart=/opt/spark/sbin/start-worker.sh spark://10.10.1.11:7077
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now spark-master      # S1
sudo systemctl enable --now spark-worker      # S2, S3
sudo systemctl status spark-worker
```

> `SPARK_NO_DAEMONIZE=true` થી script foreground માં ચાલે છે, એટલે systemd process ને track/restart કરી શકે.
> આ રીતે **master થી workers પર passwordless SSH ની જરૂર નથી** (`start-all.sh` વાપરતા નથી).

### 7.5 ચકાસો

Browser: `http://10.10.1.11:8080`
- URL: `spark://10.10.1.11:7077`
- **Alive Workers: 2**
- દરેક worker નું address `10.10.1.12:7078` / `10.10.1.13:7078` હોવું જોઈએ (**`127.0.1.1` નહિ**)
- Cores: 12 Total, Memory: 16 GiB Total

### 7.6 (Optional) S1 પર પણ worker

RAM પૂરતી હોય તો S1 પર પણ `spark-worker.service` (SPARK_LOCAL_IP=10.10.1.11, ઓછા cores/memory) ચાલુ કરી શકાય.
ત્યારે S2/S3 → S1 : 7078 પણ ખોલવું પડશે અને port matrix #2 માં S1 ઉમેરવું.

---

## 8. Phase 3 – MongoDB અને EDB PAS ને Spark nodes માટે ખોલો

### 8.1 MongoDB – `/etc/mongod.conf` (S1, S2, S3)

```yaml
net:
  port: 27017
  bindIp: 127.0.0.1,10.10.1.11      # ⚠️ ફક્ત 127.0.0.1 હશે તો બીજા servers connect નહિ થઈ શકે (S2 પર 10.10.1.12 ...)

storage:
  wiredTiger:
    engineConfig:
      cacheSizeGB: 4                 # ⚠️ Spark સાથે RAM share કરે છે – section 13

replication:
  replSetName: rs0                   # replica set હોય તો

security:
  authorization: enabled
  keyFile: /etc/mongo/keyfile        # replica set + auth હોય ત્યારે
```
```bash
sudo systemctl restart mongod
```

### 8.2 ETL માટે MongoDB user

ETL **ફક્ત વાંચતો નથી** – job પૂરો થતા `processedFlag` (`N`→`Y`/`E`/`D`) પાછો MongoDB માં **લખે છે**. એટલે `readWrite` જોઈએ:

```javascript
// mongosh – primary પર
use admin
db.createUser({
  user: "etl_user",
  pwd:  "Strong#Pass123",
  roles: [ { role: "readWrite", db: "bronze_db" } ]
})
```

### 8.3 MongoDB Connection URI – સૌથી વધુ ભૂલ અહીં થાય છે

**Replica set હોય તો:**
```
mongodb://etl_user:Strong%23Pass123@10.10.1.11:27017,10.10.1.12:27017,10.10.1.13:27017/?replicaSet=rs0&authSource=admin
```
- Password માં special character હોય તો URL-encode (`#` → `%23`, `@` → `%40`).
- Mongo primary પર load ઓછો કરવો હોય તો `&readPreference=secondaryPreferred` ઉમેરી શકાય (flag updates તો હંમેશા primary પર જ જશે).

**3 અલગ standalone MongoDB હોય તો:** જે server પર data છે તેનો જ IP:
```
mongodb://etl_user:Strong%23Pass123@10.10.1.11:27017/?authSource=admin
```

> ⚠️ **`localhost` ક્યારેય નહિ!** URI Driver (S1) થી executors (S2/S3) સુધી એમ જ જાય છે.
> `mongodb://localhost:27017` લખશો તો S2 નો executor **S2 ના પોતાના MongoDB** સાથે connect થશે –
> Replica set ન હોય તો **error વગર ખોટો/ખાલી data** વંચાશે. આ સૌથી ખતરનાક silent bug છે.

ચકાસો (S2 પરથી):
```bash
mongosh "mongodb://etl_user:...@10.10.1.11:27017,10.10.1.12:27017,10.10.1.13:27017/?replicaSet=rs0&authSource=admin" --eval "db.getSiblingDB('bronze_db').getCollectionNames()"
```

### 8.4 EDB PAS – DB, user, remote access (S5)

```sql
-- psql -p 5444 -U enterprisedb  (EDB superuser)
CREATE USER etl_user WITH PASSWORD 'Strong#Pass456';
CREATE DATABASE silver_db OWNER etl_user;
```
Owner હોવાથી Flyway `etl_audit`, `etl_staging` અને star-schema tables બનાવી શકશે.

**`postgresql.conf`** (path દા.ત. `/var/lib/edb/as16/data/` – તમારા version પ્રમાણે):
```
listen_addresses = '*'
port = 5444
max_connections = 200      # executors × parallel JDBC partitions + driver pool (10) + બીજી apps
```

**`pg_hba.conf`** – ⚠️ **ત્રણેય** Spark servers ઉમેરો (ફક્ત S1 નહિ, કારણ કે executors S2/S3 પરથી લખે છે):
```
host  silver_db  etl_user  10.10.1.11/32  scram-sha-256
host  silver_db  etl_user  10.10.1.12/32  scram-sha-256
host  silver_db  etl_user  10.10.1.13/32  scram-sha-256
```
```bash
sudo systemctl restart edb-as-16      # service name version પ્રમાણે
```

ચકાસો (S2 પરથી):
```bash
nc -zv 10.10.1.30 5444
```

**JDBC URL:**
```
jdbc:postgresql://10.10.1.30:5444/silver_db
```
> EDB PAS PostgreSQL-compatible છે, એટલે project નો `org.postgresql` JDBC driver અને Flyway PostgreSQL module ચાલે છે.
> એક વાર UAT માં Flyway migrations EDB પર ચલાવી ચકાસી લેવું.

---

## 9. Phase 4 – Code માં જરૂરી ફેરફાર (Batch Runner + Executor jars)

હાલનો project ફક્ત REST API થી trigger થાય છે અને `local[*]` પર ચાલે છે. Mode A માટે 2 વસ્તુ જોઈએ:

### 9.1 Batch Runner – command line થી pipeline ચલાવો અને exit code આપો

**નવો class:** `src/main/java/com/demo/etl/runner/EtlBatchRunner.java`

```java
package com.demo.etl.runner;

import com.demo.etl.framework.audit.JobStatus;
import com.demo.etl.framework.job.EtlJobService;
import com.demo.etl.framework.job.JobRunDetails;
import com.demo.etl.framework.pipeline.LoadType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.ExitCodeGenerator;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * Batch mode: run ONE pipeline from the command line and exit (used by Airflow).
 *
 *   java -jar app.jar --etl.runner.enabled=true --pipeline=payment-pipeline --loadType=INCREMENTAL --triggeredBy=airflow
 *
 * Exit codes: 0 = SUCCESS / SUCCESS_WITH_REJECTS, 1 = FAILED, 2 = bad arguments or crash.
 */
@Component
@ConditionalOnProperty(name = "etl.runner.enabled", havingValue = "true")
public class EtlBatchRunner implements ApplicationRunner, ExitCodeGenerator {

    private static final Logger log = LoggerFactory.getLogger(EtlBatchRunner.class);

    private final EtlJobService jobService;
    private int exitCode = 2;

    public EtlBatchRunner(EtlJobService jobService) {
        this.jobService = jobService;
    }

    @Override
    public void run(ApplicationArguments args) {
        try {
            String pipeline = requiredOption(args, "pipeline");
            LoadType loadType = LoadType.from(option(args, "loadType", "INCREMENTAL"));
            String triggeredBy = option(args, "triggeredBy", "batch");

            JobRunDetails result = jobService.runPipeline(pipeline, loadType, false, triggeredBy);
            String status = result.job().status();
            log.info("Pipeline {} finished: jobRunId={}, status={}, loaded={}, rejected={}",
                    pipeline, result.job().jobRunId(), status, result.job().loadedCount(), result.job().rejectedCount());

            exitCode = JobStatus.FAILED.name().equals(status) ? 1 : 0;
        } catch (Exception e) {
            log.error("Batch run failed", e);
            exitCode = 2;
        }
    }

    @Override
    public int getExitCode() {
        return exitCode;
    }

    private static String requiredOption(ApplicationArguments args, String name) {
        String value = option(args, name, null);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Missing --" + name + "=...");
        }
        return value;
    }

    private static String option(ApplicationArguments args, String name, String defaultValue) {
        return args.containsOption(name) ? args.getOptionValues(name).get(0) : defaultValue;
    }
}
```

**`SparkEtlApplication.java` માં main બદલો** – batch mode માં job પછી JVM બંધ થાય અને exit code બહાર જાય:

```java
public static void main(String[] args) {
    ConfigurableApplicationContext context = SpringApplication.run(SparkEtlApplication.class, args);
    if (context.getEnvironment().getProperty("etl.runner.enabled", Boolean.class, false)) {
        // Batch mode (Airflow): close Spring + SparkSession and return the job result as the process exit code
        System.exit(SpringApplication.exit(context));
    }
}
```

> **નોંધ – `JobLockManager` batch mode માં કામ નહિ કરે.** તે memory માં lock રાખે છે; દરેક job અલગ JVM હોવાથી
> બે JVM એકબીજાના lock જોઈ શકતા નથી. "એક pipeline એક સમયે એક જ વાર" ની ખાતરી **Airflow** થી કરીએ છીએ
> (`max_active_runs=1` + pool, section 12). વધારે સલામતી જોઈએ તો runner શરૂઆતમાં EDB ના `etl_audit.etl_job_run` માં
> એ pipeline નો `RUNNING` row છે કે નહિ તે check કરે (અથવા ShedLock જેવો DB lock).

### 9.2 Executors ને જોઈતા jars – "chain નો સૌથી મોટો gap"

Executors (S2/S3) પાસે ફક્ત `/opt/spark/jars` (Spark core) છે. તેમને આ પણ જોઈએ:

| Executor ને શું જોઈએ | કેમ |
|---|---|
| `mongo-spark-connector_2.12` + `mongodb-driver-sync`, `mongodb-driver-core`, `bson`, ... | Executors MongoDB partitions વાંચે છે |
| `postgresql` JDBC driver | Executors EDB માં JDBC write કરે છે |
| **તમારા project ના classes** | `MongoFlagUpdater.FlagUpdateTask` જેવો code `foreachPartition` માં **executor પર** ચાલે છે |

Spring Boot fat jar (`BOOT-INF/...`) executors વાંચી શકતા નથી, એટલે build પછી આ jars અલગ folder માં કાઢો:

```bash
# Laptop / CI પર
mvn clean package -DskipTests

# 1) Executor માટે dependency jars
mvn dependency:copy-dependencies \
    -DincludeScope=runtime \
    -DincludeGroupIds=org.mongodb,org.mongodb.spark,org.postgresql \
    -DoutputDirectory=target/executor-libs

# 2) તમારા classes નો plain jar (Spring Boot repackage પહેલાની original jar)
cp target/spark-etl-pipeline-1.0.0.jar.original target/executor-libs/app-classes.jar

ls target/executor-libs
# app-classes.jar  bson-*.jar  bson-record-codec-*.jar  mongodb-driver-core-*.jar
# mongodb-driver-sync-*.jar  mongo-spark-connector_2.12-10.7.0.jar  postgresql-*.jar
```

પછી config માં (section 10.3):
```yaml
"[spark.jars]": "/opt/etl/apps/payment-service/current/executor-libs/*.jar"
```

**આ કેવી રીતે કામ કરે છે?** `spark.jars` માં S1 નો local path છે. Driver (S1) પોતાના file server (port 7100) પરથી આ jars serve કરે છે,
અને દરેક executor શરૂ થતાં જ download કરી લે છે. **એટલે workers (S2/S3) પર JAR copy કરવાની જરૂર જ નથી** – deploy ફક્ત S1 પર.

### 9.3 Spark settings – `spark-submit` વાપરતા નથી એટલે ધ્યાન રાખો

| વાત | સમજ |
|---|---|
| `spark-defaults.conf` | `java -jar` mode માં **વંચાતી નથી**. બધી `spark.*` settings `application-prod.yml` ના `etl.spark.config` માં મૂકો |
| `spark.driver.memory` | **અસર નહિ કરે** (JVM પહેલેથી શરૂ). Driver memory `java -Xmx2g` થી આપો (`run-etl.sh` માં) |
| Driver UI (4040) | Project માં `spark.ui.enabled=false` છે (javax/jakarta servlet conflict). History Server થી જોઈ શકાય (section 14) |
| Java 17 `--add-opens` | Driver માટે jar ના MANIFEST માં પહેલેથી છે; Executors માટે Spark 3.3+ પોતે ઉમેરે છે |

---

## 10. Phase 5 – JAR deploy કરો (ક્યાં અને કેવી રીતે)

### 10.1 જવાબ: JAR **ફક્ત S1 (spark-master / edge node)** પર

| ક્યાં | શું | કેમ |
|---|---|---|
| **S1** `/opt/etl/apps/<service>/releases/<version>/` | `app.jar` (fat jar) + `executor-libs/` | Driver અહીં ચાલે છે; executors jars driver પાસેથી લે છે |
| S2, S3 | **કંઈ નહિ** | `spark.jars` આપોઆપ મોકલે છે |
| S4 Airflow | **કંઈ નહિ** (ફક્ત DAG `.py` files) | Airflow ફક્ત SSH command મોકલે છે |

### 10.2 Folder structure (S1) – multiple services માટે

તમારે અનેક services (અલગ-અલગ JAR) છે, એટલે દરેક service નું અલગ folder:

```
/opt/etl/
├── bin/
│   └── run-etl.sh                        ← એક જ script, બધી services માટે
├── apps/
│   ├── payment-service/
│   │   ├── releases/
│   │   │   ├── 1.0.0/
│   │   │   │   ├── app.jar
│   │   │   │   └── executor-libs/*.jar
│   │   │   └── 1.0.1/ ...
│   │   └── current -> releases/1.0.1     ← symlink (rollback = symlink બદલો)
│   └── customer-service/
│       ├── releases/...
│       └── current -> releases/2.3.0
├── config/
│   ├── payment-service/
│   │   ├── application-prod.yml          ← cluster + spark settings (secret નહિ)
│   │   └── secrets.env                   ← passwords (chmod 600)
│   └── customer-service/ ...
└── logs/
    └── payment-service/payment-pipeline/2026-11-03.log
```

### 10.3 Config files

**`/opt/etl/config/payment-service/secrets.env`** (`chmod 600`, owner `spark`)
```bash
MONGO_URI='mongodb://etl_user:Strong%23Pass123@10.10.1.11:27017,10.10.1.12:27017,10.10.1.13:27017/?replicaSet=rs0&authSource=admin'
MONGO_DATABASE='bronze_db'
POSTGRES_URL='jdbc:postgresql://10.10.1.30:5444/silver_db'
POSTGRES_USER='etl_user'
POSTGRES_PASSWORD='Strong#Pass456'
SPARK_MASTER='spark://10.10.1.11:7077'
SPARK_SHUFFLE_PARTITIONS='16'
```
(Project ની `application-prod.yml` આ જ environment variable names વાંચે છે.)

**`/opt/etl/config/payment-service/application-prod.yml`** – jar અંદરની `application-prod.yml` ઉપર merge થાય છે:
```yaml
etl:
  spark:
    config:
      # ---- Driver S1 પર છે; executors આ address પર પાછા connect થાય ----
      "[spark.driver.host]": "10.10.1.11"
      "[spark.driver.bindAddress]": "0.0.0.0"
      "[spark.driver.port]": "7100"
      "[spark.blockManager.port]": "7200"
      "[spark.port.maxRetries]": "16"

      # ---- એક job કેટલા resources લેશે (નહીંતર એક job આખો cluster લઈ લે!) ----
      "[spark.cores.max]": "4"
      "[spark.executor.cores]": "2"
      "[spark.executor.memory]": "3g"

      # ---- Executors ને jars ----
      "[spark.jars]": "/opt/etl/apps/payment-service/current/executor-libs/*.jar"

      # ---- History Server માટે (optional, section 14) ----
      "[spark.eventLog.enabled]": "true"
      "[spark.eventLog.dir]": "file:/opt/spark-events"
```

> **`spark.cores.max` કેમ ફરજિયાત?** Spark standalone માં default રીતે **એક application બધા free cores લઈ લે છે**.
> ત્યારે Airflow એક સાથે 2 jobs ચલાવે તો બીજો job "WAITING" માં અટકી રહે.

### 10.4 `run-etl.sh` – Airflow જે script ચલાવશે

**`/opt/etl/bin/run-etl.sh`**
```bash
#!/usr/bin/env bash
# Usage: run-etl.sh <service> <pipeline> [loadType] [runId]
#   run-etl.sh payment-service payment-pipeline INCREMENTAL scheduled__2026-11-03T02:00:00+00:00
set -euo pipefail

SERVICE="${1:?service name required}"
PIPELINE="${2:?pipeline name required}"
LOAD_TYPE="${3:-INCREMENTAL}"
RUN_ID="${4:-manual}"

APP_DIR="/opt/etl/apps/${SERVICE}/current"
CONF_DIR="/opt/etl/config/${SERVICE}"
LOG_DIR="/opt/etl/logs/${SERVICE}/${PIPELINE}"
mkdir -p "${LOG_DIR}"

# Secrets → environment variables (application-prod.yml વાંચે છે)
set -a
source "${CONF_DIR}/secrets.env"
set +a

JAVA_OPTS="${JAVA_OPTS:--Xms1g -Xmx2g}"   # driver memory અહીંથી

echo "[$(date '+%F %T')] START service=${SERVICE} pipeline=${PIPELINE} loadType=${LOAD_TYPE} runId=${RUN_ID}"

# tee: output Airflow log માં પણ દેખાય અને S1 ની file માં પણ રહે
# pipefail: java નો exit code જ script નો exit code બને (tee નો નહિ)
java ${JAVA_OPTS} -jar "${APP_DIR}/app.jar" \
    --spring.profiles.active=prod \
    --spring.config.additional-location="file:${CONF_DIR}/" \
    --spring.main.web-application-type=none \
    --etl.runner.enabled=true \
    --pipeline="${PIPELINE}" \
    --loadType="${LOAD_TYPE}" \
    --triggeredBy="airflow:${RUN_ID}" \
    2>&1 | tee -a "${LOG_DIR}/$(date +%F).log"
```
```bash
chmod 750 /opt/etl/bin/run-etl.sh
```

`--spring.main.web-application-type=none` → Tomcat શરૂ નહિ થાય, એટલે port 8080 (Spark Master UI) સાથે conflict પણ નહિ.

### 10.5 Deploy steps (દર release વખતે)

```bash
# Laptop/CI પરથી
VERSION=1.0.1
ssh spark@10.10.1.11 "mkdir -p /opt/etl/apps/payment-service/releases/${VERSION}"
scp target/spark-etl-pipeline-1.0.1.jar spark@10.10.1.11:/opt/etl/apps/payment-service/releases/${VERSION}/app.jar
scp -r target/executor-libs             spark@10.10.1.11:/opt/etl/apps/payment-service/releases/${VERSION}/

# S1 પર: નવું version active કરો
ssh spark@10.10.1.11 "ln -sfn /opt/etl/apps/payment-service/releases/${VERSION} /opt/etl/apps/payment-service/current"
```

- **જૂના jar પર overwrite ક્યારેય નહિ** – ચાલુ job ની jar બદલાઈ જાય તો crash. નવું folder + symlink swap સલામત છે.
- **Rollback**: `ln -sfn .../releases/1.0.0 .../current` – બસ.
- છેલ્લા 3–5 releases રાખો, બાકી delete.

### 10.6 Airflow વગર પહેલો manual test (S1 પર)

```bash
sudo -iu spark
/opt/etl/bin/run-etl.sh payment-service payment-pipeline INCREMENTAL manual-test-1
echo "exit code = $?"
```
એ જ સમયે `http://10.10.1.11:8080` ખોલો → **Running Applications** માં `spark-etl-pipeline` દેખાશે, 2 executors (S2 અને S3 પર).

✅ Manual run સફળ થાય **પછી જ** Airflow પર જાઓ. (Airflow માં debug કરવું વધારે અઘરું છે.)

---

## 11. Phase 6 – Airflow server setup

### 11.1 Airflow components (એક જ server S4 પર)

| Component | કામ | Port |
|---|---|---|
| `airflow api-server` | Web UI + REST API | **8080** (Admin PC થી) |
| `airflow scheduler` | સમય થાય ત્યારે task શરૂ કરે; **LocalExecutor** થી task એ જ server પર subprocess તરીકે ચાલે | – |
| `airflow dag-processor` | `dags/` folder ની `.py` files parse કરે | – |
| `airflow triggerer` | Deferrable sensors માટે | – |
| PostgreSQL (Airflow metadata) | DAG runs, task state, connections | 5432 – **ફક્ત localhost** |

> LocalExecutor પૂરતો છે; CeleryExecutor (Redis 6379 + અલગ workers) ની જરૂર નથી કારણ કે ભારે કામ Spark cluster પર થાય છે, Airflow ફક્ત SSH કરે છે.

### 11.2 Install (Ubuntu example)

```bash
sudo useradd -m -s /bin/bash airflow
sudo apt install -y python3-venv python3-dev build-essential libpq-dev postgresql

# Airflow metadata DB
sudo -u postgres psql -c "CREATE USER airflow WITH PASSWORD 'AirflowPass#1';"
sudo -u postgres psql -c "CREATE DATABASE airflow OWNER airflow;"

sudo -iu airflow
python3 -m venv ~/venv && source ~/venv/bin/activate

AIRFLOW_VERSION=3.x.y            # Airflow ની latest stable 3.x version મૂકો
PYTHON_VERSION="$(python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
pip install "apache-airflow[postgres]==${AIRFLOW_VERSION}" \
            apache-airflow-providers-ssh \
            apache-airflow-providers-fab \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
```

**`~/airflow/airflow.cfg`** (અથવા env vars) – મુખ્ય settings:
```ini
[core]
executor = LocalExecutor
load_examples = False
parallelism = 8
auth_manager = airflow.providers.fab.auth.manager.fab_auth_manager.FabAuthManager

[database]
sql_alchemy_conn = postgresql+psycopg2://airflow:AirflowPass%231@localhost:5432/airflow

[api]
port = 8080
```
```bash
airflow db migrate
airflow users create --username admin --role Admin --firstname Admin --lastname User --email admin@example.com --password '***'
```

દરેક component માટે systemd unit (એક example – બાકીના 3 માટે `ExecStart` બદલો: `scheduler`, `dag-processor`, `triggerer`):

```ini
# /etc/systemd/system/airflow-api-server.service
[Unit]
Description=Airflow API Server
After=network-online.target postgresql.service

[Service]
User=airflow
Environment=AIRFLOW_HOME=/home/airflow/airflow
ExecStart=/home/airflow/venv/bin/airflow api-server
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
```

### 11.3 Airflow → S1 SSH setup

```bash
# S4 પર, airflow user તરીકે
ssh-keygen -t ed25519 -N "" -f ~/.ssh/id_ed25519
ssh-copy-id -i ~/.ssh/id_ed25519.pub spark@10.10.1.11      # અથવા public key S1 ના ~spark/.ssh/authorized_keys માં
ssh-keyscan 10.10.1.11 >> ~/.ssh/known_hosts
ssh spark@10.10.1.11 "hostname && java -version"           # password વગર ચાલવું જોઈએ
```

**Airflow Connection બનાવો:**
```bash
airflow connections add ssh_spark_edge \
    --conn-type ssh \
    --conn-host 10.10.1.11 \
    --conn-login spark \
    --conn-port 22 \
    --conn-extra '{"key_file": "/home/airflow/.ssh/id_ed25519"}'
```

**Pool બનાવો** (cluster ની capacity = એક સાથે કેટલા Spark jobs):
```bash
airflow pools set spark_cluster 3 "Spark cluster: 12 cores / 4 cores per job = 3 parallel jobs"
```

> **Security (optional પણ સારું)**: S1 ના `~spark/.ssh/authorized_keys` માં Airflow ની key ને ફક્ત `run-etl.sh` ચલાવવાની છૂટ આપવા
> `from="10.10.1.20"` prefix ઉમેરો.

---

## 12. Phase 7 – Airflow DAG: multiple services, multiple jobs, dependency

### 12.1 Example scenario

| Service (JAR) | Pipelines | Dependency |
|---|---|---|
| `customer-service` | `customer-pipeline` | સૌથી પહેલા (dimensions) |
| `payment-service` | `payment-pipeline`, `transaction-pipeline` | customer પછી, બંને એક સાથે (parallel) |
| `reporting-service` | `daily-summary-pipeline` | payment + transaction બંને સફળ થાય પછી |

```
customer_pipeline ──┬──► payment_pipeline ─────┬──► daily_summary_pipeline
                    └──► transaction_pipeline ─┘
```

### 12.2 Reusable helper – `dags/etl_common.py`

```python
from datetime import timedelta

from airflow.providers.ssh.operators.ssh import SSHOperator

SSH_CONN_ID = "ssh_spark_edge"
SPARK_POOL = "spark_cluster"

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}


def etl_task(service: str, pipeline: str, load_type: str = "INCREMENTAL", **kwargs) -> SSHOperator:
    """One Airflow task = one Spark job on the cluster (via run-etl.sh on the master node)."""
    command = (
        f"/opt/etl/bin/run-etl.sh {service} {pipeline} "
        "{{ params.load_type | default('" + load_type + "') }} "
        "'{{ run_id }}'"
    )
    return SSHOperator(
        task_id=pipeline.replace("-", "_"),
        ssh_conn_id=SSH_CONN_ID,
        command=command,
        cmd_timeout=None,     # ⚠️ default inactivity timeout ખૂબ નાનો છે; Spark થોડી વાર log ન લખે તો task fail થાય
        get_pty=True,         # Airflow માં task kill/mark-failed કરો તો S1 પરનો java process પણ બંધ થાય
        pool=SPARK_POOL,
        **kwargs,
    )
```

### 12.3 એક DAG અંદર dependency – `dags/daily_etl_dag.py`

```python
from datetime import datetime

from airflow.sdk import DAG, Param

from etl_common import DEFAULT_ARGS, etl_task

with DAG(
    dag_id="daily_etl",
    description="Bronze (MongoDB) -> Silver (EDB PAS) daily ETL",
    schedule="0 2 * * *",                 # રોજ 02:00 (Airflow default timezone UTC છે – જરૂર પ્રમાણે સેટ કરો)
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,                    # એક જ DAG run એક સમયે → એક pipeline બે વાર સાથે નહિ ચાલે
    default_args=DEFAULT_ARGS,
    params={"load_type": Param("INCREMENTAL", enum=["INCREMENTAL", "FULL", "RETRY_REJECTED"])},
    tags=["etl", "spark"],
) as dag:

    customer = etl_task("customer-service", "customer-pipeline")
    payment = etl_task("payment-service", "payment-pipeline")
    transaction = etl_task("payment-service", "transaction-pipeline")
    daily_summary = etl_task("reporting-service", "daily-summary-pipeline")

    customer >> [payment, transaction] >> daily_summary
```

**આ DAG કેવી રીતે ચાલશે:**
1. 02:00 → `customer_pipeline` (SSH → S1 → Spark job). Exit 0 → success.
2. પછી `payment_pipeline` અને `transaction_pipeline` **એક સાથે** શરૂ (pool માં 3 slots છે, cluster પર 2 apps × 4 cores = 8 cores).
3. બંને success → `daily_summary_pipeline`.
4. કોઈ task exit 1 આપે → 5 મિનિટ પછી retry (2 વાર). છતાં fail → downstream tasks **નહિ ચાલે** (`upstream_failed`).
5. Manual FULL load: Airflow UI → "Trigger DAG w/ config" → `load_type = FULL`.

> **Retry સલામત છે?** હા – framework નું design: job FAILED થાય તો MongoDB flags commit થતા નથી,
> એટલે retry એ જ documents ફરી વાંચે છે. Duplicate data નહિ આવે (UPSERT).

### 12.4 અલગ-અલગ DAGs વચ્ચે dependency – Assets (Airflow 3)

જો દરેક service ની અલગ team/અલગ DAG હોય:

```python
# dags/payment_dag.py  – producer
from datetime import datetime

from airflow.sdk import DAG, Asset
from etl_common import DEFAULT_ARGS, etl_task

FACT_PAYMENT = Asset("edb://silver_db/fact_payment")

with DAG(dag_id="payment_etl", schedule="0 2 * * *", start_date=datetime(2026, 11, 1),
         catchup=False, max_active_runs=1, default_args=DEFAULT_ARGS):
    etl_task("payment-service", "payment-pipeline", outlets=[FACT_PAYMENT])
```

```python
# dags/reporting_dag.py  – consumer: fact_payment update થાય ત્યારે આપોઆપ ચાલે
from datetime import datetime

from airflow.sdk import DAG, Asset
from etl_common import DEFAULT_ARGS, etl_task

with DAG(dag_id="reporting_etl", schedule=[Asset("edb://silver_db/fact_payment")],
         start_date=datetime(2026, 11, 1), catchup=False, max_active_runs=1, default_args=DEFAULT_ARGS):
    etl_task("reporting-service", "daily-summary-pipeline")
```
(Airflow 2.x માં `Asset` ને બદલે `Dataset` – `from airflow.datasets import Dataset`.)

### 12.5 એક જ pipeline બે જગ્યાએથી ન ચાલે તે કેવી રીતે?

| જોખમ | ઉપાય |
|---|---|
| એ જ DAG નો બીજો run (પાછલો હજુ ચાલુ) | `max_active_runs=1` |
| એક જ pipeline બે અલગ DAG માં | તે pipeline માટે અલગ pool, 1 slot: `airflow pools set lock_payment_pipeline 1 ""` અને task માં `pool="lock_payment_pipeline"` |
| કોઈ S1 પર હાથે `run-etl.sh` ચલાવે | Runner માં DB check (section 9.1 નોંધ) |

### 12.6 Failure alert

```python
DEFAULT_ARGS = {
    ...,
    "email": ["data-team@yourcompany.com"],
    "email_on_failure": True,
}
```
(`airflow.cfg` માં `[smtp]` section configure કરવું; S4 → SMTP port ખોલવો.)

---

## 13. Resource sizing – Spark અને MongoDB એક જ server પર

### 13.1 MongoDB નો RAM trap

MongoDB WiredTiger cache **default = (RAM − 1 GB) નો 50%**. 16 GB server પર ≈ 7.5 GB ફક્ત Mongo cache!
Spark worker ને પણ 8–10 GB આપો તો server swap/OOM માં જાય → mongod કે executor kill થાય.
એટલે `cacheSizeGB` **હાથે નક્કી કરો** (section 8.1).

### 13.2 Example: દરેક server = 8 cores, 16 GB RAM

| | S1 (master/edge) | S2 / S3 (worker) |
|---|---|---|
| OS + buffer | 2 GB | 2 GB |
| MongoDB `cacheSizeGB` | 4 GB (+ ~1 GB overhead) | 4 GB (+ ~1 GB overhead) |
| Spark daemon (Master/Worker) | 1 GB | 1 GB |
| Driver JVMs (3 parallel × `-Xmx2g`) | 6 GB | – |
| `SPARK_WORKER_MEMORY` | – | **8 GB** |
| `SPARK_WORKER_CORES` | – | **6** (2 cores Mongo/OS માટે) |
| **કુલ** | ~14 GB | ~16 GB |

**Cluster capacity**: 2 workers × 6 cores = 12 cores, 16 GB.
**એક job**: `spark.cores.max=4`, `spark.executor.cores=2`, `spark.executor.memory=3g` → 2 executors (S2 પર 1, S3 પર 1), 6 GB.
**Parallel jobs**: 12 cores ÷ 4 = **3 jobs** → Airflow pool `spark_cluster` = 3. ✅ બધું એકબીજા સાથે match.

> Executor memory 3g માંગો તો JVM ખરેખર ~3.3–3.4 GB લે છે (memory overhead). Worker memory 8g માં 2 executors (6g) આરામથી સમાય.

`spark.sql.shuffle.partitions` ≈ job ના cores × 2–4 → 4 cores માટે 8–16 (`SPARK_SHUFFLE_PARTITIONS=16`).

---

## 14. Monitoring, Logs, Cleanup

### 14.1 Log ક્યાં જોવા

| શું | ક્યાં |
|---|---|
| Job નો overall log (driver) | **Airflow UI → task → Logs** (SSH output) અથવા S1: `/opt/etl/logs/<service>/<pipeline>/<date>.log` |
| Job ની DB audit (કેટલા rows, કયું step fail) | EDB: `etl_audit.etl_job_run`, `etl_job_step`, `etl_rejected_record` |
| Executor errors (stack trace) | Spark Master UI `:8080` → Application → Executor → **stderr** (S2/S3 નું `:8081` ખુલ્લું જોઈએ) અથવા `/data/spark/work/app-*/<id>/stderr` |
| Spark daemons | `/var/log/spark/` અથવા `journalctl -u spark-worker` |
| પૂરા થયેલા jobs (stages, timings) | History Server `:18080` |

### 14.2 (Optional) Spark History Server – S1

```bash
# /opt/spark/conf/spark-defaults.conf (S1)
spark.history.fs.logDirectory   file:/opt/spark-events
spark.history.ui.port           18080
spark.history.fs.cleaner.enabled true
spark.history.fs.cleaner.maxAge  14d
```
```bash
sudo -u spark SPARK_NO_DAEMONIZE=true /opt/spark/sbin/start-history-server.sh    # systemd unit બનાવી લો
```
Driver S1 પર ચાલે છે અને event logs S1 ના `/opt/spark-events` માં લખાય છે, એટલે shared storage (HDFS/NFS) ની જરૂર નથી.

### 14.3 Disk cleanup

| શું ભરાય છે | ઉપાય |
|---|---|
| Workers નું `/data/spark/work` (દરેક job ના jars + logs) | `spark.worker.cleanup.*` (section 7.3) |
| `/data/spark/tmp` (shuffle) | Job પૂરો થતા Spark સાફ કરે; crash પછી બાકી રહે તો cron |
| `/opt/etl/logs` | `logrotate` અથવા `find /opt/etl/logs -mtime +30 -delete` cron |
| `/opt/spark-events` | History cleaner (14.2) |
| જૂના releases | છેલ્લા 5 રાખો |
| Airflow logs (S4) | `airflow db clean` + log folder cleanup |

---

## 15. Chain માં જે Gap હતા – અને તેનો ઉકેલ

તમારા plan માં નીચેની કડીઓ ખૂટતી હતી / સ્પષ્ટ નહોતી. દરેક તૂટે તો આખી chain અટકે:

| # | Gap | શું થાત | ઉકેલ (section) |
|---|---|---|---|
| 1 | **JAR કેવી રીતે ચાલશે?** Project REST-based Spring Boot app છે, spark-submit job નથી | Airflow પાસે job શરૂ કરવાનો / પરિણામ જાણવાનો સાફ રસ્તો નહિ | Batch Runner + `run-etl.sh` + exit code (9.1, 10.4) |
| 2 | **Executors ને project classes + Mongo connector + JDBC driver નથી મળતા** | `ClassNotFoundException` (FlagUpdateTask, `com.mongodb...`, `org.postgresql.Driver`) | `executor-libs` + `spark.jars` (9.2) |
| 3 | **DB access ફક્ત master ને નહિ, workers ને પણ જોઈએ** | Executors ને Mongo/EDB connection timeout | Firewall + Mongo `bindIp` + EDB `pg_hba.conf` માં S2/S3 (8) |
| 4 | **Mongo URI માં `localhost`** | Executors પોતાના local Mongo માંથી વાંચે → silent ખોટો data | બધા servers ના IP વાળો URI (8.3) |
| 5 | **ETL Mongo માં flag લખે છે** | Read-only user → flag update fail | `readWrite` role (8.2) |
| 6 | **Spark ports random** | Limited-port firewall માં executors driver સુધી પહોંચે નહિ, job "no resources" માં અટકે | Fixed ports 7077/7078/7100-7116/7200-7216 (3, 10.3) |
| 7 | **`spark.driver.host=localhost`** (dev config) | Executors `localhost` પર driver શોધે | `spark.driver.host=10.10.1.11` (10.3) |
| 8 | **Ubuntu `127.0.1.1` hostname** | Workers ખોટા IP થી register | `SPARK_LOCAL_IP` (6.1, 7.3) |
| 9 | **Spark version mismatch** (local doc 3.5.1, pom 3.5.9) | Serialization errors | Cluster = pom નું exact version (7.2) |
| 10 | **એક job આખો cluster લઈ લે** | Parallel jobs WAITING | `spark.cores.max` + Airflow pool (10.3, 11.3) |
| 11 | **MongoDB અને Spark RAM માટે લડે** | OOM / swap / mongod kill | `cacheSizeGB` + worker memory sizing (13) |
| 12 | **`spark-defaults.conf` / `spark.driver.memory` java -jar માં લાગુ નથી પડતા** | Settings ignore થાય | Settings `application-prod.yml` માં, driver memory `-Xmx` (9.3) |
| 13 | **In-memory `JobLockManager` batch mode માં બેકાર** | એક pipeline બે વાર સાથે ચાલી શકે | `max_active_runs=1`, pool, DB check (12.5) |
| 14 | **SSHOperator નો default timeout** | લાંબો job અધવચ્ચે "SSH command timed out" | `cmd_timeout=None` (12.2) |
| 15 | **Airflow task kill કરો તો S1 પર job ચાલુ રહે** | Zombie Spark jobs | `get_pty=True` (12.2) |
| 16 | **Port 8080 conflict** (Spring Boot `server.port` vs Spark Master UI) | App શરૂ ન થાય | Batch mode માં web બંધ (10.4); Mode B માં `server.port=8090` (17) |
| 17 | **Worker work-dir માં દરેક job ના jars ભેગા થાય** | થોડા અઠવાડિયામાં disk full | Worker cleanup (7.3, 14.3) |
| 18 | **JAR overwrite while running** | ચાલુ job crash | Versioned releases + symlink (10.5) |
| 19 | **Passwords jar/yml માં** | Git માં leak | `secrets.env` (chmod 600) + env vars (10.3) |
| 20 | **Spark standalone પર auth નથી** | 7077 સુધી પહોંચનાર કોઈ પણ code ચલાવી શકે | 7077/7078 ફક્ત cluster IPs માટે (3.2, 6.4); વધારે માટે `spark.authenticate` |
| 21 | **Replica set members ના hostnames** | Driver connect થાય પણ executors replica set discover ન કરી શકે | એ hostnames બધા Spark servers ના `/etc/hosts` માં (6.1) |
| 22 | **EDB `max_connections`** | ઘણા executors + partitions → "too many connections" | max_connections / JDBC partitions નું આયોજન (8.4) |

---

## 16. Troubleshooting

| લક્ષણ (error) | સંભવિત કારણ | તપાસ / ઉકેલ |
|---|---|---|
| `Initial job has not accepted any resources; check your cluster UI` | (a) Workers alive નથી (b) માંગેલી memory/cores worker પાસે નથી (c) **Executors driver સુધી પહોંચી શકતા નથી** | Master UI માં workers? `spark.executor.memory` ≤ worker memory? S2 પરથી `nc -zv 10.10.1.11 7100` (job ચાલુ હોય ત્યારે). Worker UI → executor stderr માં `Failed to connect to /10.10.1.11:7100` |
| Master UI માં worker નું address `127.0.1.1` | `/etc/hosts` trap | `SPARK_LOCAL_IP` set કરો, worker restart |
| Worker register જ નથી થતો | 7077 બંધ / master URL ખોટો | S2: `nc -zv 10.10.1.11 7077`, `journalctl -u spark-worker` |
| Executor "EXITED" વારંવાર | 7078 બંધ / Java path ખોટો / memory | Worker UI → stderr |
| `ClassNotFoundException: ...FlagUpdateTask` અથવા `com.mongodb...` executor પર | `spark.jars` ખોટો path / `app-classes.jar` નથી | `ls /opt/etl/apps/<svc>/current/executor-libs/`, executor stderr માં "Fetching ... jar" lines |
| `No suitable driver` / `org.postgresql.Driver` not found | postgresql jar `executor-libs` માં નથી | copy-dependencies ફરી |
| `InvalidClassException ... local class incompatible: serialVersionUID` | Driver અને cluster નું Spark version અલગ | `/opt/spark/RELEASE` vs pom |
| `MongoTimeoutException` executor પર | Firewall 27017 / `bindIp` / replica set hostname resolve નથી | S2 પરથી `mongosh "<same URI>"` |
| Data ઓછો / ખાલી વંચાય, error નહિ | Mongo URI માં `localhost` | URI ચકાસો (8.3) |
| `no pg_hba.conf entry for host "10.10.1.12"` | Worker IP pg_hba માં નથી | 8.4 |
| `FATAL: sorry, too many clients already` | EDB max_connections | Parallel jobs/partitions ઘટાડો અથવા limit વધારો |
| Airflow: `SSH command timed out` | `cmd_timeout` default | `cmd_timeout=None` |
| Airflow: `Authentication failed` (SSH) | Key/known_hosts | S4 પર `sudo -iu airflow ssh spark@10.10.1.11` હાથે ચલાવો |
| Airflow task success પણ data load નથી | Script exit code ગળી ગયો | `set -o pipefail` છે? Runner `System.exit(SpringApplication.exit(...))` કરે છે? |
| બીજો job "WAITING" | `spark.cores.max` નથી / pool મોટો | 10.3, 11.3 |
| Worker disk full | Cleanup બંધ | 7.3, 14.3 |
| Job ચાલુ હોય ને mongod restart / OOM kill | RAM overcommit | `dmesg | grep -i kill`, section 13 |

---

## 17. Alternative: Spring Boot ને REST service તરીકે રાખવો (Mode B)

જો code માં Batch Runner ઉમેરવો ન હોય, તો હાલની REST API એમ જ વાપરી શકાય:

```
Airflow (S4) ──HTTP 8090──► S1: Spring Boot app (હંમેશા ચાલુ, systemd) ──► Spark cluster
```

**ફેરફાર:**
- `server.port: 8090` (8080 Spark Master UI પાસે છે).
- Port matrix માં ઉમેરો: **S4 → S1 : 8090** (port 22 ની જરૂર નહિ).
- `spark.jars`, `spark.driver.*` ports, Mongo/EDB settings – section 9.2 અને 10.3 પ્રમાણે જ.
- `spark.cores.max` જરૂરી – નહિતર આ એક app હંમેશા આખો cluster પકડી રાખે.
  Idle હોય ત્યારે executors છોડવા: `spark.dynamicAllocation.enabled=true`, `spark.dynamicAllocation.shuffleTracking.enabled=true`.
- Airflow DAG:
  1. `HttpOperator` → `POST /api/etl/pipelines/payment-pipeline/run?loadType=INCREMENTAL&async=true` → response માંથી `jobRunId` (XCom).
  2. `HttpSensor` (mode=`reschedule`) → `GET /api/etl/jobs/{jobRunId}` → `status` `RUNNING` ન રહે ત્યાં સુધી poll; `FAILED` હોય તો task fail કરો.

**કેમ Mode A વધુ સારો:**

| | Mode A (SSH batch) | Mode B (REST service) |
|---|---|---|
| Job નું પરિણામ Airflow ને | Exit code – સીધું | Polling logic લખવું પડે |
| Cluster resources | Job પૂરો → છૂટા | App હંમેશા કંઈક પકડી રાખે |
| App restart/deploy | ચાલુ jobs પર અસર નહિ | ચાલુ async jobs ગુમ (RUNNING row અટકે) |
| એક job ની memory leak/crash | ફક્ત એ job | આખી app ને અસર |
| Code ફેરફાર | નાનો runner class | કંઈ નહિ |

---

## 18. Final Checklist

**Phase 0 – માહિતી**
- [ ] OS, CPU, RAM દરેક server ના નોંધ્યા
- [ ] Mongo replica set? member hostnames?
- [ ] EDB host/port/version, DBA સાથે વાત
- [ ] Network team ને port matrix (3.2) મોકલી

**Phase 1 – OS/Network (S1–S5)**
- [ ] `/etc/hosts` પાંચેય પર
- [ ] chrony
- [ ] `spark` user + directories
- [ ] Firewall rules + `nc` થી ચકાસ્યા

**Phase 2 – Spark**
- [ ] Java 17 (S1–S3)
- [ ] Spark = pom version (S1–S3)
- [ ] `spark-env.sh` (SPARK_LOCAL_IP!)
- [ ] systemd master/worker, enable
- [ ] Master UI: 2 alive workers, સાચા IP

**Phase 3 – DBs**
- [ ] Mongo `bindIp`, `cacheSizeGB`, `etl_user` readWrite
- [ ] S2/S3 પરથી `mongosh` connect
- [ ] EDB `listen_addresses`, `pg_hba` (3 IPs), `etl_user`, `silver_db`
- [ ] S2/S3 પરથી `nc -zv edb-server 5444`

**Phase 4 – Code**
- [ ] `EtlBatchRunner` + main ફેરફાર
- [ ] `executor-libs` build step
- [ ] Local પર `java -jar ... --etl.runner.enabled=true --pipeline=...` → exit code ચકાસ્યો

**Phase 5 – Deploy**
- [ ] `/opt/etl` structure, `run-etl.sh`, `secrets.env` (600), `application-prod.yml`
- [ ] Manual run S1 પર સફળ, Master UI માં 2 executors દેખાયા
- [ ] EDB audit tables માં SUCCESS row, Mongo flags `Y`

**Phase 6/7 – Airflow**
- [ ] Airflow 3 + ssh provider, LocalExecutor, Postgres metadata
- [ ] SSH key, known_hosts, `ssh_spark_edge` connection
- [ ] `spark_cluster` pool = cluster capacity
- [ ] DAG: `cmd_timeout=None`, `get_pty=True`, `max_active_runs=1`, retries
- [ ] Test: task fail કરાવી retry + alert ચકાસ્યા
- [ ] Test: Airflow માંથી task "mark failed" → S1 પર java process બંધ થયો?

**Phase 8 – Operations**
- [ ] Worker cleanup, log rotation, History Server
- [ ] Rollback એક વાર practice કર્યું
- [ ] આ document માં તમારા real IP/ports update કર્યા
