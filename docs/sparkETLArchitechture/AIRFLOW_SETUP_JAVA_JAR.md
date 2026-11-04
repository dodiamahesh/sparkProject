# Airflow Installation ane Setup: spark-submit vagar (`java -jar`), Linux A to Z

Aa guide **s4 (Airflow server)** par Airflow ne shunya thi chalu karva mate che. Ahiya ETL jar **`java -jar`** thi chale che, `spark-submit` thi nahi. Setup puru thaya pachi:

- Browser ma `http://s4:8080` thi login thai shakase.
- Server reboot thay to pan Airflow automatic chalu thase (systemd).
- Airflow `BashOperator` thi ETL jar chalavse. Jar ni andar Spark che, ane te Spark cluster (s1, s2, s3) sathe connect thase.
- DAG file `/opt/airflow/dags/` ma muko etle te UI ma aavi jase.

> spark-submit vali rit mate [AIRFLOW_SETUP.md](AIRFLOW_SETUP.md) juo. Ports mate [CLUSTER_PORTS.md](CLUSTER_PORTS.md) juo.

---

## 0. spark-submit vali rit karta su alag che

| | Aa guide (`java -jar`) | spark-submit guide |
|---|---|---|
| s4 par Spark install | **Na**, fakt Java 17 | Haa (Spark 3.5.1 client) |
| Jar nu packaging | **Atyare jevu j** (Spring Boot fat jar, Spark andar) | Shade jar, Spark `provided` |
| Airflow operator | `BashOperator` (ane option: `SSHOperator`) | `SparkSubmitOperator` |
| Airflow Spark provider ane connection | Na | Haa |
| Spark settings kya | Jar ni bahar `application-cluster.properties` | `spark-defaults.conf` + DAG |
| Java 17 `--add-opens` flags | **Haath thi** aapva pade (wrapper script ma) | spark-submit pote umere |
| Executors ne connector jars | `spark.jars` thi (`executor-jars/` folder s4 par) | App jar ma j hoy |
| Spark version | Jar ma bundled 3.5.1 = cluster ma pan **3.5.1** j joie | Cluster no Spark vaparay |

### Project ma jaruri ferfar (aa guide pehla)

Fakt Airflow mate jaruri ferfar, packaging nahi badlay:
1. **`JobRunner`**: `--pipeline.job=<naam>` thi ek job chalave ane exit thay.
2. **Scheduler switch**: `PipelineScheduler` par `@ConditionalOnProperty(name = "pipeline.scheduler.enabled", matchIfMissing = true)`.
3. **Exit code**: `main()` ma `System.exit(SpringApplication.exit(...))`. Job fail thay to Airflow ne non-zero code male.

Shade plugin, `provided` scope ke `setIfMissing` **nathi** joitu.

---

## 1. Su install thase ane kem

```
┌─────────────────────────── s4 (Airflow server) ───────────────────────────┐
│  airflow api-server    → UI + REST API (port 8080) + Execution API        │
│  airflow scheduler     → time thay etle task chalave (LocalExecutor)      │
│  airflow dag-processor → dags/ folder ni .py files vanchi ne DAG banave   │
│  airflow triggerer     → deferrable tasks mate (future mate)              │
│  PostgreSQL            → Airflow metadata DB                              │
│  Java 17               → ETL jar chalavva                                 │
│  /opt/etl/             → jar + executor-jars + config + run-job.sh        │
│                                                                           │
│  BashOperator → /opt/etl/run-job.sh <job> → java -jar  (= Spark DRIVER)   │
└───────────────────────────────────────────────────────────────────────────┘
          │ spark://s1:7077
          ▼
   s1 Spark Master ──► s2, s3 Workers ──► Mongo (s1-s3), EDBP (s5)
```

| Nirnay | Kem |
|---|---|
| **Airflow 3.x** | Airflow 2 nu support 22 April 2026 e puru thai gayu |
| **LocalExecutor** | Ek j server che. Celery mate Redis ane worker server ni jarur nathi |
| **PostgreSQL** | Airflow nu default SQLite fakt testing mate che, ane ema ek vaar ma ek j process lakhi shake. Fakt test karvu hoy to step 5 skip kari SQLite thi chalavi shako |
| **Wrapper script `run-job.sh`** | Java flags, memory ane config path ek j jagya e rahe che. DAG saaf rahe che, ane job haath thi pan e j rite chalavi shakay che |
| **Config jar ni bahar** | Server, port ke memory badlavva mate jar fari build nathi karvi padti |

### Folder layout

```
/opt/airflow/                  AIRFLOW_HOME
├── venv/                      Python virtualenv
├── dags/                      DAG files ahiya
├── logs/                      Task logs
└── plugins/
/opt/etl/
├── spark-weather-etl-1.0.0.jar
├── spark-weather-etl.jar      → symlink (current version)
├── executor-jars/             Mongo connector, MySQL driver (executors mate)
├── config/application-cluster.properties
├── etl.env                    DB credentials (fakt ETL mate)
└── run-job.sh                 Job chalavvanu wrapper
/etc/airflow/airflow.env       Airflow no config + secrets
```

---

## 2. OS taiyari

Badha commands `root` (ke `sudo`) thi chalavo.

### 2.1 Update ane packages

```bash
# Ubuntu 22.04 / 24.04
apt update && apt -y upgrade
apt -y install build-essential libpq-dev curl wget tar netcat-openbsd vim

# RHEL / Rocky / Alma 9
dnf -y update
dnf -y groupinstall "Development Tools"
dnf -y install libpq-devel curl wget tar nmap-ncat vim
```
**Kem:**
- Airflow ni ghani Python libraries install vakhte compile thay che (gcc).
- `libpq` Postgres driver mate joie.
- `nc` thi ports test karishu.

### 2.2 Timezone

```bash
timedatectl set-timezone Asia/Kolkata
```
**Kem:** server ane Airflow no time same hoy to schedule ane logs samjva saral pade.

### 2.3 Hostnames

```bash
cat >> /etc/hosts <<'EOF'
<s1-ip>  s1
<s2-ip>  s2
<s3-ip>  s3
<s4-ip>  s4
<s5-ip>  s5
EOF
```
**Kem:** Spark ane Mongo replica set ek bija ne naam thi shodhe che. Aa j entries **s1, s2, s3 par pan** hovi joie, jethi executors `s4` naam thi driver sudhi pahonchi shake.

### 2.4 `airflow` user ane folders

```bash
useradd --system --create-home --home-dir /opt/airflow --shell /bin/bash airflow
mkdir -p /opt/airflow/{dags,logs,plugins} /opt/etl/{executor-jars,config} /etc/airflow
chown -R airflow:airflow /opt/airflow /opt/etl
```
**Kem:** Airflow ane ETL `root` thi na chalavva joie.

---

## 3. Java 17

```bash
# Ubuntu
apt -y install openjdk-17-jre-headless
# RHEL
dnf -y install java-17-openjdk-headless

java -version
```
**Kem:** ETL jar ane eni andar no Spark driver Java 17 par chale che (project `java.version=17`). Workers par pan Java 17 j hovu joie.

**Spark install nathi karvanu**, kem ke Spark libraries jar ni andar j che.

### 3.1 Cluster sudhi pahonchay che? (test)

```bash
nc -zv s1 7077     # Spark master
nc -zv s1 27017    # Mongo
nc -zv s2 27017
nc -zv s3 27017
nc -zv s5 3306     # EDBP
```
Badha `succeeded` ke `open` batavvu joie. Aa test pass thaya vagar aagal na steps kaam nahi kare.

---

## 4. ETL jar ane config (s4 par)

### 4.1 Build ane copy (tamara build machine par)

```bash
mvn clean package
# target/spark-weather-etl-1.0.0.jar  ane  target/executor-jars/*.jar  banse

scp target/spark-weather-etl-1.0.0.jar   airflow@s4:/opt/etl/
scp target/executor-jars/*.jar            airflow@s4:/opt/etl/executor-jars/
```
**Kem:**
- `executor-jars/` ma Mongo connector ane MySQL driver che. pom.xml nu `maven-dependency-plugin` e banave che.
- Driver aa jars s2 ane s3 na executors ne **pote mokle che**, etle workers par kai copy nathi karvanu.

### 4.2 Symlink

```bash
sudo -iu airflow
ln -sfn /opt/etl/spark-weather-etl-1.0.0.jar /opt/etl/spark-weather-etl.jar
```
**Kem:** navu version aave tyare fakt symlink badlo. Juno version paacho lavvo (rollback) hoy to pan fakt link badlavvi pade che.

### 4.3 `application-cluster.properties` (jar ni bahar)

```bash
cat > /opt/etl/config/application-cluster.properties <<'EOF'
# ---------- Spark cluster ----------
spark.master=spark://s1:7077
spark.app-name=weather-etl
# executors aa naam thi driver (s4) sudhi pahonche
spark.conf.[spark.driver.host]=s4
spark.conf.[spark.driver.bindAddress]=0.0.0.0
# fixed ports, jethi firewall rule lagavi shakay
spark.conf.[spark.driver.port]=7200
spark.conf.[spark.driver.blockManager.port]=7201
spark.conf.[spark.blockManager.port]=7100
spark.conf.[spark.port.maxRetries]=16
spark.conf.[spark.ui.port]=4040
# resources: darek executor 2 cores, kul 4 cores (s2 ane s3 par 1-1 executor)
spark.conf.[spark.executor.memory]=4g
spark.conf.[spark.executor.cores]=2
spark.conf.[spark.cores.max]=4
# executors ne connector jars (driver aa files workers ne mokle che)
spark.conf.[spark.jars]=/opt/etl/executor-jars/mongo-spark-connector_2.12.jar,/opt/etl/executor-jars/mongodb-driver-sync.jar,/opt/etl/executor-jars/mongodb-driver-core.jar,/opt/etl/executor-jars/bson.jar,/opt/etl/executor-jars/bson-record-codec.jar,/opt/etl/executor-jars/mysql-connector-j.jar

# ---------- Sources / targets ----------
pipeline.mongo-uri=mongodb://s1:27017,s2:27017,s3:27017/?replicaSet=rs0
pipeline.silver-jdbc-url=jdbc:mysql://s5:3306/de_silver
pipeline.gold-jdbc-url=jdbc:mysql://s5:3306/de_gold
EOF
```
**Kem:**
- `--spring.config.additional-location` thi Spring aa file **jar ni andar na config upar** load kare che.
- Server, port ke memory badlavva hoy to fakt aa file badlo, rebuild ke redeploy ni jarur nathi.
- `dev` profile vapro to tema OpenLineage listener che. `cluster` profile ma te nathi, etle OpenMetadata vagar pan chalse.

### 4.4 `etl.env` (DB credentials)

```bash
cat > /opt/etl/etl.env <<'EOF'
# Spring relaxed binding: pipeline.db-username -> PIPELINE_DBUSERNAME
PIPELINE_DBUSERNAME=etl_user
PIPELINE_DBPASSWORD=<edbp-password>
EOF
chmod 600 /opt/etl/etl.env
```
**Kem:**
- Spring ma OS environment variables properties files karta vadhu priority dharave che. Etle jar ni andar no `Root@123` vado password aapoaap override thai jay che.
- Password Airflow na env ke DAG ma nathi rakhvo padto, fakt ETL na script ne j male che.

### 4.5 `run-job.sh` (wrapper)

```bash
cat > /opt/etl/run-job.sh <<'EOF'
#!/usr/bin/env bash
# Usage: run-job.sh <job_name>     e.g. run-job.sh bronze_to_silver
set -euo pipefail

JOB="${1:?job name aapo, jem ke bronze_to_silver}"
ETL_HOME=/opt/etl

set -a; source "$ETL_HOME/etl.env"; set +a

# Java 17 par Spark ne aa module flags joie (pom.xml na spring-boot-maven-plugin jevi j list)
JAVA_MODULE_OPTS="--add-opens=java.base/java.lang=ALL-UNNAMED \
 --add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
 --add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
 --add-opens=java.base/java.io=ALL-UNNAMED \
 --add-opens=java.base/java.net=ALL-UNNAMED \
 --add-opens=java.base/java.nio=ALL-UNNAMED \
 --add-opens=java.base/java.util=ALL-UNNAMED \
 --add-opens=java.base/java.util.concurrent=ALL-UNNAMED \
 --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED \
 --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
 --add-opens=java.base/sun.nio.cs=ALL-UNNAMED \
 --add-opens=java.base/sun.security.action=ALL-UNNAMED \
 --add-opens=java.base/sun.util.calendar=ALL-UNNAMED"

cd "$ETL_HOME"   # relative paths (jo koi hoy) /opt/etl thi resolve thay

# exec: Airflow task kill kare (timeout) to signal seedho java ne male
exec java -Xms512m -Xmx2g $JAVA_MODULE_OPTS \
  -jar "$ETL_HOME/spark-weather-etl.jar" \
  --spring.profiles.active=cluster \
  --spring.config.additional-location=file:$ETL_HOME/config/ \
  --pipeline.scheduler.enabled=false \
  --pipeline.job="$JOB"
EOF
chmod 750 /opt/etl/run-job.sh
```

| Bhag | Kem |
|---|---|
| `-Xmx2g` | Driver ni memory. `java -jar` ma `spark.driver.memory` kaam nathi kartu, etle aa JVM flag thi j set thay |
| `--add-opens ...` | Java 17 par Spark ne internal classes access joie. Aa vagar `IllegalAccessError` ke `InaccessibleObjectException` aave. pom.xml ni list fakt `mvn spring-boot:run` ma lage che, `java -jar` ma nahi |
| `--spring.profiles.active=cluster` | `application-cluster.properties` load thay |
| `--pipeline.scheduler.enabled=false` | Jar no potano 2-minute scheduler band. Have Airflow schedule kare che |
| `--pipeline.job=$JOB` | `JobRunner` aa ek job chalave ane exit thay |
| `exec` | Shell na badle java process j task bane. Timeout vakhte saaf rite band thay, ane Spark executors pan chhuta thay |

### 4.6 Haath thi test (Airflow vagar)

```bash
sudo -iu airflow
/opt/etl/run-job.sh bronze_to_silver; echo "exit code = $?"
```
`exit code = 0` aave ane `etl_job_run` table ma `SUCCESS` vali row aave etle ETL taiyar che. Run chalto hoy tyare Spark master UI (`http://s1:8080`) ma app `weather-etl` dekhase.

**Aa pass thaya pachi j Airflow par jao.** Pachi problem aave to te Airflow ma che evu seedhu samjay.

---

## 5. PostgreSQL (Airflow metadata DB)

> Fakt testing mate SQLite thi chalavvu hoy to aa step skip karo ane step 7 ma `SQL_ALCHEMY_CONN` vali line na lakho.

### 5.1 Install

```bash
# Ubuntu
apt -y install postgresql
systemctl enable --now postgresql

# RHEL
dnf -y install postgresql-server
postgresql-setup --initdb
systemctl enable --now postgresql
```

### 5.2 Database ane user

```bash
sudo -u postgres psql <<'EOF'
CREATE USER airflow WITH PASSWORD 'AirflowDbPass2026';
CREATE DATABASE airflow OWNER airflow ENCODING 'UTF8';
EOF
```
**Kem:**
- `OWNER airflow` rakhvathi Postgres 15+ ma `public` schema nu permission issue nathi aavtu.
- Password ma `@ : / #` na rakho, nahi to connection URL ma escape karva pade.

### 5.3 (Fakt RHEL) password login

```bash
vim /var/lib/pgsql/data/pg_hba.conf
# host  all  all  127.0.0.1/32  ident   →   host  all  all  127.0.0.1/32  scram-sha-256
systemctl restart postgresql
```
**Kem:** RHEL default ma `ident` auth vapre che, etle password thi login fail thay.

### 5.4 Test

```bash
psql "postgresql://airflow:AirflowDbPass2026@localhost:5432/airflow" -c "select 1;"
```

---

## 6. Python ane Airflow install

### 6.1 Python 3.12

```bash
# Ubuntu 24.04
apt -y install python3.12 python3.12-venv python3.12-dev
# RHEL 9
dnf -y install python3.12 python3.12-devel
```

### 6.2 venv ane Airflow

```bash
sudo -iu airflow

python3.12 -m venv /opt/airflow/venv
source /opt/airflow/venv/bin/activate
pip install --upgrade pip

AIRFLOW_VERSION=3.3.2      # pypi.org/project/apache-airflow par latest 3.x joi ne set karo
PYTHON_VERSION=3.12
CONSTRAINTS="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

pip install "apache-airflow[postgres]==${AIRFLOW_VERSION}" \
    apache-airflow-providers-fab \
    apache-airflow-providers-standard \
    --constraint "${CONSTRAINTS}"

airflow version
```

| Package | Kem |
|---|---|
| `apache-airflow[postgres]` | Airflow core + Postgres driver |
| `apache-airflow-providers-fab` | Username ane password vado login, roles. Default `SimpleAuthManager` fakt dev mate che |
| `apache-airflow-providers-standard` | **`BashOperator`**. Aa guide no mukhya operator |
| `--constraint` | Badhi dependencies tested versions par pin thay |

**Spark provider ni jarur nathi**, kem ke `SparkSubmitOperator` nathi vaparvo.

---

## 7. Configuration (`/etc/airflow/airflow.env`)

### 7.1 Secrets banavo

```bash
source /opt/airflow/venv/bin/activate
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"   # FERNET_KEY
openssl rand -hex 32    # JWT_SECRET
openssl rand -hex 32    # API_SECRET_KEY
exit
```

### 7.2 Env file (root thi)

```bash
cat > /etc/airflow/airflow.env <<'EOF'
# ---------- Paths ----------
AIRFLOW_HOME=/opt/airflow
# systemd $PATH expand nathi kartu, etle aakho PATH lakho
PATH=/opt/airflow/venv/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

# ---------- Core ----------
AIRFLOW__CORE__DAGS_FOLDER=/opt/airflow/dags
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__CORE__LOAD_EXAMPLES=False
AIRFLOW__CORE__DEFAULT_TIMEZONE=Asia/Kolkata
AIRFLOW__CORE__PARALLELISM=8
AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG=4
AIRFLOW__CORE__FERNET_KEY=<FERNET_KEY>
AIRFLOW__CORE__AUTH_MANAGER=airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager
AIRFLOW__CORE__EXECUTION_API_SERVER_URL=http://localhost:8080/execution/

# ---------- Database ----------
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:AirflowDbPass2026@localhost:5432/airflow

# ---------- API server / UI ----------
AIRFLOW__API__PORT=8080
AIRFLOW__API__BASE_URL=http://s4:8080
AIRFLOW__API__SECRET_KEY=<API_SECRET_KEY>
AIRFLOW__API_AUTH__JWT_SECRET=<JWT_SECRET>

# ---------- DAG files ----------
AIRFLOW__DAG_PROCESSOR__REFRESH_INTERVAL=60

# ---------- Logs ----------
AIRFLOW__LOGGING__BASE_LOG_FOLDER=/opt/airflow/logs
EOF

chown root:airflow /etc/airflow/airflow.env
chmod 640 /etc/airflow/airflow.env
```

### Darek setting kem jaruri che

| Setting | Su kare | Na muko to su thay |
|---|---|---|
| `EXECUTOR=LocalExecutor` | Scheduler pote ek sathe ghana tasks chalave | Tasks ek pachi ek j chale |
| `LOAD_EXAMPLES=False` | Demo DAG load nathi thata | UI 50+ demo DAGs thi bharai jay |
| `FERNET_KEY` | Connections na passwords encrypted save thay | Passwords plain text ma rahe. Key no backup rakho |
| `AUTH_MANAGER=...FabAuthManager` | Username ane password login, roles | Default SimpleAuthManager (dev mate) |
| `EXECUTION_API_SERVER_URL` | Airflow 3 ma task potanu status aa URL par mokle | Port badlo ane aa na badlo to tasks atki ke fail thay |
| `API_AUTH__JWT_SECRET` | Process vachhe tokens sign thay | Darek process potani random key banave, ane tasks `403 InvalidSignatureError` thi fail thay |
| `API__SECRET_KEY` | UI login session | Restart pachi badha users logout thai jay |
| `API__BASE_URL` | UI ane emails ma saacha links | Links `localhost` batave |
| `DAG_PROCESSOR__REFRESH_INTERVAL=60` | Navi DAG 1 minute ma dekhay | 5 minute rah jovi pade |

ETL na DB password ahiya **nathi**, te `/opt/etl/etl.env` ma che (step 4.4).

### 7.3 `airflow` user na shell ma config

```bash
cat >> /opt/airflow/.bashrc <<'EOF'
set -a; source /etc/airflow/airflow.env; set +a
source /opt/airflow/venv/bin/activate
EOF
```
**Kem:** CLI commands pan same DB ane same secrets vapre. Nahi to CLI potanu alag SQLite DB banavi le che.

---

## 8. Database initialize ane admin user

```bash
sudo -iu airflow

airflow db migrate            # Airflow na tables
airflow fab-db migrate        # login, users ane roles na tables

airflow users create \
    --username admin \
    --firstname Admin --lastname User \
    --role Admin \
    --email admin@example.com \
    --password 'ChangeMe@2026'
```
Bija users mate aa roles vaparo:
- `Viewer`: fakt jovu
- `User`: DAG trigger kari shake
- `Op`: connections pan manage kari shake

---

## 9. systemd services

```bash
# root thi
for svc in api-server scheduler dag-processor triggerer; do
cat > /etc/systemd/system/airflow-${svc}.service <<EOF
[Unit]
Description=Airflow ${svc}
After=network-online.target postgresql.service
Wants=network-online.target postgresql.service

[Service]
User=airflow
Group=airflow
EnvironmentFile=/etc/airflow/airflow.env
WorkingDirectory=/opt/airflow
ExecStart=/opt/airflow/venv/bin/airflow ${svc}
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF
done

systemctl daemon-reload
systemctl enable --now airflow-api-server airflow-scheduler airflow-dag-processor airflow-triggerer
systemctl status airflow-api-server airflow-scheduler airflow-dag-processor airflow-triggerer --no-pager
```

| Service | Su kare | Band hoy to |
|---|---|---|
| `airflow-api-server` | UI (8080) + API + Execution API | UI nahi khule, ane tasks status lakhi nahi shake |
| `airflow-scheduler` | Tasks chalave. LocalExecutor ma `run-job.sh` aa service na child process tarike chale | DAG runs "queued" ma j rahe |
| `airflow-dag-processor` | `dags/` parse kare | Navi DAG nahi dekhay |
| `airflow-triggerer` | Deferrable tasks | Aapna tasks par asar nathi |

Logs jova: `journalctl -u airflow-scheduler -f`

---

## 10. Firewall (s4)

```bash
# firewalld (RHEL)
firewall-cmd --permanent --add-rich-rule='rule family=ipv4 source address=<admin-subnet>/24 port port=8080 protocol=tcp accept'
firewall-cmd --permanent --add-rich-rule='rule family=ipv4 source address=<admin-subnet>/24 port port=4040-4045 protocol=tcp accept'
for ip in <s1-ip> <s2-ip> <s3-ip>; do
  firewall-cmd --permanent --add-rich-rule="rule family=ipv4 source address=${ip} port port=7200-7201 protocol=tcp accept"
done
firewall-cmd --reload

# ufw (Ubuntu)
ufw allow from <admin-subnet>/24 to any port 8080 proto tcp
ufw allow from <admin-subnet>/24 to any port 4040:4045 proto tcp
for ip in <s1-ip> <s2-ip> <s3-ip>; do ufw allow from ${ip} to any port 7200:7201 proto tcp; done
```

| s4 par inbound port | Kon thi | Kem |
|---|---|---|
| 8080 | Admin network | Airflow UI |
| 4040–4045 | Admin network | Spark job UI. Fakt job chale tyare khule |
| 7200, 7201 | s1, s2, s3 | Spark driver: master ane executors pacha s4 par connect thay. `spark.jars` pan 7200 thi jay che |
| 22 | Admin | SSH |

**Outbound** (s4 thi bahar): s1:7077, s1–s3:27017, s5:3306, s2–s3:7100–7116.

**8080 busy hoy to:** `AIRFLOW__API__PORT`, `AIRFLOW__API__BASE_URL` ane `AIRFLOW__CORE__EXECUTION_API_SERVER_URL`, **tranye** ma navo port muko. Pachi restart karo ane firewall rule pan badlo.

---

## 11. DAGs: kya ane kevi rite mukva

### 11.1 Kya

**`/opt/airflow/dags/`**. File no owner `airflow`, extension `.py`. Sub-folders chale che.

### 11.2 Kevi rite

| Rit | Kyare |
|---|---|
| `scp file.py airflow@s4:/opt/airflow/dags/` | Shuruat ke testing |
| **Git (salah)**: `dags/` git repo, cron thi `git pull` | Team ma. History ane rollback male |

```bash
sudo -iu airflow
rm -rf /opt/airflow/dags && git clone <your-dags-repo-url> /opt/airflow/dags
(crontab -l 2>/dev/null; echo "* * * * * cd /opt/airflow/dags && git pull -q") | crontab -
```

### 11.3 File mukya pachi su thay

1. `dag-processor` dar 60 second e folder scan kare che.
2. File ma error na hoy to DAG UI ma **paused** state ma aave che, jethi navi DAG pote chalu na thai jay.
3. UI ma toggle on karo, athva `airflow dags unpause <dag_id>` chalavo.

```bash
airflow dags list
airflow dags list-import-errors
```

### 11.4 DAG #1: `hello_check.py` (Airflow ane Java)

```python
import pendulum
from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator

with DAG(
    dag_id="hello_check",
    start_date=pendulum.datetime(2026, 11, 1, tz="Asia/Kolkata"),
    schedule=None,
    catchup=False,
):
    BashOperator(task_id="hello", bash_command="echo Airflow OK && java -version")
```

### 11.5 DAG #2: `connectivity_check.py` (cluster ane DB sudhi network)

```python
import pendulum
from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator

CHECKS = {
    "spark_master": "s1 7077",
    "mongo_s1": "s1 27017",
    "mongo_s2": "s2 27017",
    "mongo_s3": "s3 27017",
    "edbp": "s5 3306",
}

with DAG(
    dag_id="connectivity_check",
    start_date=pendulum.datetime(2026, 11, 1, tz="Asia/Kolkata"),
    schedule=None,
    catchup=False,
):
    for name, target in CHECKS.items():
        BashOperator(task_id=name, bash_command=f"nc -zv -w 5 {target}")
```
**Kem:** ETL chalavta pehla j kayo server pahonchato nathi te alag task (lal rang) ma dekhay che.

### 11.6 DAG #3: `weather_medallion.py` (aapnu ETL)

```python
from datetime import timedelta

import pendulum
from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator

RUN_JOB = "/opt/etl/run-job.sh"


def etl_task(job: str) -> BashOperator:
    """Ek Java job = ek Airflow task. Jar same, fakt job naam alag."""
    return BashOperator(
        task_id=job,
        bash_command=f"{RUN_JOB} {job}",
        execution_timeout=timedelta(minutes=60),
    )


with DAG(
    dag_id="weather_medallion",
    description="Bronze (Mongo) -> Silver -> Gold (EDBP)",
    start_date=pendulum.datetime(2026, 11, 1, tz="Asia/Kolkata"),
    schedule="*/15 * * * *",      # dar 15 minute
    catchup=False,
    max_active_runs=1,            # be run ek sathe nahi, karan watermark ek j che
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=3),
    },
    tags=["etl", "spark"],
):
    etl_task("bronze_to_silver") >> etl_task("silver_to_gold")
```

| Setting | Kem |
|---|---|
| `BashOperator` + `run-job.sh` | Airflow fakt script chalave che. Exit code `0` hoy to success, biju kai pan hoy to fail |
| `max_active_runs=1` | Watermark ek j che, etle be run ek sathe chale to gadbad thay |
| `catchup=False` | Juna chhuta slots nathi chalavvana. Watermark pote baki no data uthavi le che |
| `retries=2` | Temporary error par auto retry. Jobs idempotent che, etle safe |
| `>>` | Job #1 fail thay to Job #2 `upstream_failed` thai ne skip thay |
| `execution_timeout` | Job atki jay to 60 minute pachi kill thay (`exec` ne karane java seedhu band thay) |

**BashOperator ni ek chhupi vaat:** `bash_command` no chhello bhag `.sh` hoy (argument vagar), jem ke `"/opt/etl/check.sh"`, to Airflow ene **Jinja template file** samje che ane `TemplateNotFound` error aape che. Tyare chhelle ek space muko: `"/opt/etl/check.sh "`. Aapna command ma job naam che, etle aa problem nathi.

**Navi job umervi hoy to:**
1. Java ma navo `XyzJob` class banavo.
2. `JobRunner` ma ek `case` umero.
3. Build kari ne jar deploy karo (step 12).
4. DAG ma `etl_task("xyz_job")` umero.

---

## 12. Navu jar version deploy

```bash
# build machine
mvn clean package
scp target/spark-weather-etl-1.1.0.jar airflow@s4:/opt/etl/
scp target/executor-jars/*.jar          airflow@s4:/opt/etl/executor-jars/   # dependency badli hoy to j

# s4 (airflow user)
airflow dags pause weather_medallion          # chalu run puro thava do
ln -sfn /opt/etl/spark-weather-etl-1.1.0.jar /opt/etl/spark-weather-etl.jar
/opt/etl/run-job.sh bronze_to_silver          # optional: haath thi ek vaar test
airflow dags unpause weather_medallion
```
**Rollback** mate symlink juna version par pachi muki do.

---

## 13. Final verification checklist

| # | Check | Command / Kya | Saachu result |
|---|---|---|---|
| 1 | ETL haath thi | `/opt/etl/run-job.sh bronze_to_silver` | exit code 0 |
| 2 | Services | `systemctl is-active airflow-api-server airflow-scheduler airflow-dag-processor airflow-triggerer` | 4 vaar `active` |
| 3 | UI | `http://s4:8080` | Login page |
| 4 | Login | `admin` / step 8 no password | DAGs page |
| 5 | Health | `curl -s http://localhost:8080/api/v2/monitor/health` | Badha `healthy` |
| 6 | DAG parse | `airflow dags list-import-errors` | Khali |
| 7 | `hello_check` | Trigger | Green, Java 17 version |
| 8 | `connectivity_check` | Trigger | Badha 5 tasks green |
| 9 | `weather_medallion` | Unpause + Trigger | Banne green, `etl_job_run` ma 2 navi SUCCESS rows |
| 10 | Spark UI | `http://s1:8080` | App `weather-etl` "Completed" ma |

Pehla login pachi **admin password badli do**.

---

## 14. Rojinda kaam

| Kaam | Command |
|---|---|
| Restart | `systemctl restart airflow-api-server airflow-scheduler airflow-dag-processor airflow-triggerer` |
| Service logs | `journalctl -u airflow-scheduler -n 200` |
| Task (ETL) logs | UI → task → Logs. Java no aakho output ahiya aave che |
| Job haath thi | `airflow dags trigger weather_medallion`, athva seedhu `/opt/etl/run-job.sh <job>` |
| Juna logs saaf (cron) | `find /opt/airflow/logs -type f -mtime +30 -delete` |
| Metadata DB saaf (mahine) | `airflow db clean --clean-before-timestamp "$(date -d '-90 days' +%F)" --yes` |
| DB backup (raat) | `pg_dump -U airflow -h localhost airflow > /backup/airflow_$(date +%F).sql` |

**Backup ma aa vastu rakho:**
- `/etc/airflow/airflow.env`, khas kari ne `FERNET_KEY`.
- `/opt/etl/config/`, `/opt/etl/etl.env` ane `/opt/etl/run-job.sh`.
- Postgres dump.
- `dags/`.

---

## 15. Common problems

| Problem | Karan | Ukel |
|---|---|---|
| `InaccessibleObjectException` / `IllegalAccessError` | `--add-opens` flags nathi | `run-job.sh` thi j chalavo, seedhu `java -jar` nahi |
| Spark log: `Initial job has not accepted any resources` | Cores ke memory ochha che, ke executors s4:7200 sudhi pahonchta nathi | `spark.cores.max` ane `spark.executor.memory` ghatado. s2 thi `nc -zv s4 7200` chalavo. `/etc/hosts` ma `s4` che? |
| Executor par `ClassNotFoundException: com.mongodb...` ke `Failed to find data source: mongodb` | `spark.jars` na path khota che ke file nathi | `ls /opt/etl/executor-jars/` ane properties ma naam sarkha karo |
| `InvalidClassException` / serialVersionUID mismatch | Cluster no Spark version jar thi alag che | Cluster par pan Spark **3.5.1 (Scala 2.12)** rakho |
| Job local ma (`local[*]`) chale che, cluster par nahi | `cluster` profile ke `additional-location` load nathi thayu | Log ma `The following profiles are active: cluster` che ke nahi te juo. Path ne chhelle `/` hovo joie |
| `Access denied for user` (EDBP) | `etl.env` load nathi thayu ke password khoto che | `run-job.sh` ma `source` line, ane file permission |
| Tasks: `InvalidSignatureError` / 403 | `JWT_SECRET` set nathi | Env file ma set karo ane badhi services restart karo |
| DAG nathi dekhati | Parse error ke permission | `airflow dags list-import-errors`, `chown airflow:airflow` |
| Tasks "queued" ma atke | Scheduler band che | `systemctl status airflow-scheduler` |
| `TemplateNotFound: ...sh` | BashOperator `.sh` ne template samje che | Command ne chhelle space muko |
| CLI ma DAGs ke users nathi | Env load nathi thayu (SQLite vaparay che) | `sudo -iu airflow` thi login karo |

---

## Appendix: Jar alag server (s6) par rakhvi hoy to (SSHOperator)

Driver **s6** par chale che. Airflow fakt SSH thi command mokle che.

**s6 par:**
- Step 2 (hosts, `etl` user) ane Step 3 (Java 17) karo.
- Step 4 aakhu karo (`/opt/etl/...`). Properties ma `spark.driver.host=s6` muko.
- Firewall: s6 par 7200 ane 7201 s1–s3 mate kholo, ane 22 s4 mate. s6 thi s1:7077, 27017 ane s5:3306 outbound joie.

**s4 par:**
```bash
pip install apache-airflow-providers-ssh --constraint "${CONSTRAINTS}"     # airflow venv ma

sudo -iu airflow
ssh-keygen -t ed25519 -N "" -f ~/.ssh/id_ed25519
ssh-copy-id etl@s6
ssh etl@s6 "java -version"          # pehli vaar host key accept thay

airflow connections add etl_s6 \
    --conn-type ssh --conn-host s6 --conn-login etl \
    --conn-extra '{"key_file": "/opt/airflow/.ssh/id_ed25519"}'
```

**DAG ma `etl_task` aam badlo:**
```python
from airflow.providers.ssh.operators.ssh import SSHOperator

def etl_task(job: str) -> SSHOperator:
    return SSHOperator(
        task_id=job,
        ssh_conn_id="etl_s6",
        command=f"/opt/etl/run-job.sh {job}",
        cmd_timeout=None,        # default 10 second che: lambi job vachhe j kapai jay
        get_pty=True,            # task kill thay to s6 par java pan band thay
        execution_timeout=timedelta(minutes=60),
    )
```

| Setting | Kem |
|---|---|
| `cmd_timeout=None` | SSH provider no default timeout bahu nano che. Spark job minuto chale che |
| `get_pty=True` | Airflow task kill kare to remote process pan band thay, nahi to s6 par java chaltu rahe |

**Fayda:** Airflow server par ETL ni memory ke load nathi aavto, ane ETL ne alag manage kari shakay che.
**Nuksan:** ek vadhu server, SSH keys sambhalva pade, ane SSH tute to task fail thay.

---

## Sources

- [Airflow: Installation from PyPI (constraints)](https://airflow.apache.org/docs/apache-airflow/stable/installation/installing-from-pypi.html)
- [Airflow 3: Architecture overview](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/overview.html)
- [Airflow 3: JWT token authentication](https://airflow.apache.org/docs/apache-airflow/stable/security/jwt_token_authentication.html)
- [Airflow version support / EOL](https://endoflife.date/api/v1/products/apache-airflow)
