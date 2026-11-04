# Airflow Installation ane Setup (Linux, A to Z)

Aa guide **s4 (Airflow server)** par Airflow ne shunya thi chalu karva mate che. Setup puru thaya pachi:

- Browser ma `http://s4:8080` thi login thai shakase.
- Server reboot thay to pan Airflow automatic chalu thase (systemd).
- Airflow `spark-submit` thi Spark cluster (s1, s2, s3) par ETL jobs chalavse.
- DAG file `/opt/airflow/dags/` ma muko etle te UI ma aavi jase.

> **Pehla:** aa guide **spark-submit client mode** mate che. Driver s4 par chale che. ETL project ma spark-submit vala ferfar (shade jar, `JobRunner`, `SparkConfig` ma `setIfMissing`) pehla thai java joie. Spark cluster (s1, s2, s3) nu setup ane ports mate [CLUSTER_PORTS.md](CLUSTER_PORTS.md) juo.

---

## 0. Su install thase ane kem

```
┌─────────────────────────── s4 (Airflow server) ───────────────────────────┐
│                                                                           │
│  airflow api-server   → UI + REST API (port 8080). Tasks pan aa API thi   │
│                         potanu status lakhe che (Execution API)           │
│  airflow scheduler    → DAG no time thay etle task chalave (LocalExecutor)│
│  airflow dag-processor→ dags/ folder ni .py files vanchi ne DAG banave    │
│  airflow triggerer    → "deferrable" tasks mate (future mate, halka)      │
│                                                                           │
│  PostgreSQL           → Airflow nu metadata DB (DAG runs, task status,    │
│                         connections, users)                               │
│  Java 17 + Spark 3.5.1 client → spark-submit mate (koi daemon nahi)       │
│  /opt/etl/*.jar       → ETL jar                                           │
└───────────────────────────────────────────────────────────────────────────┘
          │ spark-submit (client mode): driver ahiya j chale
          ▼
   s1 Spark Master ──► s2, s3 Workers ──► Mongo (s1-s3), EDBP (s5)
```

| Nirnay | Kem |
|---|---|
| **Airflow 3.x** | Airflow 2 nu support 22 April 2026 e puru thai gayu |
| **LocalExecutor** | Ek j server che. Celery mate Redis ane alag worker server ni jarur nathi |
| **PostgreSQL (s4 par local)** | SQLite ek vaar ma ek j task chalave. Postgres local rakhvathi s5 (EDBP) par bojh nathi padto ane firewall rule ochha joie |
| **venv ma pip install** | OS na Python ne bagadya vagar Airflow alag rahe che. Upgrade karvu saral che |
| **systemd services** | Reboot pachi auto-start, crash thay to auto-restart, logs `journalctl` ma |
| **Ek env file** | Badha 4 process ane CLI ne **same secrets** ane config male. Aa na thay to JWT errors aave che |

### Folder layout

```
/opt/airflow/            AIRFLOW_HOME
├── venv/                Python virtualenv (Airflow ahiya install)
├── dags/                DAG files ahiya mukvani
├── logs/                Task logs
└── plugins/             (optional)
/opt/spark/              Spark 3.5.1 client
/opt/etl/                ETL jar
/etc/airflow/airflow.env Badho config + secrets (ek j jagya)
```

---

## 1. OS taiyari

Badha commands `root` (ke `sudo`) thi chalavo. Ubuntu ane RHEL/Rocky/Alma ma je alag che te banne aapya che.

### 1.1 Update ane jaruri packages

```bash
# Ubuntu 22.04 / 24.04
apt update && apt -y upgrade
apt -y install build-essential libpq-dev curl wget tar netcat-openbsd vim

# RHEL / Rocky / Alma 9
dnf -y update
dnf -y groupinstall "Development Tools"
dnf -y install libpq-devel curl wget tar nmap-ncat vim
```
**Kem:** Airflow ni ghani Python libraries install vakhte compile thay che (gcc). `libpq` Postgres driver mate joie. `nc` thi aagal ports test karishu.

### 1.2 Timezone

```bash
timedatectl set-timezone Asia/Kolkata
timedatectl
```
**Kem:** server no time ane Airflow no `default_timezone` same hoy to schedule ane logs ma time samjvama confusion nathi thatu.

### 1.3 Hostnames (`/etc/hosts`)

```bash
cat >> /etc/hosts <<'EOF'
<s1-ip>  s1
<s2-ip>  s2
<s3-ip>  s3
<s4-ip>  s4
<s5-ip>  s5
EOF
```
**Kem:**
- spark-submit, Mongo replica set ane executors ek bija ne naam thi shodhe che.
- Mongo `rs.conf()` ma je hostnames che te s4 thi resolve thava joie.
- Aa j entries **s1, s2, s3 par pan** hovi joie, jethi workers `s4` naam thi driver sudhi pahonchi shake.

### 1.4 `airflow` user ane folders

```bash
useradd --system --create-home --home-dir /opt/airflow --shell /bin/bash airflow
mkdir -p /opt/airflow/{dags,logs,plugins} /opt/etl /etc/airflow
chown -R airflow:airflow /opt/airflow /opt/etl
```
**Kem:** Airflow `root` thi na chalavvu joie. DAG code sathe chale che, etle alag user thi bhul ke security issue nu nuksan ochhu thay.

---

## 2. Java 17

```bash
# Ubuntu
apt -y install openjdk-17-jre-headless
# RHEL
dnf -y install java-17-openjdk-headless

java -version
dirname $(dirname $(readlink -f $(which java)))   # aa JAVA_HOME che, note karo
```
**Kem:** spark-submit ane aapnu ETL driver JVM ahiya chale che. Version cluster sathe match thavu joie: Java 17, kem ke project `java.version=17` che.

---

## 3. Spark 3.5.1 client

```bash
cd /tmp
wget https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz
tar -xzf spark-3.5.1-bin-hadoop3.tgz -C /opt
ln -s /opt/spark-3.5.1-bin-hadoop3 /opt/spark
chown -R airflow:airflow /opt/spark-3.5.1-bin-hadoop3
```
**Kem:**
- Airflow nu `SparkSubmitOperator` andar thi `spark-submit` command j chalave che.
- **Version cluster jevu j (3.5.1, Scala 2.12)** hovu joie, nahi to driver ane executors vachhe serialization errors aave che.
- Symlink `/opt/spark` rakhvathi upgrade vakhte fakt link badlavi pade che.
- Ahiya **master ke worker start nathi karvana**, fakt binaries joie.

### 3.1 `spark-defaults.conf` (s4 par)

```bash
cat > /opt/spark/conf/spark-defaults.conf <<'EOF'
spark.master                     spark://s1:7077
# executors aa naam thi driver (s4) sudhi pahonche
spark.driver.host                s4
spark.driver.bindAddress         0.0.0.0
# fixed ports, jethi firewall rule lagavi shakay
spark.driver.port                7200
spark.driver.blockManager.port   7201
spark.blockManager.port          7100
spark.port.maxRetries            16
spark.ui.port                    4040
EOF
chown airflow:airflow /opt/spark/conf/spark-defaults.conf
```
**Kem:** dar spark-submit aa file automatic vanche che. Etle ports ane master URL darek DAG ma lakhva nathi padta, ek jagya e j badlavvu pade che.

### 3.2 Cluster sudhi pahonchay che? (test)

```bash
nc -zv s1 7077          # Spark master
nc -zv s1 27017         # Mongo
nc -zv s5 3306          # EDBP (MySQL)
```
Tranye `succeeded` ke `open` batavvu joie. Na batave to network ke firewall team sathe aagal vadho. Aa test pass thaya vagar aagal na steps kaam nahi kare.

---

## 4. PostgreSQL (Airflow metadata DB)

### 4.1 Install

```bash
# Ubuntu
apt -y install postgresql
systemctl enable --now postgresql

# RHEL
dnf -y install postgresql-server
postgresql-setup --initdb
systemctl enable --now postgresql
```

### 4.2 Database ane user

```bash
sudo -u postgres psql <<'EOF'
CREATE USER airflow WITH PASSWORD 'AirflowDbPass2026';
CREATE DATABASE airflow OWNER airflow ENCODING 'UTF8';
EOF
```
**Kem:**
- `OWNER airflow` rakhvathi Postgres 15+ ma `public` schema par permission ni takleef nathi aavti.
- Password ma `@ : / #` jeva characters na rakho, nahi to connection URL ma escape karva pade.

### 4.3 (Fakt RHEL) password login on karo

RHEL default ma `ident` auth vapre che, etle Airflow password thi connect nahi thai shake.
```bash
vim /var/lib/pgsql/data/pg_hba.conf
# aa line ne:   host  all  all  127.0.0.1/32  ident
# aam badlo:    host  all  all  127.0.0.1/32  scram-sha-256
systemctl restart postgresql
```

### 4.4 Test
```bash
psql "postgresql://airflow:AirflowDbPass2026@localhost:5432/airflow" -c "select 1;"
```

---

## 5. Python ane Airflow install

### 5.1 Python 3.12

```bash
# Ubuntu 24.04 (3.12 default che)
apt -y install python3.12 python3.12-venv python3.12-dev
# RHEL 9
dnf -y install python3.12 python3.12-devel
```
**Kem:** Airflow 3 supported Python version mange che. 3.12 sauthi safe choice che. Install karta pehla official docs ma supported versions check karjo.

### 5.2 venv ane Airflow

```bash
sudo -iu airflow           # have badha commands airflow user thi

python3.12 -m venv /opt/airflow/venv
source /opt/airflow/venv/bin/activate
pip install --upgrade pip

AIRFLOW_VERSION=3.3.2      # pypi.org/project/apache-airflow par latest 3.x joi ne set karo
PYTHON_VERSION=3.12
CONSTRAINTS="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

pip install "apache-airflow[postgres]==${AIRFLOW_VERSION}" \
    apache-airflow-providers-apache-spark \
    apache-airflow-providers-fab \
    apache-airflow-providers-standard \
    --constraint "${CONSTRAINTS}"

airflow version
```

| Package | Kem |
|---|---|
| `apache-airflow[postgres]` | Airflow core + Postgres driver (`psycopg2`) |
| `apache-airflow-providers-apache-spark` | `SparkSubmitOperator` + "Spark" connection type |
| `apache-airflow-providers-fab` | Username ane password vado login, roles (Admin, Viewer...). Default `SimpleAuthManager` fakt dev mate che |
| `apache-airflow-providers-standard` | `BashOperator`, `PythonOperator` vagere (test DAG mate) |
| `--constraint` | Airflow ni seknda dependencies **tested versions** par pin thay che. Aa vagar install "chale pan kale tute" evu bane |

---

## 6. Configuration (`/etc/airflow/airflow.env`)

Badho config ek env file ma rakhishu. Airflow `AIRFLOW__<SECTION>__<KEY>` env variables ne `airflow.cfg` karta vadhu priority aape che.

### 6.1 Secrets banavo (ek vaar)

```bash
source /opt/airflow/venv/bin/activate
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"   # FERNET_KEY
openssl rand -hex 32    # JWT_SECRET
openssl rand -hex 32    # API_SECRET_KEY
exit                    # pacha root par
```

### 6.2 Env file (root thi)

```bash
cat > /etc/airflow/airflow.env <<'EOF'
# ---------- Paths ----------
AIRFLOW_HOME=/opt/airflow
JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
SPARK_HOME=/opt/spark
# systemd $PATH expand nathi kartu, etle aakho PATH lakho
PATH=/opt/airflow/venv/bin:/opt/spark/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

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
# navi DAG file ketla second ma dekhay (default 300)
AIRFLOW__DAG_PROCESSOR__REFRESH_INTERVAL=60

# ---------- Logs ----------
AIRFLOW__LOGGING__BASE_LOG_FOLDER=/opt/airflow/logs

# ---------- ETL jar mate (Spring relaxed binding: pipeline.db-password -> PIPELINE_DBPASSWORD) ----------
PIPELINE_DBUSERNAME=etl_user
PIPELINE_DBPASSWORD=<edbp-password>
EOF

chown root:airflow /etc/airflow/airflow.env
chmod 640 /etc/airflow/airflow.env
```

`JAVA_HOME` ma step 2 ma note karelo path muko. RHEL par te mostly `/usr/lib/jvm/jre-17` hoy che.

### Darek setting kem jaruri che

| Setting | Su kare | Na muko to su thay |
|---|---|---|
| `EXECUTOR=LocalExecutor` | Scheduler pote ek sathe ghana tasks chalave | Default SQLite + sequential mode ma ek vaar ma ek j task chale |
| `LOAD_EXAMPLES=False` | Airflow na 50+ demo DAG load nathi thata | UI demo DAGs thi bharai jay |
| `FERNET_KEY` | Connections na passwords DB ma **encrypted** save thay | Passwords plain text ma rahe. Key badlay to juna passwords vanchata nathi, etle key no backup rakho |
| `AUTH_MANAGER=...FabAuthManager` | Username ane password login, users ane roles | Default SimpleAuthManager (fakt dev mate) |
| `EXECUTION_API_SERVER_URL` | Airflow 3 ma task chale tyare potanu status aa URL par API server ne mokle che | Port badlo ane aa na badlo to tasks "running" ma atki jay ke fail thay |
| `API_AUTH__JWT_SECRET` | Scheduler ane API server vachhe tokens sign thay che | Darek process potani random key banave, ane tasks `403 InvalidSignatureError` thi fail thay |
| `API__SECRET_KEY` | UI login session sign thay | Restart pachi badha users logout thai jay |
| `API__BASE_URL` | UI ane emails ma links saacha bane | Links `localhost` batave |
| `DAG_PROCESSOR__REFRESH_INTERVAL=60` | Navi DAG file 1 minute ma UI ma aave | 5 minute rah jovi pade |
| `PATH` ma `/opt/spark/bin` | `SparkSubmitOperator` ne `spark-submit` male | `FileNotFoundError: spark-submit` |
| `PIPELINE_DB*` | ETL jar ne DB credentials env thi male, jar ke DAG ma password nathi lakhvo padto | Credentials jar ni properties ma hardcode rahe |

### 6.3 `airflow` user na shell ma pan aa config

```bash
cat >> /opt/airflow/.bashrc <<'EOF'
set -a; source /etc/airflow/airflow.env; set +a
source /opt/airflow/venv/bin/activate
EOF
```
**Kem:** `airflow` CLI commands (users, connections, dags) pan **same DB ane same secrets** vapre. Nahi to CLI juda SQLite DB banavi le che.

---

## 7. Database initialize ane admin user

```bash
sudo -iu airflow

airflow db migrate            # Airflow na badha tables Postgres ma banave
airflow fab-db migrate        # login, users ane roles na tables (FAB)

airflow users create \
    --username admin \
    --firstname Admin --lastname User \
    --role Admin \
    --email admin@example.com \
    --password 'ChangeMe@2026'
```
**Kem:** `db migrate` khali DB ma schema banave che. Upgrade vakhte pan aa j command juna schema ne navu kare che.

Bija users mate pan aa j command vaparo, sathe yogya role:
- `Viewer`: fakt jovu
- `User`: DAG trigger kari shake
- `Op`: connections pan manage kari shake

---

## 8. systemd services (auto-start)

Char services banavishu. Badhi same env file vapre che.

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
| `airflow-api-server` | UI (8080) + REST API + Execution API | UI nahi khule, ane tasks status lakhi nahi shake |
| `airflow-scheduler` | Time thay etle tasks chalave (LocalExecutor) | DAG runs "queued" ma j rahe |
| `airflow-dag-processor` | `dags/` folder parse kare | Navi ke badleli DAG UI ma nahi dekhay |
| `airflow-triggerer` | Deferrable tasks (sensors vagere) | Aapna Spark tasks par asar nathi, pan future mate chalu rakho |

**Kem systemd:** `Restart=always` thi crash thay to 10 second ma fari chalu thay che, ane `enable` thi reboot pachi pan chalu rahe che.

Logs jova:
```bash
journalctl -u airflow-scheduler -f
```

---

## 9. Firewall (s4)

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
| 8080 | Admin/office network | Airflow UI |
| 4040–4045 | Admin network | Spark job UI. Fakt job chale tyare j khule |
| 7200, 7201 | s1, s2, s3 | Spark driver: executors ane master pacha s4 par connect thay |
| 22 | Admin | SSH |

Postgres (5432) ane Execution API **local** che, etle bahar kholvani jarur nathi.

**Outbound** (s4 thi bahar): s1:7077, s1–s3:27017, s5:3306, s2–s3:7100–7116. Biji vigat mate [CLUSTER_PORTS.md](CLUSTER_PORTS.md) juo. Aa jagya e driver s6 ni badle s4 par che.

**8080 busy hoy to:** `AIRFLOW__API__PORT`, `AIRFLOW__API__BASE_URL` ane `AIRFLOW__CORE__EXECUTION_API_SERVER_URL`, **tranye** ma navo port muko. Pachi `systemctl restart airflow-*` karo ane firewall rule pan badlo.

---

## 10. Spark connection (Airflow ma)

```bash
sudo -iu airflow
airflow connections add spark_cluster \
    --conn-type spark \
    --conn-host spark://s1 \
    --conn-port 7077 \
    --conn-extra '{"deploy-mode": "client", "spark-binary": "spark-submit"}'

airflow connections get spark_cluster
```
**Kem:** DAG ma master URL lakhvani jagya `conn_id="spark_cluster"` lakhie chie. Master badlay to fakt connection badlavvu pade, DAG nahi. UI ma pan **Admin → Connections** ma dekhase.

---

## 11. ETL jar deploy

```bash
# tamara machine thi (build pachi)
scp target/spark-weather-etl-1.0.0.jar airflow@s4:/opt/etl/

# s4 par
ls -l /opt/etl/
```
**Kem:** client mode ma jar fakt s4 par joie. spark-submit ene executors (s2, s3) ne pote mokle che, etle workers par copy nathi karvani.

Navu version deploy karvu hoy to:
- Same naam thi file replace karo.
- Aagal no run chalto hoy to te puro thava do. UI ma DAG ne **pause** kari, copy kari, pachi unpause karo.

---

## 12. DAGs: kya ane kevi rite mukva

### 12.1 Kya

**`/opt/airflow/dags/`** (je `AIRFLOW__CORE__DAGS_FOLDER` che).
- File no owner `airflow` hovo joie ane te `.py` hovi joie.
- Sub-folders chale che.
- `dags/` folder Python path ma hoy che, etle common helper file (jem ke `dags/common/etl.py`) import kari shakay che.

### 12.2 Kevi rite

| Rit | Kyare |
|---|---|
| `scp file.py airflow@s4:/opt/airflow/dags/` | Shuruat ma ke testing ma |
| **Git (salah)**: `dags/` ne git repo banavo, ane cron thi dar minute `git pull` | Team kaam kare tyare. History ane rollback male che |

Git rit:
```bash
sudo -iu airflow
rm -rf /opt/airflow/dags && git clone <your-dags-repo-url> /opt/airflow/dags
(crontab -l 2>/dev/null; echo "* * * * * cd /opt/airflow/dags && git pull -q") | crontab -
```

### 12.3 File mukya pachi su thay

1. `dag-processor` dar 60 second e folder scan kare che (`REFRESH_INTERVAL`).
2. File ma error na hoy to DAG UI ma aave che, pan **paused** state ma. Aa Airflow no default che, jethi navi DAG pote chalu na thai jay.
3. UI ma toggle on karo, athva `airflow dags unpause <dag_id>` chalavo.

Check karva:
```bash
airflow dags list                  # DAG dekhay che?
airflow dags list-import-errors    # Python error hoy to ahiya dekhase
```

### 12.4 DAG #1: `hello_check.py` (Airflow chale che ke nahi)

```python
import pendulum
from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator

with DAG(
    dag_id="hello_check",
    start_date=pendulum.datetime(2026, 11, 1, tz="Asia/Kolkata"),
    schedule=None,          # fakt haath thi trigger
    catchup=False,
):
    BashOperator(task_id="hello", bash_command="echo Airflow OK && java -version && spark-submit --version")
```
**Kem:** Spark na pehla Airflow nu potanu kaam check thay che (scheduler, executor, logs, Java, PATH).

### 12.5 DAG #2: `spark_pi_check.py` (Spark cluster sudhi pahonchay che ke nahi)

```python
import pendulum
from airflow.sdk import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG(
    dag_id="spark_pi_check",
    start_date=pendulum.datetime(2026, 11, 1, tz="Asia/Kolkata"),
    schedule=None,
    catchup=False,
):
    SparkSubmitOperator(
        task_id="spark_pi",
        conn_id="spark_cluster",
        application="/opt/spark/examples/jars/spark-examples_2.12-3.5.1.jar",
        java_class="org.apache.spark.examples.SparkPi",
        application_args=["20"],
        executor_memory="1g",
        total_executor_cores=2,
    )
```
**Kem:** Spark nu potanu example che, emaa aapna code ke DB ni jarur nathi. Aa pass thay etle samjo ke ports, driver host ane cluster badhu barobar che. Log ma `Pi is roughly 3.14...` aavvu joie.

### 12.6 DAG #3: `weather_medallion.py` (aapnu ETL)

```python
from datetime import timedelta

import pendulum
from airflow.sdk import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

ETL_JAR = "/opt/etl/spark-weather-etl-1.0.0.jar"
MAIN_CLASS = "com.de.spark.SparkEtlApplication"


def etl_task(job: str) -> SparkSubmitOperator:
    """Ek Java job = ek Airflow task. Jar ane class same, fakt --pipeline.job alag."""
    return SparkSubmitOperator(
        task_id=job,
        conn_id="spark_cluster",
        application=ETL_JAR,
        java_class=MAIN_CLASS,
        name=f"weather_etl_{job}",
        driver_memory="2g",
        executor_memory="4g",
        executor_cores=2,
        total_executor_cores=4,
        application_args=[
            "--spring.profiles.active=cluster",
            "--pipeline.scheduler.enabled=false",
            f"--pipeline.job={job}",
        ],
        execution_timeout=timedelta(minutes=60),
    )


with DAG(
    dag_id="weather_medallion",
    description="Bronze (Mongo) -> Silver -> Gold (EDBP)",
    start_date=pendulum.datetime(2026, 11, 1, tz="Asia/Kolkata"),
    schedule="*/15 * * * *",      # dar 15 minute
    catchup=False,                # juna chhuta runs bharva nahi
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
| `max_active_runs=1` | Banne jobs watermark par chale che. Be run ek sathe chale to ek bija nu kaam bagade |
| `catchup=False` | Airflow bandh rahyu hoy to pachi juna badha slots chalavva na bese. Watermark pote j baki no data uthavi le che |
| `retries=2` | Network ke DB no temporary issue hoy to auto retry. Jobs idempotent che, etle safe che |
| `>>` | Job #1 fail thay to Job #2 `upstream_failed` thai ne skip thay |
| `execution_timeout` | Job atki jay to 60 minute pachi spark-submit kill thay, ane next run block nathi thato |

**Navi job umervi hoy to:**
1. Java ma navo `XyzJob` class banavo.
2. `JobRunner` ma ek `case` umero.
3. Jar build kari ne deploy karo.
4. DAG ma `etl_task("xyz_job")` umero.

Jar ek j rahe che.

---

## 13. Final verification checklist

| # | Check | Command / Kya | Saachu result |
|---|---|---|---|
| 1 | Services chale che | `systemctl is-active airflow-api-server airflow-scheduler airflow-dag-processor airflow-triggerer` | 4 vaar `active` |
| 2 | UI khule che | Browser: `http://s4:8080` | Login page |
| 3 | Login | `admin` / step 7 no password | DAGs page |
| 4 | Health | `curl -s http://localhost:8080/api/v2/monitor/health` | Badha `healthy` |
| 5 | DAG parse | `airflow dags list-import-errors` | Khali |
| 6 | `hello_check` | UI → Trigger | Green, log ma Java ane Spark version |
| 7 | `spark_pi_check` | UI → Trigger | Green, `Pi is roughly 3.14` |
| 8 | Spark UI | `http://s1:8080` (master) | `spark_pi_check` app "Completed" ma |
| 9 | `weather_medallion` | Unpause + Trigger | Banne tasks green, `etl_job_run` table ma 2 navi rows |

Pehla login pachi **admin password badli do**: UI → Your profile → Reset password.

---

## 14. Rojinda kaam (operations)

| Kaam | Command |
|---|---|
| Restart badhu | `systemctl restart airflow-api-server airflow-scheduler airflow-dag-processor airflow-triggerer` |
| Service logs | `journalctl -u airflow-scheduler -n 200` |
| Task logs | UI → task → Logs, athva `/opt/airflow/logs/dag_id=.../` |
| DAG haath thi chalavvi | `airflow dags trigger weather_medallion` |
| Juna logs saaf (cron, dar raat) | `find /opt/airflow/logs -type f -mtime +30 -delete` |
| Metadata DB saaf (mahine ek vaar) | `airflow db clean --clean-before-timestamp "$(date -d '-90 days' +%F)" --yes` |
| DB backup (dar raat) | `pg_dump -U airflow -h localhost airflow > /backup/airflow_$(date +%F).sql` |

**Backup ma aa vastu rakho:**
- `/etc/airflow/airflow.env`, khas kari ne `FERNET_KEY`. E gaya pachi save karela connections na password pacha nathi vanchata.
- Postgres dump.
- `dags/` (git ma hoy to te pote backup che).

### Airflow upgrade

1. Badhi services stop karo.
2. Postgres no backup lo.
3. Navu `AIRFLOW_VERSION` ane constraints sathe `pip install ...` fari chalavo.
4. `airflow db migrate` ane `airflow fab-db migrate` chalavo.
5. Services start karo.

---

## 15. Common problems

| Problem | Karan | Ukel |
|---|---|---|
| Tasks fail: `InvalidSignatureError` / 403 | `JWT_SECRET` set nathi ke process vachhe alag che | Env file ma `AIRFLOW__API_AUTH__JWT_SECRET` set karo ane badhi services restart karo |
| DAG UI ma nathi dekhati | Parse error, file permission, ke 60 second thaya nathi | `airflow dags list-import-errors`, `chown airflow:airflow`, ane `journalctl -u airflow-dag-processor` |
| Tasks "queued" ma atke | Scheduler band che | `systemctl status airflow-scheduler` |
| Task "running" pachi fail ke atke | `EXECUTION_API_SERVER_URL` khotu che | API server na port sathe match karo |
| `spark-submit: not found` | PATH ma `/opt/spark/bin` nathi | Env file no `PATH`, ane restart |
| Spark log: `Initial job has not accepted any resources` | Workers pase cores ke memory ochha che, ke executors driver (s4:7200) sudhi pahonchta nathi | `total_executor_cores` ane `executor_memory` ghatado. s1–s3 thi `nc -zv s4 7200` chalavo. `/etc/hosts` ma `s4` che? |
| Spark log: `Connection refused ... :7077` | s4 thi master sudhi network nathi | `nc -zv s1 7077`, ane s1 par firewall |
| UI login pachi turant logout | `API__SECRET_KEY` set nathi | Env file ma set karo, restart |
| `airflow` CLI ma users ke DAGs nathi | CLI SQLite vapre che, env load nathi thayu | `sudo -iu airflow` thi login karo (`.bashrc` env load kare che) |
| `password authentication failed` (Postgres) | RHEL ma `ident` auth | Step 4.3 |

---

## Sources

- [Airflow: Installation from PyPI (constraints)](https://airflow.apache.org/docs/apache-airflow/stable/installation/installing-from-pypi.html)
- [Airflow 3: Architecture overview (dag-processor jaruri)](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/overview.html)
- [Airflow 3: JWT token authentication (`jwt_secret`)](https://airflow.apache.org/docs/apache-airflow/stable/security/jwt_token_authentication.html)
- [Airflow version support / EOL](https://endoflife.date/api/v1/products/apache-airflow)
