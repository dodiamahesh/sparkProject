# Apache Spark Standalone Cluster Setup: 1 Master + 2 Workers (Linux, A to Z, spark-submit sathe)

Aa guide **s1 (master), s2 ane s3 (workers)** par Spark cluster banavva mate che. Sathe **s4 (Airflow server)** par Spark client pan install karishu, jethi tya thi `spark-submit` thi ETL jobs chalavi shakay. Setup puru thaya pachi:

- `http://s1:8080` par master UI ma **2 ALIVE workers** dekhase.
- Reboot pachi master ane workers automatic chalu thase (systemd).
- s4 thi `spark-submit` **client mode** (salah) ke **cluster mode** ma jobs chalavi shakase.

> Related: spark-submit vagar ni rit mate [SPARK_CLUSTER_SETUP.md](SPARK_CLUSTER_SETUP.md) juo. Airflow mate [AIRFLOW_SETUP.md](AIRFLOW_SETUP.md) (SparkSubmitOperator) juo. Ports ni list [CLUSTER_PORTS.md](CLUSTER_PORTS.md) ma che.

---

## 0. Overview

```
        s4: Airflow + Spark CLIENT (spark-submit) + ETL jar
            client mode → DRIVER ahiya (s4)
                          │ spark://s1:7077
          ┌───────────────▼───────────────┐
          │ s1: Spark Master  + MongoDB   │
          └───────┬───────────────┬───────┘
      ┌───────────▼─────┐   ┌─────▼───────────┐
      │ s2: Worker + Mongo│ │ s3: Worker + Mongo│  executors
      │ (cluster mode ma  │ │  ane driver pan   │
      │  driver ahiya)    │ │  ahiya aavi shake)│
      └───────────┬─────┘   └─────┬───────────┘
                  └──── s5 (EDBP / RDBMS) ────┘
```

| Component | Kya | Su kare |
|---|---|---|
| **Master** | s1 | Workers no hisab rakhe che, ane apps ne cores ane memory vahenche che |
| **Worker** | s2, s3 | Cores ane RAM offer kare che, ane executors (ane cluster mode ma driver pan) start kare che |
| **Executor** | s2, s3 | Asli data processing |
| **Driver** | Client mode: **s4**. Cluster mode: **s2 ke s3** | Job plan, task scheduling, merge, watermark ane audit |
| **spark-submit** | s4 | Classpath (`$SPARK_HOME/jars` + app jar), Java 17 flags, ane config taiyar kari ne driver start kare |

### Client ke Cluster mode?

| | **Client mode (salah)** | Cluster mode |
|---|---|---|
| Driver kya | s4 | s2 ke s3 (master nakki kare) |
| Jar kya joie | Fakt s4 | **s2 ane s3 banne par same path** par. Standalone mode jar upload nathi kartu |
| Logs | spark-submit na output ma, etle Airflow task log ma seedha | Worker na `driver-*` folder ma. Airflow ma nathi dekhata |
| Job puri thay tyare spark-submit | Exit code sathe pachu aave | Default ma submit karta j exit thay. `waitAppCompletion=true` joie |
| Worker resources | Fakt executors vapre | Driver pan worker na cores ane RAM vapre |
| s4 par RAM | Driver mate ~2 GB | Ochhi joie |

Atyare ek ETL ane ek s4 che, etle **client mode** saral che ane debug karvo saral che. Cluster mode nu setup step 7 ma optional che.

### Versions (s1, s2, s3, **s4**: exactly same)

| | Version | Kem |
|---|---|---|
| Spark | **3.5.1** (Hadoop 3 build) | spark-submit ane cluster same version hova joie |
| Scala | **2.12** | Mongo connector `_2.12` che |
| Java | **17** | Project `java.version=17` che |

### Project ma jaruri ferfar (spark-submit mate)

1. **pom.xml:**
   - `spark-sql` ne `provided` scope ma karo.
   - `maven-shade-plugin` thi flat jar banavo, ane `spring-boot-maven-plugin` kadho.
   - `<start-class>` property set karo.
   - `spring-boot-starter-logging` ne exclude karo.
2. **SparkConfig:** `setMaster(...)` ni jagya `setIfMissing(...)` vaparo, jethi spark-submit nu `--master` ane `--conf` override na thay.
3. **`JobRunner`, scheduler switch ane exit code:** `--pipeline.job=<naam>` thi ek job chale ane exit thay.

Shade jar ma Mongo connector ane MySQL driver andar hoy che. Spark aakhi app jar executors ne aape che, etle **`spark.jars` ke `executor-jars/` ni jarur nathi**.

---

## 1. Badha 4 server par (s1, s2, s3, s4)

Badha commands `root` (ke `sudo`) thi chalavo. Ubuntu ane RHEL/Rocky/Alma ma je alag che te banne aapya che.

### 1.1 Packages ane time sync

```bash
# Ubuntu
apt update && apt -y upgrade
apt -y install curl wget tar netcat-openbsd vim chrony
systemctl enable --now chrony

# RHEL
dnf -y update
dnf -y install curl wget tar nmap-ncat vim chrony
systemctl enable --now chronyd

timedatectl set-timezone Asia/Kolkata
```
**Kem:** badha server no time same hovo joie. Logs no order, Mongo replica set ane ETL nu watermark badhu time par aadhar rakhe che.

### 1.2 `/etc/hosts`

```bash
cat >> /etc/hosts <<'EOF'
<s1-ip>  s1
<s2-ip>  s2
<s3-ip>  s3
<s4-ip>  s4
<s5-ip>  s5
EOF
```
**Ubuntu par khas check karo:** `127.0.1.1  <potanu-hostname>` vali line **kadhi nakho**.

**Kem:** Ubuntu server nu naam `127.0.1.1` par map kare che. Etle Spark daemon ke driver bija servers ne potanu address `127.0.1.1` kahe, ane tya koi connect nathi thai shaktu. Aa sauthi common setup bhul che.

```bash
getent hosts s1 s2 s3 s4 s5     # badha ma real IP, 127.x nahi
```

### 1.3 `spark` user

```bash
useradd --system --create-home --home-dir /opt/spark-home --shell /bin/bash spark
```
**Kem:** daemons `root` thi na chalavva joie. s4 par spark-submit `airflow` user chalavse. Tene `/opt/spark` vanchva ni permission joie, je default ma male che.

### 1.4 Java 17

```bash
# Ubuntu
apt -y install openjdk-17-jre-headless
# RHEL
dnf -y install java-17-openjdk-headless

java -version
dirname $(dirname $(readlink -f $(which java)))    # JAVA_HOME, note karo
```

### 1.5 Spark download ane install

```bash
cd /tmp
wget https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz
wget https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz.sha512

sha512sum spark-3.5.1-bin-hadoop3.tgz
cat spark-3.5.1-bin-hadoop3.tgz.sha512        # banne hash sarkha hova joie

tar -xzf spark-3.5.1-bin-hadoop3.tgz -C /opt
ln -sfn /opt/spark-3.5.1-bin-hadoop3 /opt/spark
chown -R spark:spark /opt/spark-3.5.1-bin-hadoop3
```
**Kem:**
- Checksum thi khabar pade ke file adhuri ke badleli nathi.
- Symlink rakhvathi upgrade vakhte fakt link badlavvi pade.
- s4 par aa j install **client** tarike kaam kare che. Tya koi daemon start nathi karvano.

### 1.6 Data folders

```bash
mkdir -p /data/spark/{logs,run,work,tmp,recovery,events}
chown -R spark:spark /data/spark
```

| Folder | Kya vaparay | Su rahe |
|---|---|---|
| `logs` | s1, s2, s3 | Master ane worker daemon logs |
| `run` | s1, s2, s3 | PID files (default `/tmp` hoy, je OS saaf kari nakhe che) |
| `work` | s2, s3 | Darek app nu folder: executor ane driver na `stdout`/`stderr`, ane app jar ni copy |
| `tmp` | s2, s3 (s4 pan) | Shuffle ane spill files. Fast disk par rakho |
| `recovery` | s1 | Master restart pachi state |
| `events` | s4 | Event logs (History Server, step 8) |

s4 par `events` folder `airflow` user ne lakhva devu pade: `chown -R airflow:airflow /data/spark/events` (step 4 pachi).

### 1.7 Environment (shell mate)

```bash
cat > /etc/profile.d/spark.sh <<'EOF'
export SPARK_HOME=/opt/spark
export PATH=$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH
EOF
```

---

## 2. Master (fakt s1)

### 2.1 `/opt/spark/conf/spark-env.sh`

```bash
cat > /opt/spark/conf/spark-env.sh <<'EOF'
JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64     # RHEL: /usr/lib/jvm/jre-17

SPARK_MASTER_HOST=s1
SPARK_LOCAL_HOSTNAME=s1
SPARK_MASTER_PORT=7077
SPARK_MASTER_WEBUI_PORT=8080          # busy hoy to 9090 vagere

SPARK_DAEMON_MEMORY=1g
SPARK_LOG_DIR=/data/spark/logs
SPARK_PID_DIR=/data/spark/run

# restart pachi workers ane apps ni yaad pachi aave
# defaultCores: spark.cores.max vagar ni app aakho cluster na lai le
SPARK_MASTER_OPTS="-Dspark.deploy.recoveryMode=FILESYSTEM -Dspark.deploy.recoveryDirectory=/data/spark/recovery -Dspark.deploy.defaultCores=4"
EOF
chown spark:spark /opt/spark/conf/spark-env.sh
```

| Setting | Su kare | Na muko to |
|---|---|---|
| `SPARK_MASTER_HOST=s1` | Master `s1` par listen kare | `127.0.1.1` par bind thay, ane workers connect nathi thai shakta |
| `SPARK_MASTER_PORT=7077` | Workers ane spark-submit aa port par aave | Default 7077 j che, pan lakhvathi saaf rahe |
| `SPARK_MASTER_WEBUI_PORT` | Master UI | 8080 busy hoy to Spark 8081 lai le, je worker UI sathe confuse thay |
| `recoveryMode=FILESYSTEM` | Master restart pachi state paachi aave | Restart pachi chalu apps tute |
| `defaultCores=4` | Limit vagar ni app max 4 cores | Ek app badha cores lai le, ane biji app "WAITING" ma rahe |

**Cluster mode Airflow sathe vaparvo hoy to j** (step 7): `SPARK_MASTER_OPTS` ma `-Dspark.master.rest.enabled=true -Dspark.master.rest.port=6066` umero.

### 2.2 systemd service

```bash
cat > /etc/systemd/system/spark-master.service <<'EOF'
[Unit]
Description=Apache Spark Master
After=network-online.target
Wants=network-online.target

[Service]
User=spark
Group=spark
Environment=SPARK_HOME=/opt/spark
Environment=SPARK_NO_DAEMONIZE=true
ExecStart=/opt/spark/sbin/start-master.sh
Restart=always
RestartSec=10
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable --now spark-master
systemctl status spark-master --no-pager
```
**Kem:**
- `SPARK_NO_DAEMONIZE=true`: script java ne foreground ma chalave, jethi systemd process track kari shake ane crash par restart kari shake.
- `LimitNOFILE`: shuffle vakhte ghani files ek sathe khuli shake.

Check:
```bash
nc -zv s1 7077
curl -s http://s1:8080 | grep -o "Spark Master at spark://[^<]*"
```

---

## 3. Workers (s2 ane s3)

### 3.1 Memory ane cores nu planning (Mongo same server par che)

| Server RAM | OS | Mongo (cache + overhead) | **SPARK_WORKER_MEMORY** |
|---|---|---|---|
| 16 GB | 2 GB | 4 GB cache + ~2 GB = 6 GB | **8g** |
| 32 GB | 3 GB | 8 GB cache + ~3 GB = 11 GB | **18g** |
| 64 GB | 4 GB | 16 GB cache + ~4 GB = 20 GB | **40g** |

**Mongo ni cache limit** (s1, s2, s3 na `/etc/mongod.conf`):
```yaml
storage:
  wiredTiger:
    engineConfig:
      cacheSizeGB: 4
```
Pachi `systemctl restart mongod` chalavo, replica set na **ek pachi ek** member par.

**Kem:** Mongo default ma aashare adhi RAM cache mate lai le che. Spark pan RAM le, to OOM killer Mongo ke executor ne mari nakhe.

**Cores:** `SPARK_WORKER_CORES` = CPU cores − 1 ke 2. Cluster mode vaparo to driver mate pan 1 core gano.

### 3.2 `/opt/spark/conf/spark-env.sh` (s2 par, s3 par naam badli ne)

```bash
cat > /opt/spark/conf/spark-env.sh <<'EOF'
JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

SPARK_LOCAL_HOSTNAME=s2              # s3 par: s3
SPARK_WORKER_CORES=6
SPARK_WORKER_MEMORY=8g               # step 3.1
SPARK_WORKER_PORT=7078               # default random, fixed karvo jaruri
SPARK_WORKER_WEBUI_PORT=8081

SPARK_WORKER_DIR=/data/spark/work
SPARK_LOCAL_DIRS=/data/spark/tmp
SPARK_LOG_DIR=/data/spark/logs
SPARK_PID_DIR=/data/spark/run
SPARK_DAEMON_MEMORY=1g

# juna app ane driver folders 7 divas pachi saaf, dar 30 minute check
SPARK_WORKER_OPTS="-Dspark.worker.cleanup.enabled=true -Dspark.worker.cleanup.interval=1800 -Dspark.worker.cleanup.appDataTtl=604800"
EOF
chown spark:spark /opt/spark/conf/spark-env.sh
```

| Setting | Su kare | Na muko to |
|---|---|---|
| `SPARK_LOCAL_HOSTNAME=s2` | Worker potanu naam `s2` kahe. **Cluster mode ma driver pan aa naam vapre** | Khotu naam ke `127.0.1.1` jay |
| `SPARK_WORKER_CORES` / `MEMORY` | Spark ne ketla resources aapva | Badhu Spark lai le, ane Mongo sathe takrav thay |
| `SPARK_WORKER_PORT=7078` | Master aa port thi executor ke driver launch kare | Random port, ane firewall block kare |
| `SPARK_WORKER_DIR` / `LOCAL_DIRS` | Logs, jars ane shuffle | `/opt/spark/work` ane `/tmp` bharai jay |
| `worker.cleanup.*` | Juna app folders delete | Dar run e jar copy thay, ane disk divaso ma bharai jay |

### 3.3 systemd service

```bash
cat > /etc/systemd/system/spark-worker.service <<'EOF'
[Unit]
Description=Apache Spark Worker
After=network-online.target
Wants=network-online.target

[Service]
User=spark
Group=spark
Environment=SPARK_HOME=/opt/spark
Environment=SPARK_NO_DAEMONIZE=true
ExecStart=/opt/spark/sbin/start-worker.sh spark://s1:7077
Restart=always
RestartSec=15
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable --now spark-worker
systemctl status spark-worker --no-pager
```
**Kem `Restart=always`:** master modo chalu thay to worker thoda prayaso pachi exit thai jay che. systemd ene fari chalu kare, etle server kaya order ma chalu thay te mahatvanu nathi rahetu.

**Passwordless SSH ke `conf/workers` file ni jarur nathi.** Te fakt `start-all.sh` mate che.

---

## 4. Spark client (fakt s4)

Step 1 (1.1–1.7) s4 par pan karyu che. Have spark-submit mate config karo.

### 4.1 `/opt/spark/conf/spark-env.sh` (s4)

```bash
cat > /opt/spark/conf/spark-env.sh <<'EOF'
JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
# client mode ma driver aa naam thi executors ne potanu address aape
SPARK_LOCAL_HOSTNAME=s4
SPARK_LOCAL_DIRS=/data/spark/tmp
EOF
```
**Kem:**
- `spark-submit` start thata pehla aa file vanche che.
- `spark.driver.host=s4` ne `spark-defaults.conf` ma **na** muko. Cluster mode ma driver worker par jay tyare e setting khotu thai jay. `SPARK_LOCAL_HOSTNAME` darek server par potanu saachu naam aape che.

### 4.2 `/opt/spark/conf/spark-defaults.conf` (s4)

```bash
cat > /opt/spark/conf/spark-defaults.conf <<'EOF'
spark.master                                spark://s1:7077

# fixed ports (firewall). Cluster mode ma aa j ports worker par vaparay
spark.driver.port                           7200
spark.driver.blockManager.port              7201
spark.blockManager.port                     7100
spark.port.maxRetries                       16
spark.ui.port                               4040

# default executor size (spark-submit flags thi override thay)
spark.executor.memory                       4g
spark.executor.cores                        2
spark.cores.max                             4

# executor logs rotate, jethi worker ni disk na bharay
spark.executor.logs.rolling.strategy        size
spark.executor.logs.rolling.maxSize         104857600
spark.executor.logs.rolling.maxRetainedFiles 5

# History Server mate (step 8). Na joitu hoy to aa 2 lines kadho
spark.eventLog.enabled                      true
spark.eventLog.dir                          file:///data/spark/events
EOF
chown spark:spark /opt/spark/conf/spark-defaults.conf
```

| Setting | Kem |
|---|---|
| `spark.master` | Darek command ma `--master` lakhvu nathi padtu |
| `spark.driver.port`, `blockManager.port` | Default random hoy che. Fixed rakhvathi firewall rule lagavi shakay |
| `spark.executor.*`, `spark.cores.max` | **`SPARK_WORKER_MEMORY` ane `SPARK_WORKER_CORES` ma samavva joie**, nahi to app WAITING ma rahe |
| `spark.executor.logs.rolling.*` | Lamba jobs na logs GB ma na jay |
| `spark.eventLog.*` | Puri thayela jobs na UI pachi pan jova male (step 8) |

**Settings ni priority (sauthi vadhu pehla):**
1. spark-submit na flags (`--executor-memory`, `--conf`)
2. `spark-defaults.conf`
3. App code. `setIfMissing` ne karane code fakt khoti value j bhare che

Java 17 na `--add-opens` flags spark-submit **driver ane executors banne ne pote aape che**, etle tamare lakhva nathi padta.

### 4.3 ETL jar ane config (s4)

```bash
mkdir -p /opt/etl/config
cp spark-weather-etl-1.0.0.jar /opt/etl/       # build machine thi scp
ln -sfn /opt/etl/spark-weather-etl-1.0.0.jar /opt/etl/spark-weather-etl.jar

cat > /opt/etl/config/application-cluster.properties <<'EOF'
pipeline.mongo-uri=mongodb://s1:27017,s2:27017,s3:27017/?replicaSet=rs0
pipeline.silver-jdbc-url=jdbc:mysql://s5:3306/de_silver
pipeline.gold-jdbc-url=jdbc:mysql://s5:3306/de_gold
pipeline.db-username=etl_user
pipeline.db-password=<edbp-password>
EOF
chown -R airflow:airflow /opt/etl
chmod 640 /opt/etl/config/application-cluster.properties
```
**Kem:**
- Ahiya **fakt `pipeline.*`** che. Spark ni badhi settings `spark-defaults.conf` ma che.
- Spring `--spring.config.additional-location` thi aa file load kare che.
- Password file ma rakhvathi client ane cluster banne mode ma e j rit kaam kare che. Cluster mode ma aa folder s2 ane s3 par pan joie (step 7).

---

## 5. Firewall

### s1 (Master + Mongo)
| Port | Kon thi | Kem |
|---|---|---|
| 7077 | s2, s3, s4 | Workers register, spark-submit app register kare |
| 8080 | Admin network | Master UI |
| 27017 | s2, s3, s4 | Mongo |
| 6066 | s4 | **Fakt cluster mode + Airflow status tracking** (step 7) |

### s2 ane s3 (Worker + Mongo)
| Port | Kon thi | Kem |
|---|---|---|
| 7078 | s1 | Master executor (ane driver) launch kare |
| 7100–7116 | s4, ane s2↔s3 | Executor block manager |
| 8081 | Admin network, s1 | Worker UI ane logs |
| 27017 | s1, s2, s3, s4 | Mongo |
| 7200–7216 | s1, s2, s3 | **Fakt cluster mode**: driver worker par chale tyare |

### s4 (spark-submit + client mode driver)
| Port | Kon thi | Kem |
|---|---|---|
| 7200, 7201 | s1, s2, s3 | Driver RPC + block manager. App jar pan 7200 thi executors ne jay che |
| 4040–4045 | Admin network | Job UI (job chale tyare) |
| 18080 | Admin network | History Server (step 8, optional) |

### Example (s2, firewalld)
```bash
firewall-cmd --permanent --add-rich-rule='rule family=ipv4 source address=<s1-ip> port port=7078 protocol=tcp accept'
firewall-cmd --permanent --add-rich-rule='rule family=ipv4 source address=<admin-subnet>/24 port port=8081 protocol=tcp accept'
firewall-cmd --permanent --add-rich-rule='rule family=ipv4 source address=<s1-ip> port port=8081 protocol=tcp accept'
for ip in <s3-ip> <s4-ip>; do
  firewall-cmd --permanent --add-rich-rule="rule family=ipv4 source address=${ip} port port=7100-7116 protocol=tcp accept"
done
for ip in <s1-ip> <s3-ip> <s4-ip>; do
  firewall-cmd --permanent --add-rich-rule="rule family=ipv4 source address=${ip} port port=27017 protocol=tcp accept"
done
firewall-cmd --reload
```
Ubuntu (`ufw`) mate: `ufw allow from <ip> to any port 7100:7116 proto tcp` jevi rit.

---

## 6. Check ane test (client mode, s4 thi)

### 6.1 Master UI

`http://s1:8080` par aa dekhavu joie:
- **Alive Workers: 2**
- Workers `s2` ane `s3` naam sathe hoy (`127.x` nahi).
- Cores ane memory saacha hoy.

```bash
curl -s http://s1:8080/json/ | python3 -m json.tool | grep -E '"aliveworkers"|"host"|"cores"|"memory"'
```

### 6.2 Test 1: SparkPi (Spark nu potanu example)

```bash
sudo -iu airflow
spark-submit \
  --class org.apache.spark.examples.SparkPi \
  --name spark_pi_test \
  /opt/spark/examples/jars/spark-examples_2.12-3.5.1.jar 100
```
Output ma `Pi is roughly 3.14...` aavvu joie, ane `echo $?` → `0`.

**Kem pehla aa test:** aama aapna code ke DB ni jarur nathi. Aa pass thay etle master, workers, ports ane driver host badhu barobar che.

### 6.3 Test 2: aapnu ETL

```bash
spark-submit \
  --deploy-mode client \
  --class com.de.spark.SparkEtlApplication \
  --name weather_etl_bronze_to_silver \
  --driver-memory 2g \
  /opt/etl/spark-weather-etl.jar \
  --spring.profiles.active=cluster \
  --spring.config.additional-location=file:/opt/etl/config/ \
  --pipeline.scheduler.enabled=false \
  --pipeline.job=bronze_to_silver
echo "exit code = $?"
```

| Bhag | Kem |
|---|---|
| `--class` ane jar **pehla** na flags | spark-submit mate |
| Jar **pachhi** na `--spring...`, `--pipeline...` | Aapni Spring app ne male che |
| `--driver-memory 2g` | Driver JVM ni memory. spark-submit e JVM start thata pehla set kare che |
| `--deploy-mode client` | Default j che, pan spasht lakhyu che |

Job chale tyare:
- `http://s1:8080` → Running Applications ma tamari app dekhase, ane executors s2 ane s3 par.
- `http://s4:4040` → stages ane tasks.

Exit code `0` aave ane `etl_job_run` table ma `SUCCESS` vali row aave etle setup puru.

### 6.4 Airflow thi

Aa j command Airflow nu `SparkSubmitOperator` banave che. Vigat [AIRFLOW_SETUP.md](AIRFLOW_SETUP.md) na step 10 ane 12 ma che. Spark ni settings `spark-defaults.conf` ma che, etle DAG ma fakt `application`, `java_class`, `driver_memory` ane `application_args` joie.

---

## 7. (Optional) Cluster mode

Tyare j vaparo jyare s4 par driver mate RAM ochhi hoy, athva driver ne workers ni najik rakhvo hoy.

### 7.1 Jar ane config s2 ane s3 par

Standalone cluster mode **jar upload nathi kartu**. Worker ene potani local disk par shodhe che.
```bash
# s4 thi, dar navi jar version vakhte
for h in s2 s3; do
  ssh spark@$h "mkdir -p /opt/etl/config"
  scp /opt/etl/spark-weather-etl-1.0.0.jar              spark@$h:/opt/etl/
  scp /opt/etl/config/application-cluster.properties    spark@$h:/opt/etl/config/
  ssh spark@$h "ln -sfn /opt/etl/spark-weather-etl-1.0.0.jar /opt/etl/spark-weather-etl.jar && chmod 640 /opt/etl/config/*"
done
```

### 7.2 Command

```bash
spark-submit \
  --deploy-mode cluster \
  --class com.de.spark.SparkEtlApplication \
  --name weather_etl_bronze_to_silver \
  --driver-memory 2g --driver-cores 1 \
  --conf spark.standalone.submit.waitAppCompletion=true \
  file:///opt/etl/spark-weather-etl.jar \
  --spring.profiles.active=cluster \
  --spring.config.additional-location=file:/opt/etl/config/ \
  --pipeline.scheduler.enabled=false \
  --pipeline.job=bronze_to_silver
```

| Bhag | Kem |
|---|---|
| `file:///opt/etl/...` | Aa path **worker** par shodhay che, s4 par nahi |
| `waitAppCompletion=true` | spark-submit job puri thay tya sudhi rah jove. Nahi to submit karta j exit 0 aape, ane Airflow ne khabar na pade ke job fail thai |
| `--driver-cores 1` | Driver pan worker na resource vapre che. Worker na cores ma jagya rakho |
| `--supervise` **na** vaparo | Fail thayeli driver ne master pote fari chalave, ane Airflow ni retry sathe double run thay |

**Logs:** worker par `/data/spark/work/driver-*/stderr` ma, athva Master UI → Completed Drivers ma.

**Airflow `SparkSubmitOperator` cluster mode ma** driver nu status REST API (6066) thi poochhe che:
- s1 na `SPARK_MASTER_OPTS` ma `-Dspark.master.rest.enabled=true -Dspark.master.rest.port=6066` umero.
- `systemctl restart spark-master` karo.
- Firewall ma 6066 **fakt s4 mate** kholo. Aa API par koi authentication nathi.

### 7.3 Cluster mode test (SparkPi)

Examples jar badha server par same path par che, etle seedho test thai shake che:
```bash
spark-submit --deploy-mode cluster \
  --class org.apache.spark.examples.SparkPi \
  --conf spark.standalone.submit.waitAppCompletion=true \
  file:///opt/spark/examples/jars/spark-examples_2.12-3.5.1.jar 100
```
Result worker na `driver-*/stdout` ma aavse.

---

## 8. (Optional) History Server, s4 par

Job puri thaya pachi `4040` UI band thai jay che. History Server juna jobs na stages, tasks ane timings pachi pan batave che.

```bash
cat >> /opt/spark/conf/spark-defaults.conf <<'EOF'
spark.history.fs.logDirectory               file:///data/spark/events
spark.history.ui.port                       18080
spark.history.fs.cleaner.enabled            true
spark.history.fs.cleaner.maxAge             14d
EOF
chown -R airflow:airflow /data/spark/events

cat > /etc/systemd/system/spark-history.service <<'EOF'
[Unit]
Description=Apache Spark History Server
After=network-online.target

[Service]
User=airflow
Environment=SPARK_HOME=/opt/spark
Environment=SPARK_NO_DAEMONIZE=true
ExecStart=/opt/spark/sbin/start-history-server.sh
Restart=always

[Install]
WantedBy=multi-user.target
EOF
systemctl daemon-reload && systemctl enable --now spark-history
```
Browser ma `http://s4:18080` kholo.

**Nondh:**
- Event log driver lakhe che. Client mode ma driver s4 par che, etle aa setup chale che.
- Cluster mode ma driver s2 ke s3 par lakhe che. Tya History Server ne te file joie, etle shared folder (NFS) ni jarur pade.
- `spark-defaults.conf` na `eventLog` vala settings sathe `events` folder **s4 par** hovu joie.

---

## 9. Rojinda kaam

| Kaam | Command |
|---|---|
| Master | `systemctl start\|stop\|restart spark-master` (s1) |
| Worker | `systemctl start\|stop\|restart spark-worker` (s2, s3) |
| Daemon logs | `/data/spark/logs/`, athva `journalctl -u spark-worker -n 200` |
| Executor logs | Master UI → app → executor → `stderr`, athva `/data/spark/work/app-*/<id>/stderr` |
| Driver logs | Client: spark-submit ke Airflow task log. Cluster: `/data/spark/work/driver-*/stderr` |
| Chalti app kill | Master UI → app → **kill**, athva Airflow task ne "Mark failed" karo |
| Disk | `du -sh /data/spark/work /data/spark/tmp` |

### Navo worker umervo

1. Navo server par step 1 ane 3 karo, potana naam sathe.
2. Firewall rules umero.
3. Badha servers na `/etc/hosts` ma entry umero.

Master, s4 ke jar ma kai badlavvanu nathi.

### Spark upgrade (3.5.x)

1. ETL nu `spark.version` badli ne jar rebuild karo.
2. **Badha 4 server** par navu tarball extract karo.
3. `conf/spark-env.sh` ane `conf/spark-defaults.conf` navi directory ma copy karo.
4. `ln -sfn` thi symlink badlo.
5. Workers stop karo, master restart karo, pachi workers start karo.

---

## 10. Common problems

| Problem | Karan | Ukel |
|---|---|---|
| Master UI ma worker nathi, ke IP `127.0.1.1` che | `/etc/hosts` ni Ubuntu line, ke s1:7077 firewall | Line kadho, `SPARK_LOCAL_HOSTNAME` set karo, ane `journalctl -u spark-worker` juo |
| `Initial job has not accepted any resources` | Executor size worker ma samatu nathi, ke executors s4:7200 sudhi pahonchta nathi | `spark.executor.memory` ane `cores` ghatado. s2 thi `nc -zv s4 7200` chalavo |
| `ClassNotFoundException: com.de.spark.SparkEtlApplication` | Spring Boot (`BOOT-INF`) jar che, shade jar nathi | pom ma shade plugin vaparo ane `spring-boot-maven-plugin` kadho |
| `Failed to find data source: mongodb` | Shade jar ma `META-INF/services` merge nathi thai | Shade ma `ServicesResourceTransformer` che ke nahi te juo (Spring Boot parent ma hoy che) |
| Job `local[*]` ma chale che, cluster par nahi | `SparkConfig` haju `setMaster` vapre che | `setIfMissing` karo |
| `LoggerFactory is not a Logback LoggerContext` | Spark nu log4j2 ane Spring nu logback takray che | `spring-boot-starter-logging` exclude karo |
| `InvalidClassException ... serialVersionUID` | Spark version alag che | Badha 4 server par 3.5.1 rakho |
| Cluster mode: spark-submit turant exit 0, pan job fail | `waitAppCompletion` nathi | `--conf spark.standalone.submit.waitAppCompletion=true` |
| Cluster mode: `java.nio.file.NoSuchFileException: /opt/etl/...jar` | Jar worker par nathi | Step 7.1 |
| Cluster mode: driver `Access denied` (DB) | `config/` worker par nathi | Step 7.1 |
| `Too many open files` | File limit ochhi che | systemd ma `LimitNOFILE=65536` |
| OOM, Mongo ke executor achanak band | RAM vadhare vaparay che | Step 3.1 |
| Port 8080 busy | Biji service | `SPARK_MASTER_WEBUI_PORT=9090`, restart, ane firewall |

---

## 11. Badha server par shu karvu (summary)

| Step | s1 | s2 | s3 | s4 |
|---|---|---|---|---|
| 1: packages, hosts, user, Java, Spark, folders | ✔ | ✔ | ✔ | ✔ |
| 2: master `spark-env.sh` + `spark-master.service` | ✔ | | | |
| 3.1: Mongo `cacheSizeGB` | ✔ | ✔ | ✔ | |
| 3.2–3.3: worker `spark-env.sh` + `spark-worker.service` | | ✔ | ✔ | |
| 4: client `spark-env.sh`, `spark-defaults.conf`, jar + config | | | | ✔ |
| 5: firewall | ✔ | ✔ | ✔ | ✔ |
| 6: tests (SparkPi, ETL) | | | | ✔ (chalavo) |
| 7: cluster mode (optional): jar + config, REST 6066 | ✔ (6066) | ✔ | ✔ | |
| 8: History Server (optional) | | | | ✔ |
