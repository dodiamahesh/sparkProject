# Apache Spark Standalone Cluster Setup: 1 Master + 2 Workers (Linux, A to Z, spark-submit vagar)

Aa guide **s1 (master), s2 ane s3 (workers)** par Spark cluster banavva mate che. ETL jar **s4 par `java -jar`** thi chale che ane te Spark driver bane che. Etle ahiya `spark-submit` ni jarur nathi. Setup puru thaya pachi:

- `http://s1:8080` par master UI ma **2 ALIVE workers** dekhase.
- Reboot pachi master ane workers automatic chalu thase (systemd).
- s4 nu ETL jar `spark://s1:7077` sathe connect thai ne s2 ane s3 par kaam karshe.

> Related: Airflow ane jar setup mate [AIRFLOW_SETUP_JAVA_JAR.md](AIRFLOW_SETUP_JAVA_JAR.md) juo. Ports ni aakhi list mate [CLUSTER_PORTS.md](CLUSTER_PORTS.md) juo. Tya driver s6 par che, ahiya s4 par.

---

## 0. Overview

```
                 s4 (Airflow + ETL jar = Spark DRIVER, java -jar)
                          │ spark://s1:7077
          ┌───────────────▼───────────────┐
          │ s1: Spark Master  + MongoDB   │  resources vahenche, kaam nathi kartu
          └───────┬───────────────┬───────┘
      ┌───────────▼─────┐   ┌─────▼───────────┐
      │ s2: Worker + Mongo│ │ s3: Worker + Mongo│  executors ahiya chale (asli kaam)
      └───────────┬─────┘   └─────┬───────────┘
                  └──── s5 (EDBP / RDBMS) ────┘
```

| Component | Kya | Su kare |
|---|---|---|
| **Master** | s1 | Workers no hisab rakhe che. App aave tyare kaya worker par ketla executors aapva te nakki kare che |
| **Worker** | s2, s3 | Potana cores ane RAM master ne "offer" kare che. Master kahe tyare executor JVM start kare che |
| **Executor** | s2, s3 (app chale tyare j) | Asli data processing: Mongo mathi vanche, transform kare, EDBP ma lakhe |
| **Driver** | s4 (ETL jar) | Job no plan banave, executors ne tasks aape, ane merge, watermark ane audit kare |

### Versions (badha server par **exactly same**)

| | Version | Kem |
|---|---|---|
| Spark | **3.5.1** (pre-built for Hadoop 3) | ETL jar ma `spark.version=3.5.1` bundled che. Version alag hoy to `InvalidClassException` aave |
| Scala | **2.12** | Spark 3.5.1 nu default tarball Scala 2.12 nu che, ane jar pan `_2.12` che |
| Java | **17** | Project `java.version=17` che. Driver ane executors same Java par chale e safe che |

---

## 1. Badha server par (s1, s2, s3)

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
**Kem:**
- Badha server no time same hovo joie. Time ma farak hoy to logs ma events no order khoto dekhay.
- Mongo replica set ane ETL nu watermark (time based) pan time par aadhar rakhe che. `chrony` time sync kare che.

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
**Ubuntu par khas check karo:** `/etc/hosts` ma `127.0.1.1  s2` jevi line hoy to te **kadhi nakho**, nahi to comment karo.

**Kem:**
- Ubuntu server nu potanu naam `127.0.1.1` par map kare che. Etle worker master ne potanu address `127.0.1.1` kahe, ane bija servers tya connect nathi thai shakta.
- Aa sauthi common setup bhul che.
- `s4` ni entry jaruri che, kem ke executors driver ne `s4` naam thi shodhe che.

Check:
```bash
getent hosts s1 s2 s3 s4 s5     # badha ma real IP aavvu joie, 127.x nahi
hostname -f
```

### 1.3 `spark` user

```bash
useradd --system --create-home --home-dir /opt/spark-home --shell /bin/bash spark
```
**Kem:** Spark daemons `root` thi na chalavva joie. Executor ma app code chale che, etle alag user thi nuksan ochhu thay.

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
- Symlink `/opt/spark` rakhvathi upgrade vakhte fakt link badlavvi pade, ane config ke systemd files nathi badlavvi padti.

### 1.6 Data folders

```bash
mkdir -p /data/spark/{logs,run,work,tmp,recovery}
chown -R spark:spark /data/spark
```

| Folder | Su rahe | Kem alag |
|---|---|---|
| `logs` | Master ane worker daemon na logs | Default `/opt/spark/logs` upgrade vakhte link sathe jato rahe che |
| `run` | PID files | Default `/tmp` che. OS e saaf kare to stop scripts kaam nathi karta |
| `work` | (worker) Darek app nu folder: executor `stdout`/`stderr` ane app jars | Ghanu motu thay che, etle mota disk par rakho |
| `tmp` | (worker) Shuffle ane spill files | Bahu I/O thay che. Fast disk (SSD) par rakho |
| `recovery` | (master) Restart pachi workers ane apps ni yaad | Step 2 |

Moti disk alag mount hoy (jem ke `/data`) to aa folders e j disk par rakho.

### 1.7 Environment (shell mate)

```bash
cat > /etc/profile.d/spark.sh <<'EOF'
export SPARK_HOME=/opt/spark
export PATH=$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH
EOF
```
**Kem:** login karya pachi `spark-shell` ane `start-worker.sh` jeva commands seedha chale. systemd ne aa file nathi joiti.

---

## 2. Master (fakt s1)

### 2.1 `/opt/spark/conf/spark-env.sh`

```bash
cat > /opt/spark/conf/spark-env.sh <<'EOF'
JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64     # step 1.4 no path (RHEL: /usr/lib/jvm/jre-17)

SPARK_MASTER_HOST=s1
SPARK_LOCAL_HOSTNAME=s1
SPARK_MASTER_PORT=7077
SPARK_MASTER_WEBUI_PORT=8080          # busy hoy to 9090 vagere

SPARK_DAEMON_MEMORY=1g
SPARK_LOG_DIR=/data/spark/logs
SPARK_PID_DIR=/data/spark/run

# master restart thay to workers ane apps ni yaad /data/spark/recovery mathi pachi aave
# defaultCores: je app spark.cores.max na aape te pan aakho cluster na pachadi le
SPARK_MASTER_OPTS="-Dspark.deploy.recoveryMode=FILESYSTEM -Dspark.deploy.recoveryDirectory=/data/spark/recovery -Dspark.deploy.defaultCores=4"
EOF
chown spark:spark /opt/spark/conf/spark-env.sh
```

| Setting | Su kare | Na muko to |
|---|---|---|
| `SPARK_MASTER_HOST=s1` | Master `s1` naam par listen kare | Koi vaar `127.0.1.1` par bind thay, ane workers connect nathi thai shakta |
| `SPARK_MASTER_PORT=7077` | Workers ane driver aa port par aave | Default pan 7077 che, pan lakhvathi saaf rahe |
| `SPARK_MASTER_WEBUI_PORT` | Master UI | Default 8080. Busy hoy to Spark 8081 lai le, je worker UI sathe confuse thay |
| `SPARK_DAEMON_MEMORY=1g` | Master process ni RAM | Default 1g. Master halku che, ane s1 par Mongo mate RAM bache |
| `recoveryMode=FILESYSTEM` | Master restart pachi workers ane apps fari register thay | Master restart thay to chalu job tuti jay ane workers pacha jodata vaar lage |
| `defaultCores=4` | `spark.cores.max` vagar ni app max 4 cores le | Ek app aakha cluster na badha cores lai le, ane biji app "WAITING" ma rahe |

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
# script ne foreground ma chalave, jethi systemd process ne track kari shake
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
- `SPARK_NO_DAEMONIZE=true` vagar script background ma java start kari ne exit thai jay. Pachi systemd ne lage ke service band thai gai.
- `LimitNOFILE` thi shuffle vakhte ghani files ek sathe khuli shake che (`Too many open files` nathi aavtu).

Check:
```bash
nc -zv s1 7077
curl -s http://s1:8080 | grep -o "Spark Master at spark://[^<]*"
```

---

## 3. Workers (s2 ane s3)

### 3.1 Memory ane cores nu planning (Mongo same server par che)

s2 ane s3 par **MongoDB pan chale che**, etle aakhi RAM Spark ne na aapo.

| Server RAM | OS | Mongo (cache + overhead) | **SPARK_WORKER_MEMORY** |
|---|---|---|---|
| 16 GB | 2 GB | 4 GB cache + ~2 GB = 6 GB | **8g** |
| 32 GB | 3 GB | 8 GB cache + ~3 GB = 11 GB | **18g** |
| 64 GB | 4 GB | 16 GB cache + ~4 GB = 20 GB | **40g** |

**Mongo ni cache limit fix karo** (s1, s2, s3 par `/etc/mongod.conf`):
```yaml
storage:
  wiredTiger:
    engineConfig:
      cacheSizeGB: 4
```
Pachi `systemctl restart mongod` chalavo. Ek sathe nahi, replica set na ek pachi ek member restart karo.

**Kem:** Mongo default ma aashare **adhi RAM** cache mate lai le che. Spark pan potani RAM le, to OS memory khatam thay ane **OOM killer** Mongo ke executor ne mari nakhe.

**Cores:** `SPARK_WORKER_CORES` = CPU cores − 1 ke 2 (Mongo ane OS mate). Jem ke 8-core server par `6`.

### 3.2 `/opt/spark/conf/spark-env.sh` (s2 par, s3 par naam badli ne)

```bash
cat > /opt/spark/conf/spark-env.sh <<'EOF'
JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

SPARK_LOCAL_HOSTNAME=s2              # s3 par: s3
SPARK_WORKER_CORES=6
SPARK_WORKER_MEMORY=8g               # step 3.1 nu table juo
SPARK_WORKER_PORT=7078               # default random, fixed karvo jaruri (firewall)
SPARK_WORKER_WEBUI_PORT=8081

SPARK_WORKER_DIR=/data/spark/work
SPARK_LOCAL_DIRS=/data/spark/tmp
SPARK_LOG_DIR=/data/spark/logs
SPARK_PID_DIR=/data/spark/run
SPARK_DAEMON_MEMORY=1g

# juni app folders (jars, logs) 7 divas pachi automatic saaf, dar 30 minute check
SPARK_WORKER_OPTS="-Dspark.worker.cleanup.enabled=true -Dspark.worker.cleanup.interval=1800 -Dspark.worker.cleanup.appDataTtl=604800"
EOF
chown spark:spark /opt/spark/conf/spark-env.sh
```

| Setting | Su kare | Na muko to |
|---|---|---|
| `SPARK_LOCAL_HOSTNAME=s2` | Worker master ne potanu naam `s2` kahe | `127.0.1.1` ke khoto naam jay, ane master ke driver tya pahonchi nathi shakta |
| `SPARK_WORKER_CORES` | Ketla cores Spark ne offer karva | Badha cores Spark lai le, ane Mongo dhimu padi jay |
| `SPARK_WORKER_MEMORY` | Kul ketli RAM executors ne aapvi | Default "RAM − 1 GB" lage, je Mongo sathe OOM karave |
| `SPARK_WORKER_PORT=7078` | Master aa port thi "executor start karo" kahe | Random port, ane firewall master ne block kare |
| `SPARK_WORKER_DIR` | Executor logs ane app jars | `/opt/spark/work` ma bharay, ane upgrade vakhte gum thay |
| `SPARK_LOCAL_DIRS` | Shuffle ane spill | `/tmp` bharai jay ane OS ne asar thay |
| `worker.cleanup.*` | Juna app folders delete | Darek run e jars copy thay che. Dar 15 minute na run thi disk divaso ma bharai jay |

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
**Kem `Restart=always`:**
- Master band hoy ke modo chalu thay to worker thoda prayaso pachi exit thai jay che.
- systemd ene 15 second pachi fari chalu kare, etle master aave tyare worker pote jodai jay.
- Aa rite server kaya order ma chalu thay che te mahatvanu nathi rahetu.

**Passwordless SSH ke `conf/workers` file ni jarur nathi.** Te fakt `start-all.sh` mate hoy che. Ahiya darek server potani service jate chalave che.

---

## 4. Firewall

### s1 (Master + Mongo)
| Port | Kon thi | Kem |
|---|---|---|
| 7077 | s2, s3, s4 | Workers register thay, driver app register kare |
| 8080 | Admin network | Master UI |
| 27017 | s2, s3, s4 | Mongo replica set + Spark read |

### s2 ane s3 (Worker + Mongo)
| Port | Kon thi | Kem |
|---|---|---|
| 7078 | s1 | Master executor launch kare |
| 7100–7116 | s4, ane s2↔s3 | Executor block manager: shuffle ane results |
| 8081 | Admin network, s1 | Worker UI ane executor logs |
| 27017 | s1, s2, s3, s4 | Mongo |

### s4 (driver), Spark mate
| Port | Kon thi | Kem |
|---|---|---|
| 7200, 7201 | s1, s2, s3 | Driver RPC + block manager. `spark.jars` pan 7200 thi jay che |
| 4040 | Admin network | Job UI (job chale tyare) |

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

Firewall lagavya pachi `systemctl restart spark-worker` karo, ane step 5 nu check fari karo.

---

## 5. Cluster check

### 5.1 Master UI

Browser ma `http://s1:8080` kholo. Tya aa dekhavu joie:
- **Alive Workers: 2**
- Darek worker `s2` ane `s3` naam sathe hoy (IP `127.x` nahi).
- Cores ane Memory te j hoy je `spark-env.sh` ma lakhya che.

Command line thi:
```bash
curl -s http://s1:8080/json/ | python3 -m json.tool | grep -E '"aliveworkers"|"cores"|"memory"|"host"'
```

### 5.2 Network check (s4 thi)

```bash
nc -zv s1 7077
nc -zv s2 7100     # executor na chale tyare closed hoy, aa normal che
```

### 5.3 Asli test: ETL job (s4 thi, spark-submit vagar)

```bash
sudo -iu airflow
/opt/etl/run-job.sh bronze_to_silver; echo "exit code = $?"
```
Job chale tyare aa jagya e juo:
- `http://s1:8080`: **Running Applications** ma `weather-etl`, ane tena executors s2 ane s3 par.
- `http://s4:4040`: job na stages ane tasks.
- Job puri thay etle app **Completed Applications** ma jay, ane `exit code = 0` aave.

`run-job.sh`, `application-cluster.properties` ane `executor-jars` nu setup [AIRFLOW_SETUP_JAVA_JAR.md](AIRFLOW_SETUP_JAVA_JAR.md) na step 4 ma che.

### 5.4 Driver side (s4) ni Spark settings, cluster sathe match

`/opt/etl/config/application-cluster.properties` ma:
```properties
spark.master=spark://s1:7077
spark.conf.[spark.driver.host]=s4
spark.conf.[spark.driver.bindAddress]=0.0.0.0
spark.conf.[spark.driver.port]=7200
spark.conf.[spark.driver.blockManager.port]=7201
spark.conf.[spark.blockManager.port]=7100
spark.conf.[spark.port.maxRetries]=16
# executor size: worker ni memory ane cores ma samavvu joie
spark.conf.[spark.executor.memory]=4g
spark.conf.[spark.executor.cores]=2
spark.conf.[spark.cores.max]=4
# executor logs rotate: work folder bharai na jay
spark.conf.[spark.executor.logs.rolling.strategy]=size
spark.conf.[spark.executor.logs.rolling.maxSize]=104857600
spark.conf.[spark.executor.logs.rolling.maxRetainedFiles]=5
```

**Sizing no niyam:**
- `spark.executor.memory` ≤ `SPARK_WORKER_MEMORY`, ane `spark.executor.cores` ≤ `SPARK_WORKER_CORES`.
- Na samaay to app **WAITING** ma j rahe che ("Initial job has not accepted any resources").
- Upar na example ma darek worker par 1 executor bane che (2 cores, 4 GB), ane kul 4 cores. Master default ma executors ne workers par "spread" kare che, etle s2 ane s3 banne vaparay che.
- Java 17 na `--add-opens` executors ne **Spark pote** aape che. Driver (s4) mate te `run-job.sh` ma che.

---

## 6. Rojinda kaam

| Kaam | Command |
|---|---|
| Master start/stop/restart | `systemctl start\|stop\|restart spark-master` (s1) |
| Worker start/stop/restart | `systemctl start\|stop\|restart spark-worker` (s2, s3) |
| Daemon logs | `/data/spark/logs/spark-spark-org.apache.spark.deploy.*.out`, athva `journalctl -u spark-worker -n 200` |
| Executor logs | Master UI → app → executor → `stdout`/`stderr`, athva `/data/spark/work/app-*/<id>/stderr` |
| Disk check | `du -sh /data/spark/work /data/spark/tmp` |

### Navo worker (s7) umervo

1. s7 par step 1 aakhu karo.
2. Step 3 karo, `SPARK_LOCAL_HOSTNAME=s7` sathe.
3. Firewall rules umero.
4. Badha servers na `/etc/hosts` ma s7 ni entry umero.

Master ke driver ma kai badlavvanu nathi. Worker pote register thai jase.

### Spark upgrade (jem ke 3.5.x)

1. **ETL jar nu `spark.version` pan same version par karo** ane jar fari build karo. Banne same hova joie.
2. Badha servers par navu tarball `/opt` ma extract karo.
3. `conf/spark-env.sh` navi directory ma copy karo.
4. `ln -sfn /opt/spark-<new> /opt/spark` karo.
5. Pehla workers stop karo, pachi master restart karo, pachi workers start karo.

---

## 7. Common problems

| Problem | Karan | Ukel |
|---|---|---|
| Master UI ma worker nathi | Firewall (s1:7077), ke worker `127.0.1.1` thi register thayo | `journalctl -u spark-worker`. `/etc/hosts` ma thi `127.0.1.1 <hostname>` line kadho, ane `SPARK_LOCAL_HOSTNAME` set karo |
| Worker dekhay pan IP `127.0.1.1` che | Upar mujab | Upar mujab |
| `Initial job has not accepted any resources` | Executor memory ke cores worker ma samatu nathi, ke executors s4:7200 sudhi pahonchta nathi | `spark.executor.memory` ane `spark.executor.cores` ghatado. s2 thi `nc -zv s4 7200` chalavo |
| App chale che pan badha executors **EXITED** ke **KILLED** | Executor no Java ke Spark version alag che, ke s4 sudhi network nathi | Executor `stderr` juo (Master UI → app → executor) |
| `InvalidClassException ... serialVersionUID` | Jar no Spark version ane cluster no version alag che | Banne 3.5.1 (Scala 2.12) rakho |
| `Too many open files` | File limit ochhi che | systemd ma `LimitNOFILE=65536` che ke nahi te juo |
| Mongo ke executor achanak band thay, `dmesg` ma `Out of memory` | RAM vadhare vaparay che | Step 3.1: Mongo `cacheSizeGB` ane `SPARK_WORKER_MEMORY` ghatado |
| `/data` disk bharai | `work/` ma juna apps che | `worker.cleanup.*` setting check karo, ane executor log rolling (5.4) |
| Master restart pachi workers nathi jodata | Recovery dir ni permission | `chown -R spark:spark /data/spark/recovery` |
| UI port 8080 busy che | Biji service | `SPARK_MASTER_WEBUI_PORT=9090` muko, `systemctl restart spark-master`, ane firewall rule badlo |

---

## 8. Badha server par shu karvu (summary)

| Step | s1 | s2 | s3 |
|---|---|---|---|
| 1.1–1.7: packages, hosts, user, Java, Spark, folders | ✔ | ✔ | ✔ |
| 2: master `spark-env.sh` + `spark-master.service` | ✔ | | |
| 3.1: Mongo `cacheSizeGB` | ✔ | ✔ | ✔ |
| 3.2–3.3: worker `spark-env.sh` + `spark-worker.service` | | ✔ (`s2`) | ✔ (`s3`) |
| 4: firewall | ✔ | ✔ | ✔ |
| 5: check (UI + ETL test s4 thi) | ✔ | | |
