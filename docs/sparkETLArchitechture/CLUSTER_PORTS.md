# Spark Cluster + Airflow: Ports ane Firewall Setup

## Servers

| Server | Role |
|---|---|
| s1 | Spark Master + MongoDB (replica set member) |
| s2 | Spark Worker-1 + MongoDB (replica set member) |
| s3 | Spark Worker-2 + MongoDB (replica set member) |
| s4 | Airflow |
| s5 | EDBP (RDBMS) |
| s6 | ETL jar server. Ahiya `java -jar` chale che, etle **Spark driver** ahiya bane che |

Badha port **fixed** rakhva padse, karan Spark na ghana port default ma **random** hoy che ane random port par firewall rule nathi lagavi shakato.

---

## 1. Fixed ports set karo

### s1 (master): `/opt/spark/conf/spark-env.sh`
```bash
SPARK_MASTER_HOST=s1
SPARK_MASTER_PORT=7077
SPARK_MASTER_WEBUI_PORT=8080      # busy hoy to 9090 vagere kai pan
```

### s2, s3 (workers): `/opt/spark/conf/spark-env.sh`
```bash
SPARK_WORKER_PORT=7078            # default random → fixed karvo jaruri
SPARK_WORKER_WEBUI_PORT=8081
```

### s6 (jar): `application-cluster.properties`
```properties
spark.master=spark://s1:7077
spark.conf.[spark.driver.host]=<s6-ip>
spark.conf.[spark.driver.bindAddress]=0.0.0.0
spark.conf.[spark.driver.port]=7200
spark.conf.[spark.driver.blockManager.port]=7201
# executors (s2, s3) nu block manager: 7100 busy hoy to Spark 7101, 7102... try kare
spark.conf.[spark.blockManager.port]=7100
spark.conf.[spark.port.maxRetries]=16
spark.conf.[spark.ui.port]=4040

pipeline.mongo-uri=mongodb://s1:27017,s2:27017,s3:27017/?replicaSet=rs0
pipeline.silver-jdbc-url=jdbc:mysql://s5:3306/de_silver
pipeline.gold-jdbc-url=jdbc:mysql://s5:3306/de_gold
```

---

## 2. Darek server par kaya port open karva (inbound)

### s1: Spark Master + Mongo
| Port | Kon thi | Kem |
|---|---|---|
| 7077 | s2, s3, s6 | Workers register thay, driver app submit kare |
| 8080 | Browser (admin) | Spark Master UI |
| 27017 | s2, s3, s6 | Mongo: replica set members + driver + executors |
| 22 | Admin | SSH |

### s2 ane s3: Spark Worker + Mongo (banne par same)
| Port | Kon thi | Kem |
|---|---|---|
| 7078 | s1 | Master worker ne "executor start karo" kahe |
| 7100–7116 | s6, ane s2↔s3 ek bija thi | Executor block manager: shuffle data, cached data, task results |
| 8081 | Browser (admin), s1 | Worker UI, executor logs |
| 27017 | s1, s2, s3, s6 | Mongo replica set + Spark read |
| 22 | Admin | SSH |

### s6: ETL jar (Spark driver)
| Port | Kon thi | Kem |
|---|---|---|
| 7200 | s1, s2, s3 | Driver RPC: master app register kare, executors task le ane jars download kare (`spark.jars` aa j port thi jay) |
| 7201 | s2, s3 | Driver block manager: broadcast data |
| 4040 | Browser (admin) | Spark job UI. Fakt job chale tyare j khule. Be app saathe chale to 4041, 4042... |
| 22 | **s4 (Airflow)**, Admin | Airflow SSHOperator thi jar chalave |

### s4: Airflow
| Port | Kon thi | Kem |
|---|---|---|
| 8080 | Browser (admin) | Airflow UI. Aa s1 thi alag server che etle conflict nathi, chhata badli shako |
| 22 | Admin | SSH |

### s5: EDBP (RDBMS)
| Port | Kon thi | Kem |
|---|---|---|
| 3306 (MySQL) | **s2, s3, s6** | Executors (s2, s3) seedha write ane read kare. Driver (s6) merge, watermark, audit ane Flyway mate vapre |
| 3306 | s4 (optional) | Airflow nu metadata DB s5 par rakho to j |

RDBMS MySQL na hoy to port aa pramane badlase: PostgreSQL 5432, Oracle 1521, SQL Server 1433.

---

## 3. Communication flow (kon kone call kare)

```
s4 Airflow ──22──► s6 (java -jar start)
s6 driver  ──7077──► s1 master          s1 ──7200──► s6 (app registered)
s1 master  ──7078──► s2,s3 (executor launch)
s2,s3 exec ──7200,7201──► s6 (tasks, jars, broadcast)
s6 ◄──7100-7116──► s2,s3 ◄──► each other (shuffle / results)
s2,s3,s6   ──27017──► s1,s2,s3 (Mongo read)
s2,s3,s6   ──3306──► s5 (DB write)
```

**Mongo mate khas vaat:** URI ma ek j host aapo to pan Mongo client replica set na **badha members** sathe connect thay che. Etle s6, s2 ane s3 thi s1, s2, s3 na 27017 open joie. Replica set config (`rs.conf()`) ma je hostnames che te s6 thi resolve thava joie, etle `/etc/hosts` ke DNS set karo.

---

## 4. Je port **nathi** joie

- **6066** (Spark REST submit): aapne `java -jar` vapriye chie.
- **7337** (external shuffle service): fakt dynamic allocation mate.
- **18080** (History server): optional che. Juna job na UI jova hoy to s1 par chalavo.
- **Redis 6379 / Celery**: Airflow `LocalExecutor` par chale to jarur nathi.

---

## 5. 8080 (ke bijo koi port) busy hoy to?

**Badha port badli shakay che.** Fakt je jagya e port vaparay che tya pan update karvo pade:

| Service | Kya badalvo |
|---|---|
| Spark Master UI | `SPARK_MASTER_WEBUI_PORT=9090` (s1 nu spark-env.sh) |
| Spark Master RPC | `SPARK_MASTER_PORT=7177`, ane s6 ma `spark.master=spark://s1:7177`, ane workers na start command ma pan |
| Worker UI | `SPARK_WORKER_WEBUI_PORT=9091` |
| Airflow UI | Airflow 2: `AIRFLOW__WEBSERVER__WEB_SERVER_PORT=8090`. Airflow 3: `AIRFLOW__API__PORT=8090` |
| Mongo | `mongod.conf` ma `net.port`, sathe `rs.conf()` ma member host:port ane `pipeline.mongo-uri` |
| Driver / block manager | `spark.driver.port` vagere ma koi pan free port |

Spark nu UI port busy hoy to Spark pote aagal no port (8081...) try kare che. Pan aa worker UI sathe confuse kari shake, etle UI port **explicitly set karvo** vadhu saru che.

---

## 6. Firewall example (firewalld, s2 par)

```bash
firewall-cmd --permanent --add-rich-rule='rule family=ipv4 source address=<s1-ip> port port=7078 protocol=tcp accept'
firewall-cmd --permanent --add-rich-rule='rule family=ipv4 source address=<s6-ip> port port=7100-7116 protocol=tcp accept'
firewall-cmd --permanent --add-rich-rule='rule family=ipv4 source address=<s3-ip> port port=7100-7116 protocol=tcp accept'
firewall-cmd --reload
```

Aa ja rite badhe "source IP + port" vala rule banavo. Aakhu network ma port open karvathi (`0.0.0.0/0`) bacho.

---

## 7. Salah

Mongo ane Spark worker same server (s2, s3) par che, etle executor ni memory ane cores e rite rakho ke Mongo mate pan RAM bache. Jem ke 16 GB RAM hoy to `SPARK_WORKER_MEMORY` aashare 8 GB rakho.
