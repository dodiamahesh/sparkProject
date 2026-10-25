# Spark Standalone Cluster Setup – Windows 11 (ગુજરાતી માર્ગદર્શિકા)

> 📝 **Note:** Cluster ના steps (Spark install, master, workers) એ જ છે. Project હવે **medallion pipeline** ચલાવે છે (bronze MongoDB → silver/gold MySQL). જ્યાં `dataengineering` database કે "ETL" નો ઉલ્લેખ છે, તે જૂના pipeline નો છે. નવા pipeline ને ચલાવવા અને ચકાસવા માટે [MEDALLION_CODE_GUIDE_GU.md](MEDALLION_CODE_GUIDE_GU.md) જુઓ.
> ⚠️ RAM: Kafka stack ચાલુ હોય ત્યારે cluster (master + 2 workers) ચલાવવા આશરે 3 GB વધારાની RAM જોઈએ.

આ માર્ગદર્શિકામાં આપણે શીખીશું:
1. Windows 11 માં Apache Spark install કરવું
2. 1 Master + 2 Worker નો **Standalone Cluster** Windows પર ચલાવવો (**dev** profile)
3. આ જ cluster **Docker** માં ચલાવવો (**prod** profile)
4. આપણો Spring Boot ETL project આ cluster પર ચલાવવો

---

## 0. પહેલા સમજી લો – Architecture અને Profiles

### 0.1 ત્રણ Profiles

| Profile | ક્યાં ચાલે છે | Spark master | ક્યારે વાપરવું |
|---|---|---|---|
| `default` | ફક્ત IDE / JVM ની અંદર | `local[*]` | Coding અને જલ્દી testing |
| `dev` | Windows પર install કરેલ Spark cluster | `spark://localhost:7077` | Cluster ને locally શીખવા/test કરવા |
| `prod` | Docker containers | `spark://spark-master:7077` | Production જેવું setup |

Config files:
```
src/main/resources/application.properties        ← default (બધા profile માં common settings)
src/main/resources/application-dev.properties    ← dev (ફક્ત બદલાતી settings)
src/main/resources/application-prod.properties   ← prod (ફક્ત બદલાતી settings)
docker-compose.yml, docker/*.Dockerfile          ← prod માટે Docker setup
```
Spring Boot પહેલા `application.properties` load કરે છે, પછી active profile ની file તેની ઉપર **override** કરે છે.

### 0.2 Cluster કેવી રીતે કામ કરે છે

```
        ┌──────────────────────────────┐
        │  Spring Boot ETL App          │  ← આ જ "Spark Driver" છે
        │  (Scheduler દર 2 મિનિટે)       │     (job નો plan બનાવે છે)
        └──────────────┬───────────────┘
                       │ spark://...:7077
               ┌───────▼────────┐
               │  Spark Master  │  ← resources (CPU/RAM) ની વહેંચણી કરે છે
               └───┬────────┬───┘
          ┌────────▼──┐  ┌──▼────────┐
          │ Worker-1  │  │ Worker-2  │  ← અહીં "Executor" ચાલે છે
          │ Executor  │  │ Executor  │     (JSON વાંચે, MySQL માં લખે)
          └───────────┘  └───────────┘
```
- **Driver** = આપણી Spring Boot app. તે નક્કી કરે છે કે શું કરવું.
- **Master** = manager. કયો worker કેટલું કામ કરશે તે નક્કી કરે છે.
- **Worker / Executor** = મજૂર. ખરેખર data વાંચવાનું અને MySQL માં લખવાનું કામ અહીં થાય છે.

તેથી 2 વાત ખાસ યાદ રાખો:
1. **Input file** worker ને પણ દેખાવી જોઈએ (dev માં એક જ machine છે એટલે ચાલે; Docker માં volume mount કર્યું છે).
2. **MySQL JDBC driver jar** દરેક worker પાસે હોવી જોઈએ (કારણ કે executor જ MySQL માં લખે છે).

### 0.3 Spark Maven dependency કેમ remove નથી કરી?

તમે પૂછ્યું હતું કે standalone માટે જરૂર ન હોય તો Spark dependency કાઢી નાખો. **આ project માં તે જરૂરી છે**, કારણ:
- આપણી Spring Boot app પોતે જ **Driver** છે (client mode). Driver ને `SparkSession`, DataFrame API વગેરે માટે Spark ની jars જોઈએ જ.
- Dependency ત્યારે જ કાઢી શકાય (`provided` scope) જ્યારે app ને `spark-submit` થી ચલાવીએ. પણ તો Spring Boot નો scheduler અને Spring Boot fat-jar, Spark ની jars સાથે conflict કરે (logging, Jackson વગેરે).
- તેથી સૌથી સરળ અને સ્થિર રીત: dependency રાખો, અને **project નું Spark version = cluster નું Spark version** રાખો.

⚠️ **ખૂબ મહત્વનું:** Project માં Spark `3.5.1`, Scala `2.12`, Java `17` છે. Cluster માં પણ **બરાબર આ જ versions** હોવા જોઈએ, નહીંતર `InvalidClassException` / serialization errors આવશે.

---

## 1. પૂર્વજરૂરિયાતો (Prerequisites)

| Software | Version | ચકાસવા માટે command |
|---|---|---|
| JDK | 17 | `java -version` |
| Maven | 3.9+ | `mvn -v` |
| MySQL | 8.x (port 3306) | MySQL Workbench / `mysql -u root -p` |
| Docker Desktop | latest (ફક્ત prod માટે) | `docker --version` |

તમારા system માં JDK 17 અહીં છે: `C:\Program Files\Microsoft\jdk-17.0.19.10-hotspot`

---

# ભાગ A – dev profile: Windows 11 પર Spark Standalone Cluster

## A1. Spark download અને extract કરો

1. Browser માં ખોલો: https://archive.apache.org/dist/spark/spark-3.5.1/
2. File download કરો: **`spark-3.5.1-bin-hadoop3.tgz`**
3. PowerShell ખોલો અને extract કરો (Windows 11 માં `tar` પહેલેથી હોય છે):
   ```powershell
   cd $env:USERPROFILE\Downloads
   tar -xzf spark-3.5.1-bin-hadoop3.tgz
   Move-Item spark-3.5.1-bin-hadoop3 C:\spark
   ```
4. હવે `C:\spark\bin`, `C:\spark\jars` વગેરે folders દેખાવા જોઈએ.

> 💡 Spark ને એવા path માં રાખો જેમાં **space ન હોય** (જેમ કે `C:\spark`). `Program Files` માં ન રાખો.

## A2. Hadoop winutils install કરો (Windows માટે ફરજિયાત)

Spark અંદરથી Hadoop libraries વાપરે છે. Windows પર Hadoop ને `winutils.exe` અને `hadoop.dll` જોઈએ. તે વગર worker પર files ના permission સેટ કરતી વખતે error આવે છે.

1. Folder બનાવો: `C:\hadoop\bin`
   ```powershell
   New-Item -ItemType Directory -Force C:\hadoop\bin
   ```
2. આ GitHub page ખોલો: https://github.com/cdarlint/winutils/tree/master/hadoop-3.3.5/bin
3. ત્યાંથી આ 2 files download કરીને `C:\hadoop\bin` માં મૂકો:
   - `winutils.exe`
   - `hadoop.dll`

## A3. Environment Variables સેટ કરો

PowerShell (સામાન્ય, Admin જરૂરી નથી) માં આ commands ચલાવો:

```powershell
[Environment]::SetEnvironmentVariable("JAVA_HOME",   "C:\Program Files\Microsoft\jdk-17.0.19.10-hotspot", "User")
[Environment]::SetEnvironmentVariable("SPARK_HOME",  "C:\spark",  "User")
[Environment]::SetEnvironmentVariable("HADOOP_HOME", "C:\hadoop", "User")

$p = [Environment]::GetEnvironmentVariable("Path", "User")
[Environment]::SetEnvironmentVariable("Path", "$p;C:\spark\bin;C:\hadoop\bin", "User")
```

**પછી બધી PowerShell / VS Code windows બંધ કરીને ફરી ખોલો** (નવા variables ત્યારે જ લાગુ પડે છે).

> GUI થી કરવું હોય તો: Start → "Edit environment variables for your account" → New...

## A4. Installation ચકાસો

નવી PowerShell માં:
```powershell
echo $env:SPARK_HOME      # C:\spark
echo $env:HADOOP_HOME     # C:\hadoop
spark-submit --version    # "version 3.5.1" અને "Using Scala version 2.12" દેખાવું જોઈએ
```

## A5. MySQL JDBC driver ને Spark માં ઉમેરો

> 📝 **Medallion pipeline માટે update:** હવે executors MongoDB (bronze) પણ વાંચે છે. `dev` profile માં `spark.jars` (Mongo connector + MongoDB driver + MySQL driver) પહેલેથી સેટ છે. આ jars `mvn package` વખતે `target/executor-jars` માં copy થાય છે, અને driver તેમને executors ને આપોઆપ મોકલે છે. તેથી નીચેનું copy step હવે **optional** છે.

Executor MySQL માં લખે છે, એટલે driver jar ને `C:\spark\jars` માં copy કરો. આ jar Maven એ પહેલેથી download કરી છે:

```powershell
Copy-Item "$env:USERPROFILE\.m2\repository\com\mysql\mysql-connector-j\8.0.33\mysql-connector-j-8.0.33.jar" C:\spark\jars\
```

> જો file ન મળે તો પહેલા project folder માં `mvn package -DskipTests` ચલાવો.

## A6. Spark Master શરૂ કરો (Terminal 1)

Windows માં `start-master.sh` ચાલતું નથી (તે Linux script છે), એટલે `spark-class` વાપરીએ છીએ.

**નવી PowerShell window #1:**
```powershell
spark-class org.apache.spark.deploy.master.Master --host localhost --port 7077 --webui-port 8080
```
- આ window **ખુલ્લી રાખો** (બંધ કરશો તો master બંધ થશે).
- Browser માં ખોલો: **http://localhost:8080** → "Spark Master at spark://localhost:7077" દેખાશે.

## A7. 2 Worker શરૂ કરો (Terminal 2 અને 3)

**નવી PowerShell window #2 (Worker-1):**
```powershell
spark-class org.apache.spark.deploy.worker.Worker spark://localhost:7077 --cores 1 --memory 2g --webui-port 8081 --work-dir C:\spark\work\worker1
```

**નવી PowerShell window #3 (Worker-2):**
```powershell
spark-class org.apache.spark.deploy.worker.Worker spark://localhost:7077 --cores 1 --memory 2g --webui-port 8082 --work-dir C:\spark\work\worker2
```

Options નો અર્થ:
| Option | અર્થ |
|---|---|
| `spark://localhost:7077` | કયા master સાથે જોડાવું |
| `--cores 1` | આ worker કેટલા CPU cores આપશે |
| `--memory 2g` | આ worker કેટલી RAM આપશે |
| `--webui-port` | Worker નું UI port (બંને worker નું અલગ હોવું જોઈએ) |
| `--work-dir` | Worker ની temp/log files ક્યાં રાખવી |

હવે **http://localhost:8080** refresh કરો → **Alive Workers: 2** દેખાવું જોઈએ. ✅

## A8. ETL project ને dev profile સાથે ચલાવો

`application-dev.properties` માં આ settings છે:
```properties
spark.master=spark://localhost:7077          # Windows cluster નો master
spark.conf.[spark.driver.host]=localhost     # executor આ address પર driver સાથે વાત કરે
spark.conf.[spark.executor.memory]=1g        # દરેક executor ની RAM
spark.conf.[spark.executor.cores]=1          # દરેક executor ના cores
spark.conf.[spark.cores.max]=2               # કુલ 2 cores → દરેક worker પર 1 executor
```

ચલાવવાની 2 રીત (કોઈ પણ એક):

**રીત 1 – VS Code:** `Ctrl+Shift+D` → **"Pipeline - dev (Windows standalone cluster)"** select કરો → **F5**

**રીત 2 – Terminal #4 (project folder માં):**
```powershell
cd D:\ClaudeProjects\dataEngineeringProjects\sparkProject
mvn spring-boot:run "-Dspring-boot.run.profiles=dev"
```

## A9. ચકાસો કે job cluster પર ચાલે છે

1. **http://localhost:8080** → "Running Applications" માં **weather-etl-dev** દેખાશે, Cores = 2.
2. Application પર click કરો → **2 executors** (worker-1 અને worker-2 પર) દેખાશે.
3. **http://localhost:4040** → Spark Application UI: Jobs, Stages, SQL tab માં ETL ના steps.
4. App ના log માં:
   ```
   ETL started for ...data1.json
   Loaded table weather_observation
   ...
   ETL finished, N new observations
   ```

### Fresh data સાથે ફરી test કરવું હોય તો (Optional)
ETL પહેલેથી load થયેલા records skip કરે છે. Cluster પર આખો load જોવા માટે, MySQL માં tables ખાલી કરો (પહેલા child tables, પછી parent):
```sql
USE dataengineering;
DELETE FROM observation_section;
DELETE FROM past_weather_observation;
DELETE FROM present_weather_observation;
DELETE FROM sky_condition_observation;
DELETE FROM marine_observation;
DELETE FROM weather_observation;
```

## A10. Cluster બંધ કરવો
- App: VS Code માં Stop (⏹) અથવા terminal માં `Ctrl+C`
- Worker-1, Worker-2, Master: દરેક window માં `Ctrl+C`

## A11. dev – સામાન્ય સમસ્યાઓ

| સમસ્યા / Error | ઉકેલ |
|---|---|
| `'spark-class' is not recognized` | A3 ફરી કરો, અને નવી terminal ખોલો |
| `HADOOP_HOME and hadoop.home.dir are unset` | A2 + A3 (`HADOOP_HOME`) ચકાસો |
| `UnsatisfiedLinkError ... NativeIO$Windows` | `hadoop.dll` ખૂટે છે → `C:\hadoop\bin` માં મૂકો |
| `No suitable driver` / `ClassNotFoundException: com.mysql.cj.jdbc.Driver` (executor log માં) | A5 – jar `C:\spark\jars` માં copy કરો, પછી workers restart કરો |
| `Initial job has not accepted any resources` | Workers ચાલુ છે? `--memory` (2g) ≥ `spark.executor.memory` (1g) છે? |
| `InvalidClassException ... serialVersionUID` | Cluster નું Spark version 3.5.1 નથી → A1 ફરી કરો |
| Port 8080 already in use | બીજી app (અથવા Docker cluster) 8080 વાપરે છે → બંધ કરો |
| Executor logs ક્યાં? | http://localhost:8080 → Worker → stderr, અથવા `C:\spark\work\worker1\app-...\0\stderr` |

---

# ભાગ B – prod profile: Docker માં Master + 2 Workers

અહીં બધું containers માં ચાલે છે:

```
docker network (sparkproject_default)
 ├─ spark-master    (ports 8080, 7077)
 ├─ spark-worker-1  (UI → localhost:8081)   /data ← ./inputData
 ├─ spark-worker-2  (UI → localhost:8082)   /data ← ./inputData
 └─ etl-app         (Spring Boot = Driver, UI → localhost:4040)   /data ← ./inputData
          │
          └──► MySQL (તમારા Windows પર) via host.docker.internal:3306
```

Files:
| File | કામ |
|---|---|
| `docker/spark.Dockerfile` | Official `spark:3.5.1-scala2.12-java17-ubuntu` image + MySQL driver jar |
| `docker/app.Dockerfile` | Java 17 image માં આપણી Spring Boot jar |
| `docker-compose.yml` | 4 containers (master, 2 workers, app) એક સાથે ચલાવે |
| `application-prod.properties` | Docker માટેના paths, master URL, MySQL host |

## B1. Docker Desktop ચાલુ કરો
1. Docker Desktop install હોવું જોઈએ (તમારા system માં છે: v29.4).
2. **Docker Desktop app ખોલો** અને નીચે "Engine running" (લીલો) થાય ત્યાં સુધી રાહ જુઓ.
3. ચકાસો:
   ```powershell
   docker info
   ```

## B2. MySQL ને Docker માંથી connection આપવાની મંજૂરી આપો

Container માંથી આવતું connection MySQL માટે `localhost` નથી, એટલે `root@localhost` user ચાલશે નહીં. MySQL Workbench માં (root તરીકે) આ ચલાવો:

```sql
CREATE USER IF NOT EXISTS 'root'@'%' IDENTIFIED BY 'Root@123';
GRANT ALL PRIVILEGES ON dataengineering.* TO 'root'@'%';
FLUSH PRIVILEGES;
```

> Windows Firewall એ પૂછે તો MySQL (port 3306) ને **Private network** પર Allow કરો.

## B3. Project ની jar બનાવો
```powershell
cd D:\ClaudeProjects\dataEngineeringProjects\sparkProject
mvn clean package -DskipTests
```
`target\spark-weather-etl-1.0.0.jar` બનવી જોઈએ.

## B4. Cluster શરૂ કરો
```powershell
docker compose up --build -d
```
- પહેલી વાર Spark image (~1 GB) download થશે, થોડી મિનિટ લાગશે.
- `-d` = background માં ચલાવો.

Containers ચકાસો:
```powershell
docker compose ps
```
4 containers **running** દેખાવા જોઈએ: `spark-master`, `spark-worker-1`, `spark-worker-2`, `etl-app`.

## B5. ચકાસો
| શું | ક્યાં |
|---|---|
| Master UI (Alive Workers: 2, Running App: weather-etl-prod) | http://localhost:8080 |
| Worker-1 UI | http://localhost:8081 |
| Worker-2 UI | http://localhost:8082 |
| Spark Application UI | http://localhost:4040 |
| App logs | `docker logs -f etl-app` |

Log માં `ETL finished, N new observations` દેખાય એટલે સફળ. ✅
Processed file copy તમારા Windows folder `inputData\processedFile` માં જ દેખાશે (volume mount ને કારણે).

## B6. Code બદલ્યા પછી ફરી deploy
```powershell
mvn clean package -DskipTests
docker compose up --build -d etl-app
```

## B7. બંધ કરવું
```powershell
docker compose down
```

## B8. prod – સામાન્ય સમસ્યાઓ

| સમસ્યા / Error | ઉકેલ |
|---|---|
| `failed to connect to the docker API ... dockerDesktopLinuxEngine` | Docker Desktop ખોલો, Engine running થવા દો |
| `Host '172.x.x.x' is not allowed to connect` | B2 ની SQL ચલાવો |
| `Communications link failure` | MySQL ચાલુ છે? Firewall માં 3306 allow છે? |
| `COPY target/spark-weather-etl-1.0.0.jar: not found` | પહેલા B3 (`mvn package`) કરો |
| `Initial job has not accepted any resources` | `docker compose ps` – workers ચાલુ છે? Master UI માં workers દેખાય છે? |
| `Path does not exist: file:/data/originalFile/data1.json` | `inputData\originalFile\data1.json` file છે? Docker Desktop → Settings → Resources → File sharing માં D: drive allowed છે? |
| Port 8080 / 7077 busy | dev cluster (ભાગ A) બંધ કરો – બંને એક સાથે ન ચલાવો |

---

# ભાગ C – ટૂંકમાં: કયો profile કેવી રીતે ચલાવવો

| Profile | Cluster શરૂ કરો | App ચલાવો |
|---|---|---|
| default | કંઈ જરૂર નથી | VS Code → "Pipeline - default (local[*])" |
| dev | Master + 2 Workers (A6, A7) | VS Code → "Pipeline - dev (Windows standalone cluster)" અથવા `mvn spring-boot:run "-Dspring-boot.run.profiles=dev"` |
| prod | `docker compose up --build -d` | (app પણ Docker માં જ શરૂ થાય છે) |

### Cluster નું size બદલવું હોય તો
- **વધુ cores/RAM**: worker command માં `--cores` / `--memory` વધારો, અને profile file માં `spark.cores.max`, `spark.executor.memory` વધારો.
- **ત્રીજો worker (dev)**: નવી terminal માં A7 નો command `--webui-port 8083 --work-dir C:\spark\work\worker3` સાથે ચલાવો.
- **ત્રીજો worker (prod)**: `docker-compose.yml` માં `spark-worker-2` block copy કરીને `spark-worker-3` બનાવો (host port `8083:8081`).
- **કોઈ પણ Spark setting**: profile file માં `spark.conf.[<spark key>]=<value>` ઉમેરો – code બદલવાની જરૂર નથી.
