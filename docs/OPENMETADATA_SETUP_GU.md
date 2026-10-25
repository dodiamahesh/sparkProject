# OpenMetadata Setup – Data Lineage અને Governance (ગુજરાતી માર્ગદર્શિકા)

> 📝 **Note:** આ guide જૂના file → MySQL (`dataengineering`) pipeline માટે લખાયો હતો. હવે medallion pipeline છે: `de_source_mongo`, `de_source_mysql`, `de_bronze`, `de_silver`, `de_gold`. OpenMetadata ના install steps (ભાગ A) એ જ રહે છે. Ingestion માં `databaseSchema` ને `de_silver` / `de_gold` કરો. નવા pipeline નું lineage optional આગલું પગલું છે.

આ માર્ગદર્શિકામાં આપણે શીખીશું:
1. OpenMetadata ને Windows 11 પર **Docker** માં install કરવું, **`docker run` commands થી manually** (docker-compose અને Dockerfile વગર)
2. **Airflow વગર** ચલાવવું
3. તમારી Windows MySQL (`dataengineering`) ના tables OpenMetadata માં લાવવા (metadata ingestion)
4. Spark ETL માંથી **Lineage** (કયો data ક્યાંથી ક્યાં ગયો) આપોઆપ OpenMetadata માં મોકલવો

> Versions (tested combination):
> **OpenMetadata 2.0.6**, **Elasticsearch 9.3.0**, **OpenLineage Spark 1.37.0** (Scala 2.12), Spark 3.5.1

---

## 0. પહેલા સમજી લો

### 0.1 આખું Architecture

```
 ┌──────────────────────── Windows 11 (સીધું, Docker વગર) ────────────────────────┐
 │                                                                                  │
 │  Spark Master + Worker-1 + Worker-2         MySQL 8 (port 3306)                  │
 │            ▲                                  ▲          ▲                       │
 │            │ spark://localhost:7077           │ data     │ metadata read         │
 │  ETL jar (Spring Boot = Spark Driver) ────────┘          │ (host.docker.internal)│
 │      │                                                   │                       │
 │      │ lineage events (OpenLineage)                      │                       │
 │      │ http://localhost:8585                             │                       │
 └──────┼───────────────────────────────────────────────────┼───────────────────────┘
        ▼                                                   │
 ┌──────────────────── Docker Desktop (network: om-net) ────┼───────────────────────┐
 │  openmetadata-server  (UI + API, port 8585, 8586)        │                       │
 │        │                    │                            │                       │
 │  postgresql           elasticsearch           ingestion (ફક્ત જરૂર પડે ત્યારે  │
 │  (OM નો પોતાનો DB)    (search)                 docker run --rm થી ચલાવીએ) ──────┘
 └──────────────────────────────────────────────────────────────────────────────────┘
```

### 0.2 કયા containers અને કેમ

| Container | કામ | Port (Windows પર) |
|---|---|---|
| `postgresql` | OpenMetadata નો પોતાનો database (tags, owners, lineage વગેરે અહીં store થાય) | ❌ publish નથી કરતા |
| `elasticsearch` | UI માં search માટે | ❌ publish નથી કરતા |
| `openmetadata-server` | UI + REST API | ✅ **8585** (UI/API), **8586** (admin) |
| `ingestion` | MySQL ના tables વાંચીને OpenMetadata માં મોકલે. **ફક્ત જરૂર હોય ત્યારે** ચલાવીએ અને કામ પૂરું થતાં જ container દૂર થાય (`--rm`) | ❌ |

**Airflow કેમ નથી જોઈતું?** OpenMetadata UI માંથી ingestion ને "schedule" કરવા માટે Airflow વપરાય છે. આપણે ingestion ને **command થી manually** ચલાવીશું (`metadata ingest`), એટલે Airflow ની જરૂર નથી. Server ને `PIPELINE_SERVICE_CLIENT_ENABLED=false` થી કહીએ છીએ કે Airflow નથી.

### 0.3 તમારા હાલના setup સાથે port conflict નથી

| Port | કોણ વાપરે છે |
|---|---|
| 3306 | તમારી Windows MySQL (OpenMetadata PostgreSQL વાપરે છે અને તેનું port publish નથી કરતા) ✅ |
| 7077, 8080, 8081, 8082 | Spark master / workers (Airflow નથી, એટલે 8080 નો conflict નથી) ✅ |
| 4040 | Spark application UI ✅ |
| **8585, 8586** | OpenMetadata (નવા) ✅ |

### 0.4 RAM
OpenMetadata ને આશરે **4 GB** જોઈએ (Elasticsearch ~1.5 GB + Server ~1.5 GB + PostgreSQL). Docker Desktop → **Settings → Resources** માં **Memory ≥ 6 GB** રાખો.

### 0.5 Code માં શું બદલાશે? (ટૂંકમાં)
- **Java code: કોઈ ફેરફાર નહીં.**
- `pom.xml`: OpenLineage ની **1 dependency** ઉમેરવાની.
- Profile properties: **થોડી `spark.conf.[...]` lines** ઉમેરવાની.

OpenLineage એક **Spark Listener** છે. Spark જ્યારે પણ કંઈક વાંચે કે લખે (JSON file, MySQL table), ત્યારે તે જાતે event બનાવીને OpenMetadata ને મોકલે છે. આ listener **ફક્ત Driver (આપણી ETL app)** માં ચાલે છે, એટલે Spark workers માં કંઈ install કરવાનું નથી.

---

## 1. પૂર્વજરૂરિયાતો

| જરૂરિયાત | ચકાસો |
|---|---|
| Docker Desktop ચાલુ છે (Engine running) | `docker info` |
| Windows MySQL ચાલુ છે, version **8.0.42 કે તેથી નવું** | MySQL Workbench માં `SELECT VERSION();` |
| Spark cluster setup (`SPARK_CLUSTER_SETUP_GU.md` ભાગ A) | http://localhost:8080 |

બધા commands **PowerShell** માં ચલાવવાના છે.

---

# ભાગ A – OpenMetadata ને Docker માં install કરવું (manual commands)

## A1. Working folder બનાવો

OpenMetadata ની config files અહીં રાખીશું:
```powershell
New-Item -ItemType Directory -Force C:\openmetadata
```

## A2. Docker network બનાવો

ત્રણેય containers એકબીજાને **નામથી** શોધી શકે (દા.ત. server → `postgresql`), તે માટે એક network:
```powershell
docker network create om-net
```

## A3. Docker volumes બનાવો

Container delete થાય તો પણ data સચવાઈ રહે, તે માટે:
```powershell
docker volume create om-postgres-data
docker volume create om-es-data
```

## A4. Images download કરો (pull)

```powershell
docker pull docker.getcollate.io/openmetadata/postgresql:2.0.6
docker pull docker.elastic.co/elasticsearch/elasticsearch:9.3.0
docker pull docker.getcollate.io/openmetadata/server:2.0.6
docker pull docker.getcollate.io/openmetadata/ingestion:2.0.6
```
> ⏳ પહેલી વાર થોડો સમય લાગશે (ingestion image મોટી છે, આશરે 2–3 GB).

## A5. PostgreSQL container શરૂ કરો

```powershell
docker run -d --name postgresql --network om-net `
  -e POSTGRES_USER=postgres `
  -e POSTGRES_PASSWORD=password `
  -v om-postgres-data:/var/lib/postgresql/data `
  --restart unless-stopped `
  docker.getcollate.io/openmetadata/postgresql:2.0.6 --work_mem=10MB
```

| Option | અર્થ |
|---|---|
| `-d` | Background માં ચલાવો |
| `--name postgresql` | Container નું નામ. Network પર આ જ hostname બને છે |
| `--network om-net` | A2 માં બનાવેલ network |
| `-v om-postgres-data:...` | Data volume માં store થાય |
| `--restart unless-stopped` | Docker Desktop શરૂ થાય ત્યારે container જાતે શરૂ થાય |

આ OpenMetadata ની ખાસ PostgreSQL image છે. તે પોતે જ `openmetadata_db` database અને `openmetadata_user` / `openmetadata_password` user બનાવી દે છે.

**ચકાસો** (છેલ્લે `database system is ready to accept connections` દેખાવું જોઈએ):
```powershell
docker logs postgresql --tail 5
```

## A6. Elasticsearch container શરૂ કરો

```powershell
docker run -d --name elasticsearch --network om-net `
  -e discovery.type=single-node `
  -e ES_JAVA_OPTS="-Xms1024m -Xmx1024m" `
  -e xpack.security.enabled=false `
  -v om-es-data:/usr/share/elasticsearch/data `
  --restart unless-stopped `
  docker.elastic.co/elasticsearch/elasticsearch:9.3.0
```

| Option | અર્થ |
|---|---|
| `discovery.type=single-node` | એક જ node (local માટે) |
| `ES_JAVA_OPTS` | Elasticsearch ને 1 GB RAM |
| `xpack.security.enabled=false` | Local માટે password/SSL બંધ |

**ચકાસો** (30–60 સેકન્ડ રાહ જુઓ, `"status":"green"` અથવા `"yellow"` આવવું જોઈએ):
```powershell
docker exec elasticsearch curl -s http://localhost:9200/_cluster/health
```

## A7. Server ની settings file (env file) બનાવો

Server અને migration બંને માટે એક જ settings જોઈએ, એટલે એક file માં રાખીએ:

```powershell
@"
DB_DRIVER_CLASS=org.postgresql.Driver
DB_SCHEME=postgresql
DB_HOST=postgresql
DB_PORT=5432
DB_USER=openmetadata_user
DB_USER_PASSWORD=openmetadata_password
OM_DATABASE=openmetadata_db
DB_PARAMS=allowPublicKeyRetrieval=true&useSSL=false&serverTimezone=UTC
ELASTICSEARCH_HOST=elasticsearch
ELASTICSEARCH_PORT=9200
ELASTICSEARCH_SCHEME=http
SEARCH_TYPE=elasticsearch
SERVER_HOST_API_URL=http://openmetadata-server:8585/api
PIPELINE_SERVICE_CLIENT_ENABLED=false
OPENMETADATA_HEAP_OPTS=-Xmx1G -Xms1G
"@ | Set-Content -Encoding ascii C:\openmetadata\om.env
```

| Setting | અર્થ |
|---|---|
| `DB_*`, `OM_DATABASE` | A5 નું PostgreSQL (hostname = container નામ `postgresql`) |
| `ELASTICSEARCH_*` | A6 નું Elasticsearch |
| `PIPELINE_SERVICE_CLIENT_ENABLED=false` | **Airflow નથી** |
| `OPENMETADATA_HEAP_OPTS` | Server ને 1 GB RAM |

> ⚠️ `-Encoding ascii` જરૂરી છે. Notepad ના "UTF-8 with BOM" થી પહેલી line ખરાબ થઈ જાય છે.

## A8. Database migration ચલાવો (ફક્ત પહેલી વાર અને upgrade વખતે)

OpenMetadata ના tables PostgreSQL માં બનાવે છે:
```powershell
docker run --rm --name om-migrate --network om-net `
  --env-file C:\openmetadata\om.env `
  docker.getcollate.io/openmetadata/server:2.0.6 ./bootstrap/openmetadata-ops.sh migrate
```
છેલ્લે errors વગર પૂરું થવું જોઈએ. `--rm` ને કારણે કામ પૂરું થતાં container જાતે delete થાય છે.

## A9. OpenMetadata Server શરૂ કરો

```powershell
docker run -d --name openmetadata-server --network om-net `
  --env-file C:\openmetadata\om.env `
  -p 8585:8585 -p 8586:8586 `
  --restart unless-stopped `
  docker.getcollate.io/openmetadata/server:2.0.6
```

**ચકાસો** (1–2 મિનિટ લાગે):
```powershell
docker logs openmetadata-server --tail 20
```
પછી browser માં ખોલો: **http://localhost:8585**

| Login | Value |
|---|---|
| Username | `admin@open-metadata.org` |
| Password | `admin` |

✅ OpenMetadata UI ખુલી જાય એટલે install પૂરું.

## A10. Bot JWT Token મેળવો

ETL અને ingestion બંને OpenMetadata સાથે આ token થી વાત કરશે.

1. UI → **Settings** → **Bots** → **ingestion-bot**
2. **JWT Token** copy કરો
3. તેને Windows ના User Environment Variable તરીકે save કરો, જેથી token code માં ન લખવો પડે:
   ```powershell
   [Environment]::SetEnvironmentVariable("OM_JWT_TOKEN", "<અહીં token paste કરો>", "User")
   ```
4. VS Code / PowerShell **બંધ કરીને ફરી ખોલો**.

---

# ભાગ B – MySQL ના tables OpenMetadata માં લાવવા (Metadata Ingestion)

**કેમ જરૂરી?** OpenMetadata lineage ને **પહેલેથી હાજર tables** સાથે જ જોડી શકે છે. તેથી **પહેલા ingestion, પછી lineage**.

## B1. MySQL માં read-only user બનાવો

Container માંથી આવતું connection `localhost` નથી, એટલે `'%'` host સાથે user બનાવો. MySQL Workbench માં (root તરીકે):
```sql
CREATE USER IF NOT EXISTS 'om_reader'@'%' IDENTIFIED BY 'OmReader@123';
GRANT SELECT, SHOW VIEW ON dataengineering.* TO 'om_reader'@'%';
FLUSH PRIVILEGES;
```
> `SELECT` થી tables, columns અને sample data વંચાશે. `SHOW VIEW` થી views વંચાશે. આ user ફક્ત વાંચી શકે છે, કંઈ બદલી શકતો નથી.

## B2. Ingestion config file બનાવો

File: `C:\openmetadata\mysql_ingestion.yaml`
```yaml
source:
  type: mysql
  serviceName: local_mysql            # OpenMetadata માં MySQL service નું નામ
  serviceConnection:
    config:
      type: Mysql
      username: om_reader
      authType:
        password: OmReader@123
      hostPort: host.docker.internal:3306   # container માંથી Windows MySQL
      databaseSchema: dataengineering
  sourceConfig:
    config:
      type: DatabaseMetadata
sink:
  type: metadata-rest
  config: {}
workflowConfig:
  openMetadataServerConfig:
    hostPort: http://openmetadata-server:8585/api   # om-net network પર server
    authProvider: openmetadata
    securityConfig:
      jwtToken: "<A10 નો token અહીં paste કરો>"
```

| Field | અર્થ |
|---|---|
| `serviceName: local_mysql` | પહેલી વાર ચલાવો ત્યારે આ નામની service **જાતે બને છે** |
| `host.docker.internal` | Container માંથી તમારી Windows machine |
| `openmetadata-server:8585` | Ingestion container પણ `om-net` પર છે, એટલે server ને નામથી શોધે |

## B3. Ingestion ચલાવો

```powershell
docker run --rm --network om-net `
  -v C:\openmetadata:/config `
  --entrypoint /bin/bash `
  docker.getcollate.io/openmetadata/ingestion:2.0.6 `
  -c "metadata ingest -c /config/mysql_ingestion.yaml"
```

| Option | અર્થ |
|---|---|
| `--rm` | કામ પૂરું થતાં container delete થાય. આ Airflow નું કામ manual રીતે કરીએ છીએ |
| `-v C:\openmetadata:/config` | YAML file container ને આપો |
| `--entrypoint /bin/bash -c "metadata ingest ..."` | Image માં Airflow શરૂ કરવાને બદલે ફક્ત `metadata` CLI ચલાવો |

છેલ્લે **Workflow Summary** માં `Success %: 100` અને tables ની સંખ્યા દેખાશે.

## B4. ચકાસો

UI → **Explore → Tables** → `local_mysql` → `dataengineering` → 6 tables દેખાશે:
`weather_observation`, `observation_section`, `past_weather_observation`, `present_weather_observation`, `sky_condition_observation`, `marine_observation`

> 🔁 **ક્યારે ફરી ચલાવવું?** જ્યારે MySQL માં નવું table કે column ઉમેરાય ત્યારે B3 ફરી ચલાવો. Airflow નથી, એટલે આ જાતે ચાલતું નથી.

---

# ભાગ C – ETL project માં Lineage ચાલુ કરવું

## C1. `pom.xml` માં dependency ઉમેરો

`<dependencies>` ની અંદર:
```xml
<!-- Lineage: Spark read/write events ને OpenMetadata માં મોકલે છે -->
<dependency>
    <groupId>io.openlineage</groupId>
    <artifactId>openlineage-spark_2.12</artifactId>
    <version>1.37.0</version>
</dependency>
```
- `_2.12` એ Scala version છે અને આપણા Spark (Scala 2.12) સાથે match થવું જ જોઈએ.
- આ jar ફક્ત driver માં જોઈએ. Spark workers (`C:\spark\jars`) માં copy કરવાની **જરૂર નથી**.

## C2. Profile properties માં lineage settings ઉમેરો

`src/main/resources/application-dev.properties` ના અંતે ઉમેરો:
```properties
# ---------- OpenMetadata lineage (OpenLineage) ----------
spark.conf.[spark.extraListeners]=io.openlineage.spark.agent.OpenLineageSparkListener
spark.conf.[spark.openlineage.transport.type]=http
spark.conf.[spark.openlineage.transport.url]=http://localhost:8585
spark.conf.[spark.openlineage.transport.endpoint]=/api/v1/openlineage/lineage
spark.conf.[spark.openlineage.transport.auth.type]=api_key
spark.conf.[spark.openlineage.transport.auth.apiKey]=${OM_JWT_TOKEN}
spark.conf.[spark.openlineage.namespace]=weather_etl
spark.conf.[spark.openlineage.parentJobName]=weather_etl_pipeline
```

| Property | અર્થ |
|---|---|
| `spark.extraListeners` | Spark માં OpenLineage listener ઉમેરે છે |
| `transport.url` + `endpoint` | Events ક્યાં મોકલવા. ETL Windows પર ચાલે છે, એટલે `localhost:8585` |
| `auth.apiKey=${OM_JWT_TOKEN}` | A10 નો token, environment variable માંથી (code માં secret નથી) |
| `namespace` | આ Spark app ની ઓળખ |
| `parentJobName` | OpenMetadata માં pipeline નું નામ બનશે: **`weather_etl-weather_etl_pipeline`** |

> 💡 `default` profile (IDE, `local[*]`) માં પણ lineage જોઈતું હોય તો આ જ lines `application.properties` માં મૂકો.

## C3. Build અને Run

```powershell
cd D:\ClaudeProjects\dataEngineeringProjects\sparkProject
mvn clean package -DskipTests
mvn spring-boot:run "-Dspring-boot.run.profiles=dev"
```
અથવા VS Code → **"Pipeline - dev (Windows standalone cluster)"** → F5.
(Spark master + 2 workers પહેલેથી ચાલુ હોવા જોઈએ. જુઓ `SPARK_CLUSTER_SETUP_GU.md` ભાગ A.)

## C4. Lineage જુઓ

1. UI → **Explore → Pipelines** → service **`openlineage`** → pipeline **`weather_etl-weather_etl_pipeline`**
2. **Lineage** tab ખોલો. Pipeline અને MySQL tables વચ્ચેનું graph દેખાશે.
3. અથવા કોઈ પણ table (દા.ત. `marine_observation`) ખોલો → **Lineage** tab.

### જો tables lineage સાથે ન જોડાય
OpenLineage MySQL ને `mysql://localhost:3306` જેવા namespace થી ઓળખે છે, જ્યારે OpenMetadata માં service નું નામ `local_mysql` છે. બંનેને જોડવા માટે OpenMetadata ના **OpenLineage settings** માં **namespace → service mapping** ઉમેરો: `mysql://localhost:3306` → `local_mysql`.

UI → **Settings** માં "OpenLineage" શોધો. Menu નું ચોક્કસ નામ version પ્રમાણે બદલાઈ શકે છે, એટલે OpenMetadata ની docs (Spark Lineage page) પણ જોઈ લેજો.

---

# ભાગ D – Governance (બધું UI માં, code વગર)

Tables ingest થઈ ગયા પછી દરેક table/column પર:

| Feature | ક્યાં | ઉપયોગ |
|---|---|---|
| **Description** | Table → Edit | Table/column શું છે તે લખો |
| **Owner** | Table → Owner | આ data ની જવાબદારી કોની |
| **Tags** | Table/Column → Tags | દા.ત. `PII.Sensitive`, `Tier.Tier1` |
| **Glossary** | Govern → Glossary | Business શબ્દો (દા.ત. "Observation", "Sea Surface Temperature") |
| **Domain** | Govern → Domains | દા.ત. "Weather" domain માં બધા tables |
| **Data Quality** | Table → Data Observability → Add Test | દા.ત. `id` unique, `air_temperature` NULL નથી |

> Profiler અને Data Quality tests ને પણ ચાલવા માટે ingestion જોઈએ. Airflow નથી, એટલે તેમને પણ B3 ની જેમ `docker run ... metadata profile -c ...` / `metadata test -c ...` થી manually ચલાવવા પડે.

---

# ભાગ E – રોજિંદા ઉપયોગ

## E1. Start / Stop
`--restart unless-stopped` ને કારણે Docker Desktop શરૂ થતાં containers જાતે શરૂ થાય છે. Manually:
```powershell
# Start (આ ક્રમમાં)
docker start postgresql elasticsearch
docker start openmetadata-server

# Stop
docker stop openmetadata-server elasticsearch postgresql

# Status
docker ps --filter network=om-net
```

## E2. આખું OpenMetadata દૂર કરવું (data સહિત)
```powershell
docker rm -f openmetadata-server elasticsearch postgresql
docker volume rm om-postgres-data om-es-data
docker network rm om-net
```

## E3. રોજનો ક્રમ
```
1. Docker Desktop ખોલો          → OpenMetadata જાતે શરૂ થાય (http://localhost:8585)
2. Spark master + 2 workers     → SPARK_CLUSTER_SETUP_GU.md A6, A7
3. ETL (dev profile)            → દર 2 મિનિટે lineage OpenMetadata માં જાય
4. નવું table/column ઉમેર્યું?  → B3 ingestion ફરી ચલાવો
```

---

# ભાગ F – સામાન્ય સમસ્યાઓ

| સમસ્યા / Error | ઉકેલ |
|---|---|
| `docker: ... dockerDesktopLinuxEngine` | Docker Desktop ખોલો અને Engine running થવા દો |
| Elasticsearch વારંવાર બંધ થાય (`docker ps` માં નથી) | RAM ઓછી છે → Docker Desktop memory ≥ 6 GB. Logs: `docker logs elasticsearch` |
| `max virtual memory areas vm.max_map_count [65530] is too low` | `wsl -d docker-desktop sysctl -w vm.max_map_count=262144` અને પછી `docker restart elasticsearch` |
| Server શરૂ ન થાય, logs માં DB error | A8 migration ચલાવ્યું? `docker logs postgresql` તપાસો |
| http://localhost:8585 ન ખૂલે | `docker logs openmetadata-server` – 1–2 મિનિટ રાહ જુઓ |
| Ingestion: `Access denied for user 'om_reader'` | B1 ની SQL ચલાવો (host `'%'` જ જોઈએ) |
| Ingestion: `Can't connect to MySQL server on 'host.docker.internal'` | MySQL ચાલુ છે? Windows Firewall માં 3306 allow છે? |
| Ingestion: `401 Unauthorized` | YAML માં JWT token ખોટો કે અધૂરો છે → A10 ફરી copy કરો |
| ETL log: `Connection refused ... localhost:8585` | OpenMetadata server ચાલુ નથી. ETL ચાલતું રહે છે, ફક્ત lineage નથી જતું |
| ETL log માં `Failed to load OpenLineageExtensionProvider class` warning | **નુકસાન નથી.** Spring Boot jar ના classloader ને કારણે આવે છે, lineage કામ કરે છે |
| Pipeline દેખાય પણ tables સાથે જોડાયેલું ન હોય | પહેલા ભાગ B ingestion કરો, પછી C4 ની namespace mapping તપાસો |

### ધ્યાનમાં રાખવા જેવું
- **Input JSON file lineage graph માં દેખાશે નહીં.** OpenMetadata માં local file જેવી entity નથી. File ને lineage માં લાવવી હોય તો input MinIO/S3 પર રાખવું પડે.
- **`weather_observation` graph માં input તરીકે પણ દેખાશે.** આપણો ETL duplicate check માટે આ table **વાંચે** પણ છે, એટલે lineage graph માં તે input તરીકે પણ દેખાઈ શકે. તે સાચું જ છે.
- **ETL દર 2 મિનિટે ચાલે છે**, એટલે OpenMetadata માં pipeline ના runs પણ દર 2 મિનિટે ઉમેરાશે.
