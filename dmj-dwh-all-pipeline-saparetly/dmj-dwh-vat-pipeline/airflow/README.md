# Running dmj-dwh from Airflow

One jar holds every pipeline (today only `vat-return-pipeline`). Each pipeline has its **own DAG**. A DAG task
starts the jar in **batch mode**:

```bash
java -jar dmj-dwh-1.0.0.jar --spring.profiles.active=prod --run-pipeline=vat-return-pipeline
```

- Only `vat-return-pipeline` runs. There is no web server, and the process exits when the run ends.
- The run is recorded in `meta.meta_pipeline_run` exactly like a REST run.

| Exit code | When | In Airflow |
|---|---|---|
| `0` | `SUCCESS`, or `SUCCESS_WITH_FAILED` (rejected rows are in `meta.meta_rejected_record`) | task succeeds, dependent DAGs start |
| `1` | `FAILED`, unknown pipeline, pipeline already running, database down | task fails, Airflow retries, dependent DAGs wait |

To make `SUCCESS_WITH_FAILED` fail the task as well, set `DWH_FAIL_ON_REJECTED_RECORDS=true` in the pipeline's `env`.

The last line of every task log is easy to search for:

```
DWH_RESULT pipeline=vat-return-pipeline pipelineRunId=42 status=SUCCESS_WITH_FAILED total=21 success=14 failed=7 errorCode=null exitCode=0
```

## Files

| File | What |
|---|---|
| `dags/dmj_dwh_dags.py` | Generates one DAG per entry of the YAML file. Copy it to the Airflow `dags/` folder. |
| `dags/dmj_dwh_pipelines.yaml` | The pipelines: schedule **or** `depends_on`, java / jar / env. Copy it next to the `.py` file. |

## Dependencies between DAGs

```yaml
vat-return-pipeline:
  schedule: "0 3 * * *"
vat-report-pipeline:                  # example: a future pipeline that needs the VAT tables
  depends_on: [vat-return-pipeline]
```

- Every task publishes the asset (dataset) `dmjdwh://pipeline/<name>`, but **only when it succeeds**.
- A DAG with `depends_on` is scheduled on those assets. It starts once **all** its upstream pipelines have succeeded since its own last run.
- A failed upstream run publishes nothing, so the downstream DAG does not start.

This uses Airflow data-aware scheduling: Airflow 2.4+ (`Dataset`) and Airflow 3 (`Asset`).

**DAG id = pipeline name**, exactly the same: the pipeline `vat-return-pipeline` has the DAG `vat-return-pipeline`. The application stores it in `meta.meta_pipeline.airflow_dag_id` (equal to `pipeline_name`). A pipeline name may only contain letters, digits, `-`, `_` and `.` (Airflow rule) and at most 30 characters; the application refuses to start otherwise.

## Setup (once)

1. **Build** the jar with `mvn clean package`. This produces `target/dmj-dwh-1.0.0.jar`.
2. **Copy** the jar to every Airflow worker, at the path given in `defaults.jar` (default `/opt/dmj-dwh/dmj-dwh-1.0.0.jar`). The worker needs Java 17. The jar sets the Spark `--add-opens` flags itself.
3. **Copy** the 2 files of `dags/` to the Airflow `dags/` folder.
4. **Create** the Airflow Connection `dmj_dwh_postgres`:
   - type Postgres
   - host, port, schema = database name, login, password
5. **Create** the Airflow Variable `dmj_dwh_mongo_uri`, for example `mongodb://user:pass@mongo-host:27017`.
6. **Create the PostgreSQL tables** once by running `sample-data/vat-return-pipeline/03_postgres_tables.sql`. The application never creates tables itself.

The `env` values in the YAML are Jinja templates. `{{ conn.<id>.<field> }}` reads an Airflow Connection and `{{ var.value.<key> }}` reads an Airflow Variable, so no password is written in a file.

## Adding a pipeline

1. Write the pipeline in the code (a `@Component` that extends `AbstractEtlPipeline`). Build and copy the jar.
2. Add one entry to `dmj_dwh_pipelines.yaml`. Airflow shows the new DAG on its next parse of the folder.

## Safety

- **Same pipeline, two processes:** the jar takes a PostgreSQL advisory lock per pipeline for the whole run. A second process of the same pipeline exits with `1` ("already running"). This also applies between the REST service and Airflow. Different pipelines run in parallel.
- **Airflow setting:** `max_active_runs=1` is set on every DAG.
- **A process that dies** (killed, out of memory, worker lost):
  - PostgreSQL drops its connection, which releases the lock.
  - Its run row stays `RUNNING` until the next start of that pipeline, which marks it `FAILED` / `INTERRUPTED`.
  - `last_run_time` did not move, so the next run reads the same data again.

**Several pipelines at the same time:** each Airflow task is its own JVM, so one job's `exit` never stops another.
CPU and RAM of the worker are shared: see [../docs/airflow-execution-jar-running-on-multiple-jvm.md](../docs/airflow-execution-jar-running-on-multiple-jvm.md)
(`SPARK_MASTER`, `java_options`, Airflow pool).
