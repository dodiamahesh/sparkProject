"""
# Airflow – All Scenarios in ONE DAG file (learning reference)

Works on **Airflow 2.7+** and **Airflow 3.x** (imports are auto-detected).

Drop this file in your `dags/` folder. It registers 3 DAGs:

| DAG id                                   | Purpose                                              |
|------------------------------------------|------------------------------------------------------|
| `airflow_all_scenarios`                  | MAIN DAG – shows almost every Airflow feature        |
| `airflow_all_scenarios_child`            | Triggered by the main DAG (TriggerDagRunOperator)    |
| `airflow_all_scenarios_dataset_consumer` | Data-aware schedule – runs when main DAG updates the Dataset/Asset |

## Scenarios covered (search the number in this file, e.g. "[S07]")

| #   | Scenario                                                        |
|-----|-----------------------------------------------------------------|
| S01 | DAG config: schedule (cron), start_date + timezone, catchup, max_active_runs, tags, dagrun_timeout, doc_md |
| S02 | default_args: owner, retries, retry_delay, exponential backoff, execution_timeout |
| S03 | DAG Params (UI "Trigger with config") with type validation      |
| S04 | Callbacks: on_success / on_failure / on_retry / on_execute       |
| S05 | EmptyOperator (start / end markers)                             |
| S06 | BashOperator + Jinja templating + macros + env vars + depends_on_past |
| S07 | TaskFlow API `@task` + `multiple_outputs`                        |
| S08 | Variables & Connections (read safely inside tasks, never at top level) |
| S09 | Classic PythonOperator + op_kwargs templating + manual XCom push/pull |
| S10 | TaskGroup (classic) + NESTED TaskGroup                          |
| S11 | Dynamic Task Mapping: `.partial().expand()`, `.expand_kwargs()`, mapping a classic operator |
| S12 | Reduce step over mapped results                                 |
| S13 | `@task_group` decorator                                          |
| S14 | Branching `@task.branch` (single + multiple branches) + edge Labels |
| S15 | Trigger rules: none_failed_min_one_success, one_failed, all_done, none_failed |
| S16 | ShortCircuit `@task.short_circuit`                               |
| S17 | Datasets / Assets (producer `outlets=`) + data-aware consumer DAG |
| S18 | TriggerDagRunOperator with conf → child DAG reads `dag_run.conf` |
| S19 | Setup / Teardown tasks (like create cluster → job → delete cluster) |
| S20 | Spark job via BashOperator + Jinja `{% if %}` (SparkSubmitOperator shown in comments) |
| S21 | Sensor `@task.sensor` – mode="reschedule", timeout, soft_fail     |
| S22 | Retries demo – task fails on 1st try then succeeds              |
| S23 | AirflowFailException (fail without retry) / AirflowSkipException |
| S24 | LatestOnlyOperator (skip on backfill / old runs)                |
| S25 | Custom Operator (BaseOperator subclass + template_fields)        |
| S26 | Pools, priority_weight, weight_rule, max_active_tis_per_dag       |
| S27 | chain() helper                                                   |
| S28 | `dag.test()` – run the whole DAG locally with `python this_file.py` |

## How to play with it
* Trigger normally → everything is green (flaky task retries once by design).
* Trigger with config `{"simulate_failure": true}` → `alert_on_failure` runs, DAG goes red.
* Trigger with `{"env": "prod"}` / `{"env": "qa"}` → see different branches.
* Trigger with `{"batch_size": 0}` is rejected by Param validation (minimum=1).
* Trigger with `{"sensor_auto_pass": false}` → sensor waits for the flag file, then soft-fails (skipped).
* Create Variable `demo_variable` and Connection `demo_conn` in the UI to see them used.
"""

from __future__ import annotations

import logging
import os
import shutil
import tempfile
from datetime import timedelta

import pendulum

# ---------------------------------------------------------------------------
# Imports that work on BOTH Airflow 3.x and Airflow 2.x
# ---------------------------------------------------------------------------
try:  # ---------- Airflow 3.x (Task SDK) ----------
    from airflow.sdk import (
        DAG,
        BaseHook,
        BaseOperator,
        Label,
        Param,
        TaskGroup,
        Variable,
        chain,
        dag,
        get_current_context,
        task,
        task_group,
    )

    AIRFLOW_3 = True
except ImportError:  # ---------- Airflow 2.x ----------
    from airflow import DAG
    from airflow.decorators import dag, task, task_group
    from airflow.hooks.base import BaseHook
    from airflow.models import Variable
    from airflow.models.baseoperator import BaseOperator, chain
    from airflow.models.param import Param
    from airflow.operators.python import get_current_context
    from airflow.utils.edgemodifier import Label
    from airflow.utils.task_group import TaskGroup

    AIRFLOW_3 = False

try:  # Airflow 3.x renamed Dataset → Asset
    from airflow.sdk import Asset

    HAS_ASSET = True
except ImportError:  # Airflow 2.4+
    from airflow.datasets import Dataset

    HAS_ASSET = False

try:  # Airflow 3.x – core operators live in the "standard" provider
    from airflow.providers.standard.operators.bash import BashOperator
    from airflow.providers.standard.operators.empty import EmptyOperator
    from airflow.providers.standard.operators.latest_only import LatestOnlyOperator
    from airflow.providers.standard.operators.python import PythonOperator
    from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
except ImportError:  # Airflow 2.x
    from airflow.operators.bash import BashOperator
    from airflow.operators.empty import EmptyOperator
    from airflow.operators.latest_only import LatestOnlyOperator
    from airflow.operators.python import PythonOperator
    from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from airflow.exceptions import AirflowFailException, AirflowSkipException

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
LOCAL_TZ = "Asia/Kolkata"
DEMO_DIR = os.path.join(tempfile.gettempdir(), "airflow_demo")
FLAG_FILE = os.path.join(DEMO_DIR, "ready.flag")
SUMMARY_FILE = os.path.join(DEMO_DIR, "sales_summary.csv")

# [S17] Dataset (Airflow 2) / Asset (Airflow 3) – a logical pointer to data.
if HAS_ASSET:
    SALES_DATASET = Asset(name="sales_summary", uri=f"file://{SUMMARY_FILE}")
else:
    SALES_DATASET = Dataset(f"file://{SUMMARY_FILE}")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def get_variable(key: str, default=None):
    """[S08] Read an Airflow Variable safely (signature differs in Airflow 2 vs 3).

    Best practice: call this INSIDE a task, never at module top level –
    top-level code runs on every DAG parse (every ~30s) and hits the metadata DB.
    """
    try:
        try:
            return Variable.get(key, default=default)  # Airflow 3 signature
        except TypeError:
            return Variable.get(key, default_var=default)  # Airflow 2 signature
    except Exception:  # noqa: BLE001 – variable/DB not available
        return default


# ---------------------------------------------------------------------------
# [S04] Callbacks – receive the task context dict
# ---------------------------------------------------------------------------
def on_task_failure(context):
    ti = context["task_instance"]
    log.error(
        "TASK FAILED  dag=%s task=%s run=%s error=%s",
        ti.dag_id, ti.task_id, context["run_id"], context.get("exception"),
    )
    # Real life: send Slack / Teams / PagerDuty / email here.


def on_task_retry(context):
    ti = context["task_instance"]
    log.warning("TASK RETRY  task=%s try_number=%s", ti.task_id, ti.try_number)


def on_task_success(context):
    log.info("TASK SUCCESS  task=%s", context["task_instance"].task_id)


def on_task_execute(context):
    log.info("TASK STARTING  task=%s", context["task_instance"].task_id)


def on_dag_success(context):
    log.info("DAG RUN SUCCESS  run_id=%s", context["run_id"])


def on_dag_failure(context):
    log.error("DAG RUN FAILED  run_id=%s reason=%s", context["run_id"], context.get("reason"))


# ---------------------------------------------------------------------------
# [S25] Custom Operator
# ---------------------------------------------------------------------------
class GreetOperator(BaseOperator):
    """A tiny custom operator. `template_fields` lets users pass Jinja into `name`."""

    template_fields = ("name",)
    ui_color = "#ffe5b4"

    def __init__(self, name: str, **kwargs):
        super().__init__(**kwargs)
        self.name = name

    def execute(self, context):
        message = f"Hello {self.name}! run_id={context['run_id']}"
        self.log.info(message)
        return message  # return value is pushed to XCom automatically


# ---------------------------------------------------------------------------
# [S09] Classic python callable (used with PythonOperator)
# ---------------------------------------------------------------------------
def classic_python_fn(run_date: str, env: str, **context):
    ti = context["ti"]
    # Pull one key of a multiple_outputs task
    batch_size = ti.xcom_pull(task_ids="read_config", key="batch_size")
    log.info("Classic PythonOperator  run_date=%s env=%s batch_size=%s", run_date, env, batch_size)
    # Manual push with a custom key
    ti.xcom_push(key="classic_message", value=f"processed {batch_size} rows for {run_date}")
    return "classic_done"  # also pushed as key='return_value'


# ---------------------------------------------------------------------------
# [S02] default_args – applied to every task (a task can override any of them)
# ---------------------------------------------------------------------------
default_args = {
    "owner": "data_engineering",
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(minutes=30),
    "email_on_failure": False,  # True + "email": [...] needs SMTP configured
    "email_on_retry": False,
    "on_failure_callback": on_task_failure,
    "on_retry_callback": on_task_retry,
}

# ===========================================================================
# MAIN DAG
# ===========================================================================
with DAG(
    dag_id="airflow_all_scenarios",
    description="One DAG that demonstrates (almost) every Airflow scenario",
    doc_md=__doc__,
    # [S01] DAG-level config
    schedule="0 6 * * *",  # daily 06:00 (also: "@daily", timedelta(hours=1), None, [dataset])
    start_date=pendulum.datetime(2026, 1, 1, tz=LOCAL_TZ),
    catchup=False,  # True = backfill every missed interval since start_date
    max_active_runs=1,
    max_active_tasks=16,
    dagrun_timeout=timedelta(hours=2),
    default_args=default_args,
    tags=["learning", "all-scenarios"],
    on_success_callback=on_dag_success,
    on_failure_callback=on_dag_failure,
    # [S03] Params – shown as a form in "Trigger DAG w/ config"
    params={
        "env": Param("dev", type="string", enum=["dev", "qa", "prod"], description="Target environment"),
        "batch_size": Param(3, type="integer", minimum=1, maximum=10, description="Rows per source"),
        "simulate_flaky": Param(True, type="boolean", description="Flaky task fails on 1st try"),
        "simulate_failure": Param(False, type="boolean", description="Hard-fail a task"),
        "sensor_auto_pass": Param(True, type="boolean", description="Sensor passes immediately"),
        "run_spark": Param(False, type="boolean", description="Really call spark-submit"),
        "spark_app": Param("/opt/spark-apps/etl_job.py", type="string"),
        "owner_name": Param("Data Team", type="string"),
    },
) as main_dag:

    # -----------------------------------------------------------------------
    # [S05] Start marker
    # -----------------------------------------------------------------------
    start = EmptyOperator(task_id="start", on_execute_callback=on_task_execute)

    # -----------------------------------------------------------------------
    # [S06] BashOperator + Jinja templates + macros + env + depends_on_past
    # -----------------------------------------------------------------------
    print_run_info = BashOperator(
        task_id="print_run_info",
        bash_command="""
            echo "ds            = {{ ds }}"
            echo "ds_nodash     = {{ ds_nodash }}"
            echo "yesterday     = {{ macros.ds_add(ds, -1) if ds else 'n/a' }}"
            echo "run_id        = {{ run_id }}"
            echo "interval      = {{ data_interval_start }} -> {{ data_interval_end }}"
            echo "param env     = {{ params.env }}"
            echo "variable      = {{ var.value.get('demo_variable', 'not-set') }}"
            echo "env var       = $RUN_ENV"
        """,
        env={"RUN_ENV": "{{ params.env }}"},
        append_env=True,  # keep PATH etc. from the worker
        depends_on_past=True,  # waits for this task's success in the previous run
        on_success_callback=on_task_success,
    )

    # -----------------------------------------------------------------------
    # [S07] TaskFlow + multiple_outputs   |   [S08] Variables & Connections
    # -----------------------------------------------------------------------
    @task(multiple_outputs=True)
    def read_config(**context):
        params = context["params"]
        source_system = get_variable("demo_variable", default="default_source")

        try:
            conn = BaseHook.get_connection("demo_conn")
            conn_host = conn.host
        except Exception:  # noqa: BLE001 – connection not created yet
            conn_host = "localhost"

        # Each dict key becomes its own XCom key (because multiple_outputs=True)
        return {
            "env": params["env"],
            "batch_size": params["batch_size"],
            "source_system": source_system,
            "conn_host": conn_host,
        }

    cfg = read_config()

    # -----------------------------------------------------------------------
    # [S09] Classic PythonOperator
    # -----------------------------------------------------------------------
    classic_task = PythonOperator(
        task_id="classic_python_task",
        python_callable=classic_python_fn,
        op_kwargs={"run_date": "{{ ds }}", "env": "{{ params.env }}"},  # templated!
    )

    # -----------------------------------------------------------------------
    # [S10] TaskGroup + nested TaskGroup  (Extract)
    # -----------------------------------------------------------------------
    with TaskGroup(group_id="extract", tooltip="Extract from all sources") as extract_tg:

        @task
        def extract_api(batch_size: int):
            rows = [{"id": i, "source": "api", "amount": i * 10} for i in range(1, batch_size + 1)]
            log.info("API rows: %s", rows)
            return rows

        with TaskGroup(group_id="database") as db_tg:  # nested group → id "extract.database"

            @task
            def connect_db(host: str):
                log.info("Connecting to DB at %s", host)
                return host

            @task
            def query_db(host: str, batch_size: int):
                return [{"id": 100 + i, "source": f"db@{host}", "amount": i * 5} for i in range(1, batch_size + 1)]

            db_rows = query_db(connect_db(cfg["conn_host"]), cfg["batch_size"])

        api_rows = extract_api(cfg["batch_size"])

    # -----------------------------------------------------------------------
    # [S11] Dynamic Task Mapping   |   [S12] Reduce
    # -----------------------------------------------------------------------
    with TaskGroup(group_id="transform") as transform_tg:

        @task
        def validate(api: list, db: list):
            rows = [r for r in api + db if r["amount"] >= 0]
            log.info("Validated %d rows", len(rows))
            return rows

        @task(
            max_active_tis_per_dag=4,  # [S26] at most 4 mapped copies run in parallel
            pool="default_pool",  # [S26] create custom pools in Admin → Pools
            priority_weight=5,
            weight_rule="downstream",
        )
        def process_record(record: dict, multiplier: int):
            # One task instance is created PER record at runtime
            return record["amount"] * multiplier

        @task
        def aggregate(values):
            total = sum(values)  # `values` is a lazy list of all mapped results
            log.info("Total amount = %s", total)
            return total

        @task
        def build_partition_kwargs(rows: list):
            sources = sorted({r["source"] for r in rows})
            return [{"partition": s, "row_count": sum(1 for r in rows if r["source"] == s)} for s in sources]

        @task
        def write_partition(partition: str, row_count: int):
            log.info("Writing partition=%s rows=%s", partition, row_count)

        validated = validate(api_rows, db_rows)

        # .partial() = constant args, .expand() = mapped args
        processed = process_record.partial(multiplier=2).expand(record=validated)
        total = aggregate(processed)

        # .expand_kwargs() = map over a list of dicts (many args per copy)
        write_partition.expand_kwargs(build_partition_kwargs(validated))

    # -----------------------------------------------------------------------
    # [S13] @task_group decorator
    # -----------------------------------------------------------------------
    @task_group(group_id="quality_checks")
    def quality_checks(total_amount):
        @task
        def check_not_negative(value: int):
            if value < 0:
                raise AirflowFailException(f"Total is negative: {value}")
            return True

        @task
        def check_threshold(value: int, threshold: int = 100_000):
            if value > threshold:
                log.warning("Total %s is above threshold %s", value, threshold)
            return value <= threshold

        check_not_negative(total_amount)
        check_threshold(total_amount)

    qc = quality_checks(total)

    # -----------------------------------------------------------------------
    # [S14] Branching + Labels   |   [S15] trigger rule on the join
    # -----------------------------------------------------------------------
    @task.branch
    def choose_load_path(**context):
        env = context["params"]["env"]
        if env == "prod":
            return "load_prod"
        if env == "qa":
            return ["load_dev", "load_prod"]  # follow MULTIPLE branches
        return "load_dev"

    @task
    def load_dev(total_amount: int):
        log.info("Loading %s into DEV warehouse", total_amount)

    @task
    def load_prod(total_amount: int):
        log.info("Loading %s into PROD warehouse", total_amount)

    branch = choose_load_path()
    load_dev_task = load_dev(total)
    load_prod_task = load_prod(total)

    # Skipped branches would skip a normal (all_success) join → use this rule:
    join = EmptyOperator(task_id="join", trigger_rule="none_failed_min_one_success")

    qc >> branch
    branch >> Label("env = dev / qa") >> load_dev_task >> join
    branch >> Label("env = prod / qa") >> load_prod_task >> join

    # -----------------------------------------------------------------------
    # [S16] ShortCircuit  →  [S17] Dataset producer  →  [S18] Trigger child DAG
    # -----------------------------------------------------------------------
    @task.short_circuit(ignore_downstream_trigger_rules=False)
    def has_data(total_amount: int):
        # False → direct downstream tasks are SKIPPED.
        # ignore_downstream_trigger_rules=False → tasks further down still honour
        # their own trigger_rule (so `cleanup` / `end` still run).
        return total_amount > 0

    @task(outlets=[SALES_DATASET])
    def publish_summary(total_amount: int, **context):
        os.makedirs(DEMO_DIR, exist_ok=True)
        with open(SUMMARY_FILE, "w", encoding="utf-8") as f:
            f.write("run_id,total\n")
            f.write(f"{context['run_id']},{total_amount}\n")
        log.info("Wrote %s → Dataset/Asset updated → consumer DAG will be scheduled", SUMMARY_FILE)

    trigger_child = TriggerDagRunOperator(
        task_id="trigger_child_dag",
        trigger_dag_id="airflow_all_scenarios_child",
        conf={  # `conf` is a templated field
            "source_run_id": "{{ run_id }}",
            "total": "{{ ti.xcom_pull(task_ids='transform.aggregate') }}",
            "env": "{{ params.env }}",
        },
        wait_for_completion=False,  # True = block until child finishes (use deferrable=True then)
    )

    publish = publish_summary(total)
    join >> has_data(total) >> publish >> trigger_child

    # -----------------------------------------------------------------------
    # [S19] Setup / Teardown   |   [S20] Spark job
    # -----------------------------------------------------------------------
    with TaskGroup(group_id="spark") as spark_tg:

        @task
        def create_workspace(**context):
            # Real life: create EMR / Dataproc / Databricks cluster here
            safe_run = context["run_id"].replace(":", "_").replace("+", "_")
            path = os.path.join(DEMO_DIR, safe_run)
            os.makedirs(path, exist_ok=True)
            return path

        @task
        def delete_workspace(path: str):
            # Teardown ALWAYS runs (even if spark_job fails) – like a finally block
            shutil.rmtree(path, ignore_errors=True)
            log.info("Deleted workspace %s", path)

        spark_job = BashOperator(
            task_id="spark_job",
            bash_command=(
                "{% if params.run_spark %}"
                "spark-submit --master local[*] --name airflow_demo_{{ ds_nodash }} "
                "{{ params.spark_app }} --run-date {{ ds }}"
                "{% else %}"
                "echo 'run_spark=False → skipping spark-submit. "
                "workspace={{ ti.xcom_pull(task_ids=\"spark.create_workspace\") }}'"
                "{% endif %}"
            ),
        )

        # Provider-based alternative (pip install apache-airflow-providers-apache-spark):
        #
        # from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
        # spark_job = SparkSubmitOperator(
        #     task_id="spark_job",
        #     conn_id="spark_default",
        #     application="/opt/spark-apps/etl_job.py",
        #     application_args=["--run-date", "{{ ds }}"],
        #     conf={"spark.executor.memory": "2g"},
        #     executor_cores=2,
        #     num_executors=2,
        # )

        workspace = create_workspace()
        teardown = delete_workspace(workspace).as_teardown(setups=workspace)
        workspace >> spark_job >> teardown

    join >> spark_tg

    # -----------------------------------------------------------------------
    # [S21] Sensor  →  [S22] Retries  →  [S23] Fail / Skip exceptions
    # -----------------------------------------------------------------------
    @task.sensor(
        poke_interval=10,  # check every 10 s
        timeout=60,  # give up after 60 s
        mode="reschedule",  # frees the worker slot between pokes ("poke" holds it)
        soft_fail=True,  # on timeout → SKIPPED instead of FAILED
    )
    def wait_for_flag_file(**context):
        if context["params"]["sensor_auto_pass"]:
            return True
        log.info("Waiting for %s", FLAG_FILE)
        return os.path.exists(FLAG_FILE)
    # Other sensors: FileSensor, S3KeySensor, SqlSensor, HttpSensor, ExternalTaskSensor,
    # TimeDeltaSensor(deferrable=True) – deferrable ones need the `triggerer` running.

    @task(
        trigger_rule="none_failed",  # still runs if the sensor soft-failed (skipped)
        retries=3,
        retry_delay=timedelta(seconds=5),
        retry_exponential_backoff=True,
        on_retry_callback=on_task_retry,
    )
    def flaky_task(**context):
        ti = context["ti"]
        if context["params"]["simulate_flaky"] and ti.try_number < 2:
            raise RuntimeError(f"Simulated transient error on try {ti.try_number} – will retry")
        log.info("Succeeded on try %s", ti.try_number)

    @task(retries=5)  # retries are ignored for AirflowFailException
    def hard_fail_check(**context):
        if context["params"]["simulate_failure"]:
            raise AirflowFailException("simulate_failure=True → failing immediately, no retries")
        log.info("No failure requested")

    @task
    def weekday_only_task(**context):
        run_dt = context.get("logical_date") or pendulum.now(LOCAL_TZ)
        if run_dt.isoweekday() >= 6:  # 6 = Saturday, 7 = Sunday
            raise AirflowSkipException("Weekend → skipping this task")
        log.info("Weekday → doing weekday work")

    # [S15] one_failed → runs ONLY if any upstream failed (alerting / rollback)
    @task(trigger_rule="one_failed")
    def alert_on_failure():
        log.error("Something failed upstream! Send alert / start rollback here.")

    sensor = wait_for_flag_file()
    flaky = flaky_task()
    hard_fail = hard_fail_check()
    weekday = weekday_only_task()

    join >> sensor >> flaky >> hard_fail
    join >> weekday
    [flaky, hard_fail] >> alert_on_failure()

    # -----------------------------------------------------------------------
    # [S24] LatestOnlyOperator – downstream skipped for non-latest (backfill) runs
    # -----------------------------------------------------------------------
    latest_only = LatestOnlyOperator(task_id="latest_only")

    send_daily_report = BashOperator(
        task_id="send_daily_report",
        bash_command='echo "Sending report for {{ ds }} (only for the latest run)"',
    )

    # -----------------------------------------------------------------------
    # [S25] Custom operator   |   [S11] mapping a CLASSIC operator
    # -----------------------------------------------------------------------
    greet = GreetOperator(task_id="greet", name="{{ params.owner_name }}")

    mapped_bash = BashOperator.partial(task_id="mapped_bash").expand(
        bash_command=[
            'echo "cleaning table: orders"',
            'echo "cleaning table: customers"',
            'echo "cleaning table: products"',
        ]
    )

    # -----------------------------------------------------------------------
    # [S15] all_done cleanup + final end   |   [S27] chain()
    # -----------------------------------------------------------------------
    cleanup = BashOperator(
        task_id="cleanup",
        bash_command='echo "cleanup temp files – runs whatever happened upstream"',
        trigger_rule="all_done",
    )

    # none_failed_min_one_success → DAG goes RED if hard_fail_check failed
    end = EmptyOperator(task_id="end", trigger_rule="none_failed_min_one_success")

    # chain(a, b, c) == a >> b >> c
    chain(start, print_run_info, cfg, classic_task, extract_tg)
    start >> latest_only >> send_daily_report
    join >> [greet, mapped_bash]

    leaves = [trigger_child, spark_tg, hard_fail, weekday, send_daily_report, greet, mapped_bash]
    leaves >> cleanup
    leaves >> end
    cleanup >> end


# ===========================================================================
# CHILD DAG – triggered by TriggerDagRunOperator  [S18]
# ===========================================================================
@dag(
    dag_id="airflow_all_scenarios_child",
    schedule=None,  # only runs when triggered
    start_date=pendulum.datetime(2026, 1, 1, tz=LOCAL_TZ),
    catchup=False,
    tags=["learning", "all-scenarios"],
    default_args={"owner": "data_engineering", "retries": 0},
)
def all_scenarios_child():
    @task
    def read_trigger_conf():
        context = get_current_context()
        conf = context["dag_run"].conf or {}
        log.info("Triggered by run_id=%s total=%s env=%s",
                 conf.get("source_run_id"), conf.get("total"), conf.get("env"))
        return conf

    read_trigger_conf()

    # ExternalTaskSensor example (wait for a task in ANOTHER DAG with the same logical date):
    # from airflow.providers.standard.sensors.external_task import ExternalTaskSensor  # AF3
    # from airflow.sensors.external_task import ExternalTaskSensor                     # AF2
    # ExternalTaskSensor(task_id="wait_main", external_dag_id="airflow_all_scenarios",
    #                    external_task_id="end", mode="reschedule", timeout=3600)


all_scenarios_child()


# ===========================================================================
# CONSUMER DAG – data-aware scheduling on the Dataset/Asset  [S17]
# ===========================================================================
@dag(
    dag_id="airflow_all_scenarios_dataset_consumer",
    schedule=[SALES_DATASET],  # runs every time `publish_summary` succeeds
    start_date=pendulum.datetime(2026, 1, 1, tz=LOCAL_TZ),
    catchup=False,
    tags=["learning", "all-scenarios"],
    default_args={"owner": "data_engineering"},
)
def all_scenarios_dataset_consumer():
    @task
    def consume_summary():
        context = get_current_context()
        events = context.get("triggering_asset_events") or context.get("triggering_dataset_events")
        log.info("Triggered by dataset events: %s", events)
        if os.path.exists(SUMMARY_FILE):
            with open(SUMMARY_FILE, encoding="utf-8") as f:
                log.info("Summary file content:\n%s", f.read())

    consume_summary()


all_scenarios_dataset_consumer()


# ===========================================================================
# [S28] Local debug: `python airflow_all_scenarios_dag.py`
# (needs Airflow installed + `airflow db migrate` done once)
# ===========================================================================
if __name__ == "__main__":
    main_dag.test(run_conf={"env": "qa"})
