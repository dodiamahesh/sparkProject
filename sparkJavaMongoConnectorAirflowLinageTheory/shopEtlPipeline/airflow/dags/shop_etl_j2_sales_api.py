"""
Airflow DAG: J2_SALES  (RIT-1: API trigger)  ← app hammesha chalu hoy tyare RECOMMENDED

Flow:
    trigger_j2_sales  (POST /api/jobs/J2_SALES/runs, async)  → runId XCom ma
          │
    wait_j2_sales     (GET /api/jobs/runs/{runId} poll, mode=reschedule)
                        SUCCESS → task green,  FAILED → task red (Airflow retry),  RUNNING → pharithi poll

Kem aa rit?
  - Spark app (JVM + SparkSession) hammesha chalu che: darek run ma 10–20 s startup bachat.
  - Sensor "reschedule" mode ma: poll vacche Airflow worker slot khali rahe che.
  - windowEnd = data_interval_end: run id ane window DETERMINISTIC. Backfill / clear kari ne
    farithi chalavo to same window, same run id.
  - Same job chalu hoy to API 409 aape → task fail → Airflow retry kare.

Setup (Airflow UI → Admin → Connections):
    Conn Id : shop_etl_api
    Type    : HTTP
    Host    : http://shop-etl-host:8080

Dhyan: aa DAG athva shop_etl_j2_sales_cli.py, EK j ON rakho. App no scheduler pan prod ma bandh che.
Job-1 (dim_product) ready thay pachhi: ExternalTaskSensor / ek j DAG ma j1 >> j2 umero.
"""
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.http.sensors.http import HttpSensor

IST = pendulum.timezone("Asia/Kolkata")
TRIGGER_TASK = "trigger_j2_sales"


def extract_run_id(response):
    """POST nu response: STARTED → runId, SKIPPED → 'SKIPPED' (window pehla j load thai gai)."""
    body = response.json()
    return body["runId"] if body.get("runId") else "SKIPPED"


def run_finished(response, **context):
    """
    Sensor check:
      SKIPPED trigger      → True (kai karvanu nathi)
      404 (haju row nathi) → False (pharithi poll)
      SUCCESS              → True
      FAILED               → exception (task fail, error_message logs ma)
      RUNNING              → False
    """
    if context["ti"].xcom_pull(task_ids=TRIGGER_TASK) == "SKIPPED":
        return True
    if response.status_code == 404:
        return False
    body = response.json()
    if body["status"] == "SUCCESS":
        return True
    if body["status"] == "FAILED":
        raise AirflowException(f"J2_SALES FAILED: {body['job'].get('error_message')}")
    return False


with DAG(
    dag_id="shop_etl_j2_sales_api",
    description="Job-2 J2_SALES: customers, orders, payments → dim_customer, fact_sales, fact_payment",
    schedule="0 */4 * * *",                       # darek 4 kalake (IST): 00, 04, 08, ...
    start_date=pendulum.datetime(2026, 10, 21, tz=IST),
    catchup=False,
    max_active_runs=1,                            # ek samaye ek j run
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["shop-etl", "silver", "job-2"],
) as dag:

    trigger = HttpOperator(
        task_id=TRIGGER_TASK,
        http_conn_id="shop_etl_api",
        endpoint="/api/jobs/J2_SALES/runs",
        method="POST",
        # Form param tarike jay (Spring @RequestParam vanchi le). Ex: 2026-10-21T04:00:00+05:30
        data={"windowEnd": "{{ data_interval_end.in_timezone('Asia/Kolkata').isoformat() }}"},
        headers={"X-Triggered-By": "AIRFLOW"},
        response_check=lambda response: response.status_code in (200, 202),
        response_filter=extract_run_id,
        log_response=True,
    )

    wait = HttpSensor(
        task_id="wait_j2_sales",
        http_conn_id="shop_etl_api",
        endpoint="/api/jobs/runs/{{ ti.xcom_pull(task_ids='" + TRIGGER_TASK + "') }}",
        method="GET",
        extra_options={"check_response": False},  # 404 ne pan response_check ma aapo
        response_check=run_finished,
        poke_interval=30,
        timeout=60 * 60 * 2,                      # 2 kalak ma puru na thay to fail
        mode="reschedule",
    )

    trigger >> wait
