"""
Airflow DAG: J2_SALES  (RIT-2: CLI trigger)  ← app hammesha chalu NA hoy tyare

Airflow worker par jar chalave che. Job puro thay etle JVM bandh thay.
    exit code 0 → SUCCESS / SKIPPED  → task green
    exit code 1 → FAILED             → task red, Airflow retry

Kyare aa rit?
  - Spark app mate alag server nathi rakhvo.
  - Kubernetes ma: BashOperator ne badle KubernetesPodOperator (same arguments, image ma jar).
Kharch: darek run ma JVM + SparkSession start (≈10–20 s vadhare).

Worker par jaruri: Java 17, jar (/opt/shop-etl/), ane env variables (MONGO_URI, POSTGRES_*).
Passwords Airflow Variables / Connections / secret backend ma rakho, DAG file ma nahi.

Dhyan: aa DAG athva shop_etl_j2_sales_api.py, EK j ON rakho.
"""
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

IST = pendulum.timezone("Asia/Kolkata")

JAR = "/opt/shop-etl/shop-etl-pipeline-1.0.0-exec.jar"

with DAG(
    dag_id="shop_etl_j2_sales_cli",
    description="Job-2 J2_SALES CLI mode (java -jar)",
    schedule="0 */4 * * *",
    start_date=pendulum.datetime(2026, 10, 21, tz=IST),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["shop-etl", "silver", "job-2"],
) as dag:

    BashOperator(
        task_id="run_j2_sales",
        bash_command=(
            f"java -jar {JAR} "
            "--spring.profiles.active=prod "
            "--spring.main.web-application-type=none "   # web server nahi
            "--etl.scheduler.enabled=false "             # app scheduler nahi
            "--etl.cli.job=J2_SALES "
            "--etl.cli.window-end={{ data_interval_end.in_timezone('Asia/Kolkata').isoformat() }}"
        ),
        env={
            "MONGO_URI": "{{ var.value.shop_mongo_uri }}",
            "POSTGRES_URL": "{{ var.value.shop_postgres_url }}",
            "POSTGRES_USER": "{{ var.value.shop_postgres_user }}",
            "POSTGRES_PASSWORD": "{{ var.value.shop_postgres_password }}",
        },
        append_env=True,
        execution_timeout=timedelta(hours=2),
    )
