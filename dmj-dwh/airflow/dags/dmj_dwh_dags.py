"""
dmj-dwh: ONE Airflow DAG per pipeline, generated from dmj_dwh_pipelines.yaml (same folder).

Every DAG has one task that runs the shared jar in batch mode:

    java -jar dmj-dwh-1.0.0.jar --spring.profiles.active=prod --run-pipeline=<pipeline>

Only that pipeline runs; the process exits with 0 (success) or 1 (failed).

Dependencies: a pipeline with `depends_on` is scheduled on the assets (datasets) of its upstream pipelines.
The upstream task publishes its asset ONLY when it succeeds, so the downstream DAG starts after ALL its upstream
pipelines finished successfully. A failed upstream run publishes nothing: the downstream DAG waits.

Works with Airflow 2.4+ (Dataset) and Airflow 3 (Asset).
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta

import yaml

try:  # Airflow 3
    from airflow.providers.standard.operators.bash import BashOperator
    from airflow.sdk import DAG, Asset
except ImportError:  # Airflow 2.4+
    from airflow import DAG
    from airflow.datasets import Dataset as Asset
    from airflow.operators.bash import BashOperator

CONFIG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dmj_dwh_pipelines.yaml")


def dag_id_of(pipeline: str) -> str:
    """DAG id = pipeline name, exactly (the application stores the same value in meta.meta_pipeline.airflow_dag_id)."""
    return pipeline


def asset_of(pipeline: str) -> Asset:
    """Published when the pipeline's run succeeded; dependent DAGs are scheduled on it."""
    return Asset(f"dmjdwh://pipeline/{pipeline}")


def build_dags(config: dict) -> dict[str, DAG]:
    defaults = config.get("defaults", {})
    pipelines = config.get("pipelines", {})
    dags = {}

    for pipeline, settings in pipelines.items():
        settings = settings or {}
        upstream = settings.get("depends_on") or []
        unknown = [name for name in upstream if name not in pipelines]
        if unknown:
            raise ValueError(f"{pipeline}: depends_on unknown pipeline(s) {unknown}")
        if upstream and settings.get("schedule"):
            raise ValueError(f"{pipeline}: use either schedule or depends_on, not both")

        schedule = [asset_of(name) for name in upstream] if upstream else settings.get("schedule")
        env = {**defaults.get("env", {}), **settings.get("env", {})}
        command = " ".join([
            settings.get("java", defaults.get("java", "java")),
            settings.get("java_options", defaults.get("java_options", "")),
            "-jar", settings.get("jar", defaults["jar"]),
            f"--spring.profiles.active={settings.get('spring_profile', defaults.get('spring_profile', 'prod'))}",
            f"--run-pipeline={pipeline}",
        ])

        with DAG(
            dag_id=dag_id_of(pipeline),
            description=f"dmj-dwh pipeline {pipeline}",
            schedule=schedule,
            start_date=datetime.fromisoformat(str(settings.get("start_date", defaults.get("start_date", "2026-11-01")))),
            catchup=False,
            max_active_runs=1,            # a pipeline never runs twice at the same time (the jar also locks it)
            tags=list(defaults.get("tags", [])) + [pipeline],
            default_args={
                "owner": settings.get("owner", defaults.get("owner", "airflow")),
                "retries": settings.get("retries", defaults.get("retries", 1)),
                "retry_delay": timedelta(minutes=settings.get("retry_delay_minutes",
                                                              defaults.get("retry_delay_minutes", 10))),
            },
            doc_md=f"Runs **{pipeline}** of dmj-dwh (`--run-pipeline={pipeline}`)."
                   + (f" Starts after: {', '.join(upstream)}." if upstream else ""),
        ) as dag:
            BashOperator(
                task_id="run_pipeline",
                bash_command=command,
                env=env,
                append_env=True,          # keep PATH, JAVA_HOME ... of the worker
                outlets=[asset_of(pipeline)],
                execution_timeout=timedelta(minutes=settings.get("execution_timeout_minutes",
                                                                 defaults.get("execution_timeout_minutes", 180))),
                # an Airflow pool limits how many dmj-dwh JVMs run at the same time on the workers
                pool=settings.get("pool", defaults.get("pool", "default_pool")),
            )
        dags[dag.dag_id] = dag
    return dags


with open(CONFIG_FILE, encoding="utf-8") as config_file:
    for _dag_id, _dag in build_dags(yaml.safe_load(config_file)).items():
        globals()[_dag_id] = _dag
