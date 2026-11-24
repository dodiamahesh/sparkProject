"""
dmj-dwh: ONE Airflow DAG per pipeline, generated from dmj_dwh_pipelines.yaml (same folder).

Every DAG has one task that runs the shared jar in batch mode, with the Spring profile of THIS Airflow:

    java -jar dmj-dwh-1.0.0.jar --spring.profiles.active=<profile> --run-pipeline=<pipeline>

Only that pipeline runs; the process exits with 0 (success) or 1 (failed).

Environment: the same files are deployed to every Airflow (offDev / dev / test / prod). Which one this Airflow is
comes from the environment variable DMJ_DWH_ENV or the Airflow Variable dmj_dwh_env. Its block under
"environments" in the YAML gives the Spring profile, java options, schedules on/off and environment variables.

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
ENV_VARIABLE = "DMJ_DWH_ENV"          # environment variable on the scheduler / workers
AIRFLOW_VARIABLE = "dmj_dwh_env"      # or Airflow Variable (Admin > Variables)

# Spark CLIENT mode: the executors connect back to the driver (= this task's JVM). Use the worker's own IP unless
# SPARK_DRIVER_HOST is already set. Ignored by the offDev profile (local[*]).
DRIVER_HOST = 'export SPARK_DRIVER_HOST="${SPARK_DRIVER_HOST:-$(hostname -I 2>/dev/null | awk \'{print $1}\')}"; '


def current_environment(environments: dict) -> str:
    """offDev / dev / test / prod of THIS Airflow. Missing or unknown = the DAG file fails to load (clear error)."""
    env = os.environ.get(ENV_VARIABLE)
    if not env:
        try:
            try:  # Airflow 3
                from airflow.sdk import Variable
                env = Variable.get(AIRFLOW_VARIABLE, default=None)
            except ImportError:  # Airflow 2
                from airflow.models import Variable
                env = Variable.get(AIRFLOW_VARIABLE, default_var=None)
        except Exception:  # metadata DB not reachable while parsing
            env = None
    if env not in environments:
        raise ValueError(f"dmj-dwh: set the environment of this Airflow with the environment variable {ENV_VARIABLE} "
                         f"or the Airflow Variable {AIRFLOW_VARIABLE} to one of {sorted(environments)} (now: {env!r})")
    return env


def merge(*layers: dict) -> dict:
    """Later layers win; the "env" maps are merged key by key."""
    merged, env_vars = {}, {}
    for layer in layers:
        for key, value in (layer or {}).items():
            if key == "env":
                env_vars.update(value or {})
            elif key != "environments":
                merged[key] = value
    merged["env"] = env_vars
    return merged


def dag_id_of(pipeline: str) -> str:
    """DAG id = pipeline name, exactly (the application stores the same value in meta.meta_pipeline.airflow_dag_id)."""
    return pipeline


def asset_of(pipeline: str) -> Asset:
    """Published when the pipeline's run succeeded; dependent DAGs are scheduled on it."""
    return Asset(f"dmjdwh://pipeline/{pipeline}")


def build_dags(config: dict) -> dict[str, DAG]:
    environments = config.get("environments", {})
    env_name = current_environment(environments)
    pipelines = config.get("pipelines", {})
    dags = {}

    for pipeline, pipeline_settings in pipelines.items():
        pipeline_settings = pipeline_settings or {}
        settings = merge(config.get("defaults", {}), environments[env_name], pipeline_settings,
                         (pipeline_settings.get("environments") or {}).get(env_name))

        upstream = settings.get("depends_on") or []
        unknown = [name for name in upstream if name not in pipelines]
        if unknown:
            raise ValueError(f"{pipeline}: depends_on unknown pipeline(s) {unknown}")
        if upstream and settings.get("schedule"):
            raise ValueError(f"{pipeline}: use either schedule or depends_on, not both")

        if upstream:
            schedule = [asset_of(name) for name in upstream]        # runs after its upstream pipelines
        elif settings.get("schedules", True):
            schedule = settings.get("schedule")
        else:
            schedule = None                                         # this environment: trigger by hand

        profile = settings.get("spring_profile", env_name)
        command = DRIVER_HOST + " ".join([
            "exec", settings.get("java", "java"),
            settings.get("java_options", ""),
            "-jar", settings["jar"],
            f"--spring.profiles.active={profile}",
            f"--run-pipeline={pipeline}",
        ])

        with DAG(
            dag_id=dag_id_of(pipeline),
            description=f"dmj-dwh pipeline {pipeline} ({env_name})",
            schedule=schedule,
            start_date=datetime.fromisoformat(str(settings.get("start_date", "2026-11-01"))),
            catchup=False,
            max_active_runs=1,            # a pipeline never runs twice at the same time (the jar also locks it)
            tags=list(settings.get("tags", [])) + [pipeline, env_name],
            default_args={
                "owner": settings.get("owner", "airflow"),
                "retries": settings.get("retries", 1),
                "retry_delay": timedelta(minutes=settings.get("retry_delay_minutes", 10)),
            },
            doc_md=f"Runs **{pipeline}** of dmj-dwh in **{env_name}** "
                   f"(`--spring.profiles.active={profile} --run-pipeline={pipeline}`)."
                   + (f" Starts after: {', '.join(upstream)}." if upstream else ""),
        ) as dag:
            BashOperator(
                task_id="run_pipeline",
                bash_command=command,
                env={key: str(value) for key, value in settings["env"].items()},
                append_env=True,          # keep PATH, JAVA_HOME ... of the worker
                outlets=[asset_of(pipeline)],
                execution_timeout=timedelta(minutes=settings.get("execution_timeout_minutes", 180)),
                # an Airflow pool limits how many dmj-dwh JVMs run at the same time on the workers
                pool=settings.get("pool", "default_pool"),
            )
        dags[dag.dag_id] = dag
    return dags


with open(CONFIG_FILE, encoding="utf-8") as config_file:
    for _dag_id, _dag in build_dags(yaml.safe_load(config_file)).items():
        globals()[_dag_id] = _dag
