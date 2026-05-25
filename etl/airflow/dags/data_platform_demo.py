from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from string import Template
from typing import Mapping

import pendulum
import pymysql
import requests
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sdk import dag, task


logger = logging.getLogger(__name__)


def log_dag_failure(context: dict) -> None:
    dag_run = context.get("dag_run")
    failed = []
    if dag_run is not None:
        failed = [ti.task_id for ti in dag_run.get_task_instances(state="failed")]
    logger.error(
        "DAG %s failed (run_id=%s); failed tasks: %s",
        context.get("dag", "?"),
        getattr(dag_run, "run_id", "?"),
        failed or "<unknown>",
    )


CONNECT_URL = os.getenv("DEBEZIUM_CONNECT_URL", "http://connect:8083")
CONNECTOR_TEMPLATE_PATH = Path("/opt/airflow/connectors/wallet-postgres.json.template")
CONNECTOR_TEMPLATE_VARS = (
    "WALLET_DB_HOST",
    "WALLET_DB_PORT",
    "WALLET_DB_USER",
    "WALLET_DB_PASSWORD",
    "WALLET_DB_NAME",
)
SPARK_CONN_ID = os.getenv("SPARK_CONN_ID", "spark_default")
SPARK_DIR = "/opt/airflow/spark"
SPARK_COMMON_PY = f"{SPARK_DIR}/common.py"

STARROCKS_FE_HOST = os.getenv("STARROCKS_FE_HOST", "starrocks-fe")
STARROCKS_FE_QUERY_PORT = int(os.getenv("STARROCKS_FE_QUERY_PORT", "9030"))
STARROCKS_USER = os.getenv("STARROCKS_USER", "admin")
STARROCKS_PASSWORD = os.getenv("STARROCKS_PASSWORD", "admin")
STARROCKS_LAKE_CATALOG = os.getenv("STARROCKS_LAKE_CATALOG", "demo_lake")
STARROCKS_GOLD_DB = "gold"
# В StarRocks внешний Iceberg-каталог показывает namespace'ы (`gold`) как DB,
# а Spark-овский каталог `lakehouse` к ним не относится: REFRESH EXTERNAL TABLE
# принимает только 2 или 3 части (`db.table` либо `catalog.db.table`).
STARROCKS_GOLD_TABLES = (
    "current_balances",
    "transaction_volume_daily",
    "page_engagement_daily",
    "button_clicks_daily",
    "conversion_funnel_daily",
    "conversion_funnel_cta_daily",
    "user_engagement_daily",
    "sessions",
)

# Maven coordinates Iceberg/Kafka jars нужно передавать через spark-submit --packages,
# а не только через SparkSession.builder.config(spark.jars.packages, ...): на момент
# инициализации SparkSession уже грузятся `spark.sql.extensions` (IcebergSparkSessionExtensions),
# и без --packages класс не найден на classpath → ClassNotFoundException.
# Версии должны совпадать с etl/spark/common.py.
SPARK_PACKAGES = ",".join(
    [
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.1",
        "org.apache.iceberg:iceberg-aws-bundle:1.8.1",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5",
        # hadoop-aws нужен для checkpointLocation=s3a:// в Spark Structured Streaming
        # (Iceberg использует свой S3FileIO для данных, но checkpoint — это Hadoop FileSystem API).
        # Версия 3.3.4 совпадает с hadoop-client, поставляемым с pyspark 3.5.5.
        "org.apache.hadoop:hadoop-aws:3.3.4",
    ]
)


def render_connector_payload() -> dict:
    missing = [var for var in CONNECTOR_TEMPLATE_VARS if not os.getenv(var)]
    if missing:
        raise RuntimeError(
            f"Cannot render Debezium connector: missing env vars {missing}. "
            "Set them in .env / airflow service env."
        )
    substitutions = {var: os.environ[var] for var in CONNECTOR_TEMPLATE_VARS}
    rendered = Template(CONNECTOR_TEMPLATE_PATH.read_text()).safe_substitute(substitutions)
    return json.loads(rendered)


def spark_task(task_id: str, script: str) -> SparkSubmitOperator:
    return SparkSubmitOperator(
        task_id=task_id,
        application=f"{SPARK_DIR}/{script}",
        conn_id=SPARK_CONN_ID,
        py_files=SPARK_COMMON_PY,
        packages=SPARK_PACKAGES,
        verbose=False,
        retries=2,
        retry_delay=pendulum.duration(seconds=30),
    )


@dag(
    dag_id="microdp_data_platform_e2e",
    description="Ingest CDC and clickstream into Iceberg, then rebuild StarRocks-readable marts.",
    schedule="*/10 * * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=pendulum.duration(minutes=8),
    on_failure_callback=log_dag_failure,
    tags=["microdp", "iceberg", "starrocks"],
)
def microdp_data_platform_e2e():
    def desired_config_matches_live(
        desired: Mapping[str, str],
        live: Mapping[str, str],
    ) -> bool:
        return all(str(live.get(key)) == str(value) for key, value in desired.items())

    @task(retries=6, retry_delay=pendulum.duration(seconds=10))
    def wait_for_connect() -> str:
        # 1. Probe Connect REST API so the next task fails fast with a clear cause
        #    if Debezium isn't ready, instead of a cryptic 404 mid-registration.
        response = requests.get(f"{CONNECT_URL}/connector-plugins", timeout=5)
        response.raise_for_status()

        # 2. Если коннектор уже зарегистрирован прошлым DAG-раном — проверим, что он
        #    в RUNNING. Иначе ingest_pg_cdc будет тихо читать пустой/застойный топик
        #    (типичная причина: потерянный replication slot после `docker compose down -v`
        #    Postgres). 404 = ещё не создан, это ок — `register_debezium_connector`
        #    его создаст следующим шагом.
        name = json.loads(CONNECTOR_TEMPLATE_PATH.read_text())["name"]
        status_resp = requests.get(f"{CONNECT_URL}/connectors/{name}/status", timeout=5)
        if status_resp.status_code == 404:
            return CONNECT_URL
        status_resp.raise_for_status()
        status = status_resp.json()
        connector_state = status.get("connector", {}).get("state")
        if connector_state != "RUNNING":
            raise RuntimeError(
                f"Debezium connector {name!r} is registered but state={connector_state!r}. "
                f"Full status: {status}"
            )
        return CONNECT_URL

    @task(retries=3, retry_delay=pendulum.duration(seconds=15))
    def register_debezium_connector() -> str:
        payload = render_connector_payload()
        name = payload["name"]
        config = payload["config"]

        response = requests.get(f"{CONNECT_URL}/connectors/{name}/config", timeout=10)
        if response.status_code == 404:
            create = requests.post(f"{CONNECT_URL}/connectors", json=payload, timeout=20)
            create.raise_for_status()
            return "created"

        response.raise_for_status()
        if desired_config_matches_live(config, response.json()):
            return "unchanged"

        update = requests.put(f"{CONNECT_URL}/connectors/{name}/config", json=config, timeout=20)
        update.raise_for_status()
        return "updated"

    @task(retries=2, retry_delay=pendulum.duration(seconds=10))
    def refresh_starrocks_marts() -> dict:
        # Прогрев в три шага, чтобы первый SELECT в Superset SQL Lab уложился в его
        # 30-сек query_timeout даже после холодного `docker compose up`:
        #   1. REFRESH EXTERNAL TABLE — инвалидирует IcebergMetadataCache на FE,
        #      форсирует подтянуть свежие manifest'ы из Nessie + Garage.
        #   2. ANALYZE TABLE — собирает column-stats Iceberg-таблицы, чтобы
        #      планировщик StarRocks не делал это синхронно на первом запросе
        #      пользователя (это отдельный read-path к Parquet footer'ам).
        conn = pymysql.connect(
            host=STARROCKS_FE_HOST,
            port=STARROCKS_FE_QUERY_PORT,
            user=STARROCKS_USER,
            password=STARROCKS_PASSWORD,
            connect_timeout=10,
            read_timeout=300,
            write_timeout=10,
            autocommit=True,
        )
        refreshed: list[str] = []
        analyzed: list[str] = []
        warmed: list[str] = []
        failed: dict[str, str] = {}
        try:
            with conn.cursor() as cur:
                cur.execute(f"SET CATALOG `{STARROCKS_LAKE_CATALOG}`")
                for table in STARROCKS_GOLD_TABLES:
                    fqn = (
                        f"`{STARROCKS_LAKE_CATALOG}`.`{STARROCKS_GOLD_DB}`.`{table}`"
                    )
                    try:
                        cur.execute(f"REFRESH EXTERNAL TABLE {fqn}")
                        refreshed.append(table)
                    except Exception as exc:  # noqa: BLE001
                        failed[f"refresh:{table}"] = str(exc)
                        continue

                    try:
                        cur.execute(f"ANALYZE TABLE {fqn}")
                        cur.fetchall()
                        analyzed.append(table)
                    except Exception as exc:  # noqa: BLE001
                        # Stats — best effort: при ошибке прогрев данных всё равно
                        # сделаем, просто планировщик может оказаться чуть медленнее.
                        failed[f"analyze:{table}"] = str(exc)
        finally:
            conn.close()
        logger.info(
            "refreshed=%s analyzed=%s warmed=%s failed=%s",
            refreshed,
            analyzed,
            warmed,
            failed,
        )
        # Падаем только если не смогли REFRESH или прогрев данных — это и есть
        # то, ради чего таска существует. ANALYZE-ошибки уходят в лог, но не валят.
        critical = {k: v for k, v in failed.items() if not k.startswith("analyze:")}
        if critical:
            raise RuntimeError(f"StarRocks mart refresh had errors: {critical}")
        return {"refreshed": refreshed, "analyzed": analyzed, "warmed": warmed}

    init_lakehouse = spark_task("init_lakehouse_tables", "init_tables.py")
    ingest_pg_cdc = spark_task("ingest_postgres_cdc_to_bronze", "ingest_pg_cdc.py")
    ingest_clickstream = spark_task("ingest_clickstream_to_bronze", "ingest_clickstream.py")
    build_marts = spark_task("build_silver_and_gold_marts", "build_marts.py")
    smoke_checks = spark_task("smoke_check_layers", "smoke_checks.py")
    refresh_marts = refresh_starrocks_marts()

    connect_ready = wait_for_connect()
    connector = register_debezium_connector()
    connect_ready >> connector >> init_lakehouse >> [ingest_pg_cdc, ingest_clickstream]
    [ingest_pg_cdc, ingest_clickstream] >> build_marts
    build_marts >> [smoke_checks, refresh_marts]


microdp_data_platform_e2e()
