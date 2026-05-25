from __future__ import annotations

from pathlib import Path

import pytest

airflow = pytest.importorskip("airflow")

DAGS_DIR = Path(__file__).resolve().parents[1] / "dags"
DAG_ID = "microdp_data_platform_e2e"


@pytest.fixture(scope="module")
def dag():
    from airflow.models import DagBag

    bag = DagBag(dag_folder=str(DAGS_DIR), include_examples=False)
    assert not bag.import_errors, f"DAG import errors: {bag.import_errors}"
    assert DAG_ID in bag.dags, f"{DAG_ID} not found in DagBag"
    return bag.dags[DAG_ID]


def test_dag_task_ids(dag):
    expected = {
        "wait_for_connect",
        "register_debezium_connector",
        "init_lakehouse_tables",
        "ingest_postgres_cdc_to_bronze",
        "ingest_clickstream_to_bronze",
        "build_silver_and_gold_marts",
        "smoke_check_layers",
    }
    assert {task.task_id for task in dag.tasks} == expected


def test_dag_dependency_chain(dag):
    def downstream(task_id):
        return {t for t in dag.get_task(task_id).downstream_task_ids}

    assert "register_debezium_connector" in downstream("wait_for_connect")
    assert "init_lakehouse_tables" in downstream("register_debezium_connector")
    assert {"ingest_postgres_cdc_to_bronze", "ingest_clickstream_to_bronze"} <= downstream(
        "init_lakehouse_tables"
    )
    assert "build_silver_and_gold_marts" in downstream("ingest_postgres_cdc_to_bronze")
    assert "build_silver_and_gold_marts" in downstream("ingest_clickstream_to_bronze")
    assert "smoke_check_layers" in downstream("build_silver_and_gold_marts")


def test_dag_schedule_and_settings(dag):
    assert dag.schedule == "*/10 * * * *"
    assert dag.max_active_runs == 1
    assert dag.catchup is False
