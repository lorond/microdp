from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

pyspark = pytest.importorskip("pyspark")

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("microdp-ingest-pg-cdc-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_timestamp_millis_preserves_milliseconds(spark):
    expected_millis = int(datetime(2026, 5, 22, 9, 30, 45, 123000, tzinfo=timezone.utc).timestamp() * 1000)
    df = spark.createDataFrame([(expected_millis,)], ["ts_ms"])
    event_ts = df.withColumn("event_ts", F.timestamp_millis(F.col("ts_ms"))).collect()[0].event_ts

    assert event_ts == datetime(2026, 5, 22, 9, 30, 45, 123000)


def test_valid_invalid_split_by_op_presence(spark):
    valid_value = '{"op":"c","ts_ms":1747900245123,"source":{"table":"transactions"},"after":{"id":"tx-1"}}'
    invalid_value = '{"source":{"table":"transactions"},"after":{"id":"tx-2"}}'  # missing op

    df = spark.createDataFrame(
        [
            ("wallet.public.transactions", 0, 1, "k", valid_value),
            ("wallet.public.transactions", 0, 2, "k", invalid_value),
            ("wallet.public.transactions", 0, 3, "k", None),
        ],
        ["topic", "partition_id", "offset_value", "key_json", "value_json"],
    )
    annotated = df.withColumn("op", F.get_json_object("value_json", "$.op"))
    is_valid = F.col("value_json").isNotNull() & F.col("op").isNotNull()

    valid_count = annotated.filter(is_valid).count()
    invalid_count = annotated.filter(~is_valid).count()

    assert valid_count == 1
    assert invalid_count == 2
