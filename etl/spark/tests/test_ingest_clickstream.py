from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

import pytest

pyspark = pytest.importorskip("pyspark")

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ingest_clickstream import parse_iso_timestamp  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("microdp-ingest-clickstream-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_parse_iso_timestamp_handles_zulu_millis_and_offset(spark):
    df = spark.createDataFrame(
        [
            ("zulu", "2026-05-22T09:30:45Z"),
            ("zulu_millis", "2026-05-22T09:30:45.123Z"),
            ("offset", "2026-05-22T09:30:45+00:00"),
            ("offset_millis", "2026-05-22T09:30:45.123+00:00"),
            ("naive", "2026-05-22T09:30:45"),
            ("garbage", "not-a-timestamp"),
        ],
        ["label", "raw"],
    )

    parsed = df.withColumn("ts", parse_iso_timestamp(F.col("raw"))).collect()
    by_label = {row.label: row.ts for row in parsed}

    base = datetime(2026, 5, 22, 9, 30, 45)
    base_millis = datetime(2026, 5, 22, 9, 30, 45, 123000)
    assert by_label["zulu"] == base
    assert by_label["zulu_millis"] == base_millis
    assert by_label["offset"] == base
    assert by_label["offset_millis"] == base_millis
    assert by_label["naive"] == base
    assert by_label["garbage"] is None
