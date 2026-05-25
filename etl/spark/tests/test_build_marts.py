from __future__ import annotations

import json
import sys
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

pyspark = pytest.importorskip("pyspark")

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from build_marts import (  # noqa: E402
    clickstream_events_select,
    conversion_funnel_cta_daily_select,
    conversion_funnel_daily_select,
    current_balances_select,
    timestamp_from_column,
    transactions_select,
    user_engagement_daily_select,
)
from pyspark.sql import SparkSession  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("microdp-build-marts-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_transactions_dt_uses_occurred_at(spark):
    occurred_at = datetime(2026, 5, 20, 10, 15, 30, tzinfo=UTC)
    occurred_at_ms = int(occurred_at.timestamp() * 1000)
    payload = {
        "after": {
            "id": "tx-1",
            "user_id": "user-1",
            "account_id": "account-1",
            "type": "payment",
            "direction": "debit",
            "amount": "25.00",
            "currency": "USD",
            "description": "Backdated payment",
            "merchant": "Coffee Lab",
            "occurred_at": str(occurred_at_ms),
            "created_at": "2026-05-22T10:15:30Z",
        }
    }
    spark.createDataFrame(
        [
            {
                "value_json": json.dumps(payload),
                "source_table": "transactions",
                "op": "c",
                "dt": date(2026, 5, 22),
                "event_ts": datetime(2026, 5, 22, 10, 15, 30),
                "partition_id": 0,
                "offset_value": 1,
            }
        ]
    ).createOrReplaceTempView("pg_cdc_raw")

    row = spark.sql(transactions_select("pg_cdc_raw")).collect()[0]

    assert row.transaction_id == "tx-1"
    assert str(row.amount) == "25.00"
    assert row.dt == date(2026, 5, 20)


def test_transactions_keeps_latest_update(spark):
    base_after = {
        "id": "tx-1",
        "user_id": "user-1",
        "account_id": "account-1",
        "type": "payment",
        "direction": "debit",
        "amount": "25.00",
        "currency": "USD",
        "merchant": "Coffee Lab",
        "occurred_at": "2026-05-22T10:15:30Z",
        "created_at": "2026-05-22T10:15:30Z",
    }
    spark.createDataFrame(
        [
            {
                "value_json": json.dumps({"after": {**base_after, "description": "initial"}}),
                "source_table": "transactions",
                "op": "c",
                "dt": date(2026, 5, 22),
                "event_ts": datetime(2026, 5, 22, 10, 15, 30),
                "partition_id": 0,
                "offset_value": 1,
            },
            {
                "value_json": json.dumps({"after": {**base_after, "description": "corrected"}}),
                "source_table": "transactions",
                "op": "u",
                "dt": date(2026, 5, 22),
                "event_ts": datetime(2026, 5, 22, 10, 20, 0),
                "partition_id": 0,
                "offset_value": 2,
            },
        ]
    ).createOrReplaceTempView("pg_cdc_raw")

    rows = spark.sql(transactions_select("pg_cdc_raw")).collect()

    assert len(rows) == 1
    assert rows[0].description == "corrected"


def test_clickstream_sessionization_splits_idle_gap(spark):
    spark.createDataFrame(
        [
            {
                "topic": "clickstream.events",
                "partition_id": 0,
                "offset_value": 1,
                "event_id": "event-1",
                "session_id": "session-1",
                "user_id": "user-1",
                "event_type": "page_enter",
                "page": "/dashboard",
                "element_id": None,
                "x": None,
                "y": None,
                "dwell_ms": 0,
                "event_ts": datetime(2026, 5, 22, 12, 0, 0),
                "ingest_ts": datetime(2026, 5, 22, 12, 0, 1),
                "dt": date(2026, 5, 22),
            },
            {
                "topic": "clickstream.events",
                "partition_id": 0,
                "offset_value": 2,
                "event_id": "event-2",
                "session_id": "session-1",
                "user_id": "user-1",
                "event_type": "click",
                "page": "/dashboard",
                "element_id": "quick-action-pay",
                "x": 100,
                "y": 140,
                "dwell_ms": 0,
                "event_ts": datetime(2026, 5, 22, 12, 5, 0),
                "ingest_ts": datetime(2026, 5, 22, 12, 5, 1),
                "dt": date(2026, 5, 22),
            },
            {
                "topic": "clickstream.events",
                "partition_id": 0,
                "offset_value": 3,
                "event_id": "event-3",
                "session_id": "session-1",
                "user_id": "user-1",
                "event_type": "page_enter",
                "page": "/payments",
                "element_id": None,
                "x": None,
                "y": None,
                "dwell_ms": 0,
                "event_ts": datetime(2026, 5, 22, 12, 40, 0),
                "ingest_ts": datetime(2026, 5, 22, 12, 40, 1),
                "dt": date(2026, 5, 22),
            },
        ]
    ).createOrReplaceTempView("clickstream_raw")

    rows = (
        spark.sql(clickstream_events_select("clickstream_raw"))
        .orderBy("event_ts")
        .select("analytics_session_id")
        .collect()
    )

    assert [row.analytics_session_id for row in rows] == [
        "session-1-001",
        "session-1-001",
        "session-1-002",
    ]


def test_conversion_funnel_counts_all_transaction_ctas(spark):
    spark.createDataFrame(
        [
            {
                "dt": date(2026, 5, 22),
                "user_id": "user-1",
                "analytics_session_id": "session-1-001",
                "event_type": "page_enter",
                "element_id": None,
            },
            {
                "dt": date(2026, 5, 22),
                "user_id": "user-1",
                "analytics_session_id": "session-1-001",
                "event_type": "click",
                "element_id": "create-transaction",
            },
            {
                "dt": date(2026, 5, 22),
                "user_id": "user-1",
                "analytics_session_id": "session-1-001",
                "event_type": "click",
                "element_id": "quick-action-pay",
            },
            {
                "dt": date(2026, 5, 22),
                "user_id": "user-2",
                "analytics_session_id": "session-2-001",
                "event_type": "click",
                "element_id": "quick-action-deposit",
            },
            {
                "dt": date(2026, 5, 22),
                "user_id": "user-2",
                "analytics_session_id": "session-2-001",
                "event_type": "click",
                "element_id": "refresh-data",
            },
        ]
    ).createOrReplaceTempView("silver_clickstream_events")
    spark.createDataFrame(
        [
            {
                "dt": date(2026, 5, 22),
                "user_id": "user-1",
                "transaction_id": "tx-1",
            }
        ]
    ).createOrReplaceTempView("silver_transactions")

    funnel = spark.sql(
        conversion_funnel_daily_select(
            "silver_clickstream_events",
            "silver_transactions",
        )
    ).collect()[0]
    cta_rows = spark.sql(
        conversion_funnel_cta_daily_select("silver_clickstream_events")
    ).collect()

    assert funnel.create_transaction_clicks == 3
    assert {row.element_id for row in cta_rows} == {
        "create-transaction",
        "quick-action-pay",
        "quick-action-deposit",
    }


def test_timestamp_from_column_accepts_iso_and_epoch_millis(spark):
    occurred_at = datetime(2026, 5, 22, 9, 30, 45, tzinfo=UTC)
    occurred_at_ms = int(occurred_at.timestamp() * 1000)
    spark.createDataFrame(
        [
            {"label": "iso", "raw": "2026-05-22T09:30:45Z"},
            {"label": "iso_millis", "raw": "2026-05-22T09:30:45.123Z"},
            {"label": "iso_offset", "raw": "2026-05-22T09:30:45+00:00"},
            {"label": "iso_offset_millis", "raw": "2026-05-22T09:30:45.123+00:00"},
            {"label": "millis", "raw": str(occurred_at_ms)},
        ]
    ).createOrReplaceTempView("raw_ts")

    rows = spark.sql(
        f"SELECT label, {timestamp_from_column('raw')} AS parsed FROM raw_ts"
    ).collect()

    parsed_by_label = {row.label: row.parsed for row in rows}
    expected_iso = datetime(2026, 5, 22, 9, 30, 45)
    expected_iso_millis = datetime(2026, 5, 22, 9, 30, 45, 123000)
    expected_millis = datetime.fromtimestamp(occurred_at_ms / 1000)
    assert parsed_by_label["iso"] == expected_iso
    assert parsed_by_label["iso_millis"] == expected_iso_millis
    assert parsed_by_label["iso_offset"] == expected_iso
    assert parsed_by_label["iso_offset_millis"] == expected_iso_millis
    assert parsed_by_label["millis"] == expected_millis


def test_current_balances_sums_credits_and_debits(spark):
    spark.createDataFrame(
        [
            {
                "account_id": "acc-1",
                "user_id": "user-1",
                "currency": "USD",
                "opening_balance": 100.00,
            }
        ]
    ).createOrReplaceTempView("silver_accounts")
    spark.createDataFrame(
        [
            {
                "transaction_id": "tx-1",
                "account_id": "acc-1",
                "direction": "credit",
                "amount": 40.00,
            },
            {
                "transaction_id": "tx-2",
                "account_id": "acc-1",
                "direction": "debit",
                "amount": 15.00,
            },
            {
                "transaction_id": "tx-3",
                "account_id": "acc-1",
                "direction": "debit",
                "amount": 5.00,
            },
        ]
    ).createOrReplaceTempView("silver_transactions")

    row = spark.sql(
        current_balances_select("silver_accounts", "silver_transactions")
    ).collect()[0]

    assert row.account_id == "acc-1"
    assert float(row.balance) == 120.00
    assert row.transactions_count == 3


def test_user_engagement_daily_combines_behavior_and_money(spark):
    spark.createDataFrame(
        [
            {
                "dt": date(2026, 5, 22),
                "user_id": "user-1",
                "analytics_session_id": "session-1-001",
                "event_type": "page_enter",
                "dwell_ms": 5_000,
            },
            {
                "dt": date(2026, 5, 22),
                "user_id": "user-1",
                "analytics_session_id": "session-1-001",
                "event_type": "click",
                "dwell_ms": 0,
            },
            {
                "dt": date(2026, 5, 22),
                "user_id": "user-2",
                "analytics_session_id": "session-2-001",
                "event_type": "page_enter",
                "dwell_ms": 12_000,
            },
        ]
    ).createOrReplaceTempView("silver_clickstream_events")
    spark.createDataFrame(
        [
            {
                "dt": date(2026, 5, 22),
                "user_id": "user-1",
                "direction": "debit",
                "amount": 25.00,
            },
            {
                "dt": date(2026, 5, 22),
                "user_id": "user-3",
                "direction": "credit",
                "amount": 50.00,
            },
        ]
    ).createOrReplaceTempView("silver_transactions")

    rows = spark.sql(
        user_engagement_daily_select(
            "silver_clickstream_events",
            "silver_transactions",
        )
    ).collect()
    by_user = {row.user_id: row for row in rows}

    assert by_user["user-1"].sessions == 1
    assert by_user["user-1"].page_views == 1
    assert by_user["user-1"].clicks == 1
    assert by_user["user-1"].transactions_count == 1
    assert float(by_user["user-1"].debit_amount) == 25.00

    assert by_user["user-2"].sessions == 1
    assert by_user["user-2"].transactions_count == 0
    assert float(by_user["user-2"].debit_amount) == 0.00

    assert by_user["user-3"].sessions == 0
    assert by_user["user-3"].page_views == 0
    assert by_user["user-3"].transactions_count == 1
    assert float(by_user["user-3"].credit_amount) == 50.00
