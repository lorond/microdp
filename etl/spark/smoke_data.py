"""Optional data-quality smoke for a populated demo.

Run manually after generating data with the emulator or UI:

    docker compose exec airflow-scheduler python /opt/airflow/spark/smoke_data.py

The DAG runs structural `smoke_checks.py` only, since an empty demo stand is a
valid state. Use this script to verify that data has actually arrived end-to-end.
"""

from __future__ import annotations

import sys

from common import get_spark


REQUIRED_NON_EMPTY = (
    "lakehouse.bronze.pg_cdc_raw",
    "lakehouse.bronze.clickstream_raw",
    "lakehouse.silver.transactions",
    "lakehouse.silver.accounts",
    "lakehouse.silver.users",
    "lakehouse.silver.clickstream_events",
    "lakehouse.gold.current_balances",
)


def main() -> None:
    spark = get_spark("microdp-smoke-data")
    failures: list[str] = []
    try:
        for table in REQUIRED_NON_EMPTY:
            count = spark.sql(f"SELECT count(*) AS c FROM {table}").collect()[0]["c"]
            print(f"{table}: {count} rows")
            if count == 0:
                failures.append(table)
    finally:
        spark.stop()

    if failures:
        print(f"ERROR: empty tables after pipeline run: {failures}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
