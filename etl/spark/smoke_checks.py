from __future__ import annotations

import sys

from common import get_spark


REQUIRED_TABLES = (
    "lakehouse.bronze.pg_cdc_raw",
    "lakehouse.bronze.clickstream_raw",
    "lakehouse.bronze.dlq_pg_cdc",
    "lakehouse.bronze.dlq_clickstream",
    "lakehouse.silver.transactions",
    "lakehouse.silver.accounts",
    "lakehouse.silver.users",
    "lakehouse.silver.clickstream_events",
    "lakehouse.gold.current_balances",
)


def main() -> None:
    spark = get_spark("microdp-smoke-checks")
    failures: list[str] = []
    try:
        for table in REQUIRED_TABLES:
            try:
                count = spark.sql(f"SELECT count(*) AS c FROM {table}").collect()[0]["c"]
            except Exception as exc:
                print(f"{table}: SCHEMA MISSING ({exc})", file=sys.stderr)
                failures.append(table)
                continue
            print(f"{table}: {count} rows (structural check OK)")
    finally:
        spark.stop()

    if failures:
        print(f"ERROR: missing/unreadable tables after pipeline run: {failures}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
