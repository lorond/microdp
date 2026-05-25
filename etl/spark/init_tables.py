from common import get_spark


def main() -> None:
    spark = get_spark("microdp-init-lakehouse")

    for namespace in ("bronze", "silver", "gold"):
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS lakehouse.{namespace}")

    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS lakehouse.bronze.pg_cdc_raw (
            topic STRING,
            partition_id INT,
            offset_value BIGINT,
            key_json STRING,
            value_json STRING,
            op STRING,
            source_table STRING,
            event_ts TIMESTAMP,
            ingest_ts TIMESTAMP,
            dt DATE
        )
        USING iceberg
        PARTITIONED BY (dt)
        """
    )

    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS lakehouse.bronze.dlq_pg_cdc (
            topic STRING,
            partition_id INT,
            offset_value BIGINT,
            key_json STRING,
            raw_value STRING,
            error STRING,
            ingest_ts TIMESTAMP,
            dt DATE
        )
        USING iceberg
        PARTITIONED BY (dt)
        """
    )

    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS lakehouse.bronze.dlq_clickstream (
            topic STRING,
            partition_id INT,
            offset_value BIGINT,
            raw_value STRING,
            error STRING,
            ingest_ts TIMESTAMP,
            dt DATE
        )
        USING iceberg
        PARTITIONED BY (dt)
        """
    )

    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS lakehouse.bronze.clickstream_raw (
            topic STRING,
            partition_id INT,
            offset_value BIGINT,
            event_id STRING,
            session_id STRING,
            user_id STRING,
            event_type STRING,
            page STRING,
            element_id STRING,
            x INT,
            y INT,
            dwell_ms BIGINT,
            value_json STRING,
            event_ts TIMESTAMP,
            ingest_ts TIMESTAMP,
            dt DATE
        )
        USING iceberg
        PARTITIONED BY (dt)
        """
    )

    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS lakehouse.silver.transactions (
            transaction_id STRING,
            user_id STRING,
            account_id STRING,
            type STRING,
            direction STRING,
            amount DECIMAL(18, 2),
            currency STRING,
            description STRING,
            merchant STRING,
            balance_after DECIMAL(18, 2),
            occurred_at TIMESTAMP,
            created_at TIMESTAMP,
            dt DATE
        )
        USING iceberg
        PARTITIONED BY (dt)
        """
    )

    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS lakehouse.silver.users (
            user_id STRING,
            full_name STRING,
            email STRING,
            created_at TIMESTAMP,
            event_ts TIMESTAMP
        )
        USING iceberg
        """
    )

    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS lakehouse.silver.accounts (
            account_id STRING,
            user_id STRING,
            currency STRING,
            opening_balance DECIMAL(18, 2),
            current_balance DECIMAL(18, 2),
            created_at TIMESTAMP,
            updated_at TIMESTAMP,
            event_ts TIMESTAMP
        )
        USING iceberg
        """
    )

    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS lakehouse.silver.clickstream_events (
            event_id STRING,
            session_id STRING,
            analytics_session_id STRING,
            user_id STRING,
            event_type STRING,
            page STRING,
            element_id STRING,
            x INT,
            y INT,
            dwell_ms BIGINT,
            event_ts TIMESTAMP,
            dt DATE
        )
        USING iceberg
        PARTITIONED BY (dt)
        """
    )

    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS lakehouse.silver.sessions (
            session_id STRING,
            raw_session_id STRING,
            user_id STRING,
            started_at TIMESTAMP,
            ended_at TIMESTAMP,
            events_count BIGINT,
            total_dwell_ms BIGINT,
            dt DATE
        )
        USING iceberg
        PARTITIONED BY (dt)
        """
    )

    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS lakehouse.gold.current_balances (
            user_id STRING,
            account_id STRING,
            currency STRING,
            balance DECIMAL(18, 2),
            transactions_count BIGINT,
            updated_at TIMESTAMP
        )
        USING iceberg
        """
    )

    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS lakehouse.gold.transaction_volume_daily (
            dt DATE,
            user_id STRING,
            currency STRING,
            credit_amount DECIMAL(18, 2),
            debit_amount DECIMAL(18, 2),
            transaction_count BIGINT
        )
        USING iceberg
        PARTITIONED BY (dt)
        """
    )

    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS lakehouse.gold.page_engagement_daily (
            dt DATE,
            page STRING,
            sessions BIGINT,
            events BIGINT,
            total_dwell_ms BIGINT,
            avg_dwell_ms DOUBLE
        )
        USING iceberg
        PARTITIONED BY (dt)
        """
    )

    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS lakehouse.gold.button_clicks_daily (
            dt DATE,
            page STRING,
            element_id STRING,
            clicks BIGINT,
            users BIGINT,
            sessions BIGINT
        )
        USING iceberg
        PARTITIONED BY (dt)
        """
    )

    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS lakehouse.gold.conversion_funnel_daily (
            dt DATE,
            page_views BIGINT,
            create_transaction_clicks BIGINT,
            transactions_created BIGINT,
            conversion_rate DOUBLE
        )
        USING iceberg
        PARTITIONED BY (dt)
        """
    )

    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS lakehouse.gold.conversion_funnel_cta_daily (
            dt DATE,
            element_id STRING,
            clicks BIGINT,
            users BIGINT,
            sessions BIGINT
        )
        USING iceberg
        PARTITIONED BY (dt)
        """
    )

    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS lakehouse.gold.user_engagement_daily (
            dt DATE,
            user_id STRING,
            sessions BIGINT,
            total_dwell_ms BIGINT,
            page_views BIGINT,
            clicks BIGINT,
            transactions_count BIGINT,
            debit_amount DECIMAL(18, 2),
            credit_amount DECIMAL(18, 2)
        )
        USING iceberg
        PARTITIONED BY (dt)
        """
    )

    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS lakehouse.gold.sessions (
            session_id STRING,
            raw_session_id STRING,
            user_id STRING,
            started_at TIMESTAMP,
            ended_at TIMESTAMP,
            events_count BIGINT,
            total_dwell_ms BIGINT,
            dt DATE
        )
        USING iceberg
        PARTITIONED BY (dt)
        """
    )

    spark.stop()


if __name__ == "__main__":
    main()
