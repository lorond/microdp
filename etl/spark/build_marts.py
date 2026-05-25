from __future__ import annotations

from common import get_spark


BRONZE_CDC = "lakehouse.bronze.pg_cdc_raw"
BRONZE_CLICKSTREAM = "lakehouse.bronze.clickstream_raw"
SILVER_TRANSACTIONS = "lakehouse.silver.transactions"
SILVER_USERS = "lakehouse.silver.users"
SILVER_ACCOUNTS = "lakehouse.silver.accounts"
SILVER_CLICKSTREAM = "lakehouse.silver.clickstream_events"
SILVER_SESSIONS = "lakehouse.silver.sessions"
GOLD_CURRENT_BALANCES = "lakehouse.gold.current_balances"
GOLD_TRANSACTION_VOLUME_DAILY = "lakehouse.gold.transaction_volume_daily"
GOLD_PAGE_ENGAGEMENT_DAILY = "lakehouse.gold.page_engagement_daily"
GOLD_BUTTON_CLICKS_DAILY = "lakehouse.gold.button_clicks_daily"
GOLD_CONVERSION_FUNNEL_DAILY = "lakehouse.gold.conversion_funnel_daily"
GOLD_CONVERSION_FUNNEL_CTA_DAILY = "lakehouse.gold.conversion_funnel_cta_daily"
GOLD_USER_ENGAGEMENT_DAILY = "lakehouse.gold.user_engagement_daily"
GOLD_SESSIONS = "lakehouse.gold.sessions"

SILVER_TRANSACTIONS_COLUMNS = (
    "transaction_id",
    "user_id",
    "account_id",
    "type",
    "direction",
    "amount",
    "currency",
    "description",
    "merchant",
    "balance_after",
    "occurred_at",
    "created_at",
    "dt",
)
SILVER_USERS_COLUMNS = ("user_id", "full_name", "email", "created_at", "event_ts")
SILVER_ACCOUNTS_COLUMNS = (
    "account_id",
    "user_id",
    "currency",
    "opening_balance",
    "current_balance",
    "created_at",
    "updated_at",
    "event_ts",
)
SILVER_CLICKSTREAM_COLUMNS = (
    "event_id",
    "session_id",
    "analytics_session_id",
    "user_id",
    "event_type",
    "page",
    "element_id",
    "x",
    "y",
    "dwell_ms",
    "event_ts",
    "dt",
)
SESSIONS_COLUMNS = (
    "session_id",
    "raw_session_id",
    "user_id",
    "started_at",
    "ended_at",
    "events_count",
    "total_dwell_ms",
    "dt",
)
GOLD_CURRENT_BALANCES_COLUMNS = (
    "user_id",
    "account_id",
    "currency",
    "balance",
    "transactions_count",
    "updated_at",
)
GOLD_TRANSACTION_VOLUME_DAILY_COLUMNS = (
    "dt",
    "user_id",
    "currency",
    "credit_amount",
    "debit_amount",
    "transaction_count",
)
GOLD_PAGE_ENGAGEMENT_DAILY_COLUMNS = (
    "dt",
    "page",
    "sessions",
    "events",
    "total_dwell_ms",
    "avg_dwell_ms",
)
GOLD_BUTTON_CLICKS_DAILY_COLUMNS = (
    "dt",
    "page",
    "element_id",
    "clicks",
    "users",
    "sessions",
)
GOLD_CONVERSION_FUNNEL_DAILY_COLUMNS = (
    "dt",
    "page_views",
    "create_transaction_clicks",
    "transactions_created",
    "conversion_rate",
)
GOLD_CONVERSION_FUNNEL_CTA_DAILY_COLUMNS = (
    "dt",
    "element_id",
    "clicks",
    "users",
    "sessions",
)
GOLD_USER_ENGAGEMENT_DAILY_COLUMNS = (
    "dt",
    "user_id",
    "sessions",
    "total_dwell_ms",
    "page_views",
    "clicks",
    "transactions_count",
    "debit_amount",
    "credit_amount",
)

SESSION_IDLE_GAP_SECONDS = 30 * 60
FUNNEL_CTA_IDS = (
    "'create-transaction'",
    "'quick-action-pay'",
    "'quick-action-deposit'",
)


def timestamp_from_column(column_name: str) -> str:
    return f"""
        CASE
            WHEN {column_name} RLIKE '^-?[0-9]+$'
                THEN timestamp_millis(CAST({column_name} AS BIGINT))
            ELSE coalesce(
                to_timestamp({column_name}, "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),
                to_timestamp({column_name}, "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX"),
                to_timestamp({column_name}, "yyyy-MM-dd'T'HH:mm:ssXXX"),
                to_timestamp({column_name}, "yyyy-MM-dd'T'HH:mm:ss.SSS"),
                to_timestamp({column_name}, "yyyy-MM-dd'T'HH:mm:ss"),
                CAST({column_name} AS TIMESTAMP)
            )
        END
    """


def transactions_select(pg_cdc_table: str = BRONZE_CDC) -> str:
    return f"""
        WITH parsed AS (
            SELECT
                get_json_object(value_json, '$.after.id') AS transaction_id,
                get_json_object(value_json, '$.after.user_id') AS user_id,
                get_json_object(value_json, '$.after.account_id') AS account_id,
                get_json_object(value_json, '$.after.type') AS type,
                get_json_object(value_json, '$.after.direction') AS direction,
                CAST(get_json_object(value_json, '$.after.amount') AS DECIMAL(18, 2)) AS amount,
                get_json_object(value_json, '$.after.currency') AS currency,
                get_json_object(value_json, '$.after.description') AS description,
                get_json_object(value_json, '$.after.merchant') AS merchant,
                CAST(get_json_object(value_json, '$.after.balance_after') AS DECIMAL(18, 2)) AS balance_after,
                get_json_object(value_json, '$.after.occurred_at') AS raw_occurred_at,
                get_json_object(value_json, '$.after.created_at') AS raw_created_at,
                dt AS bronze_dt,
                event_ts,
                partition_id,
                offset_value
            FROM {pg_cdc_table}
            WHERE source_table = 'transactions'
              AND op IN ('c', 'r', 'u')
              AND get_json_object(value_json, '$.after.id') IS NOT NULL
        ),
        ranked AS (
            SELECT
                transaction_id,
                user_id,
                account_id,
                type,
                direction,
                amount,
                currency,
                description,
                merchant,
                balance_after,
                {timestamp_from_column("raw_occurred_at")} AS occurred_at,
                {timestamp_from_column("raw_created_at")} AS created_at,
                bronze_dt,
                row_number() OVER (
                    PARTITION BY transaction_id
                    ORDER BY event_ts DESC, partition_id DESC, offset_value DESC
                ) AS rn
            FROM parsed
        )
        SELECT
            transaction_id,
            user_id,
            account_id,
            type,
            direction,
            amount,
            currency,
            description,
            merchant,
            balance_after,
            occurred_at,
            created_at,
            CAST(coalesce(to_date(occurred_at), bronze_dt) AS DATE) AS dt
        FROM ranked
        WHERE rn = 1
    """


def users_select(pg_cdc_table: str = BRONZE_CDC) -> str:
    return f"""
        WITH parsed AS (
            SELECT
                get_json_object(value_json, '$.after.id') AS user_id,
                get_json_object(value_json, '$.after.full_name') AS full_name,
                get_json_object(value_json, '$.after.email') AS email,
                get_json_object(value_json, '$.after.created_at') AS raw_created_at,
                event_ts,
                partition_id,
                offset_value
            FROM {pg_cdc_table}
            WHERE source_table = 'users'
              AND op IN ('c', 'r', 'u')
              AND get_json_object(value_json, '$.after.id') IS NOT NULL
        ),
        ranked AS (
            SELECT
                user_id,
                full_name,
                email,
                {timestamp_from_column("raw_created_at")} AS created_at,
                event_ts,
                row_number() OVER (
                    PARTITION BY user_id
                    ORDER BY event_ts DESC, partition_id DESC, offset_value DESC
                ) AS rn
            FROM parsed
        )
        SELECT user_id, full_name, email, created_at, event_ts
        FROM ranked
        WHERE rn = 1
    """


def accounts_select(pg_cdc_table: str = BRONZE_CDC) -> str:
    return f"""
        WITH parsed AS (
            SELECT
                get_json_object(value_json, '$.after.id') AS account_id,
                get_json_object(value_json, '$.after.user_id') AS user_id,
                get_json_object(value_json, '$.after.currency') AS currency,
                CAST(get_json_object(value_json, '$.after.opening_balance') AS DECIMAL(18, 2)) AS opening_balance,
                CAST(get_json_object(value_json, '$.after.current_balance') AS DECIMAL(18, 2)) AS current_balance,
                get_json_object(value_json, '$.after.created_at') AS raw_created_at,
                get_json_object(value_json, '$.after.updated_at') AS raw_updated_at,
                event_ts,
                partition_id,
                offset_value
            FROM {pg_cdc_table}
            WHERE source_table = 'accounts'
              AND op IN ('c', 'r', 'u')
              AND get_json_object(value_json, '$.after.id') IS NOT NULL
        ),
        ranked AS (
            SELECT
                account_id,
                user_id,
                currency,
                opening_balance,
                current_balance,
                {timestamp_from_column("raw_created_at")} AS created_at,
                {timestamp_from_column("raw_updated_at")} AS updated_at,
                event_ts,
                row_number() OVER (
                    PARTITION BY account_id
                    ORDER BY event_ts DESC, partition_id DESC, offset_value DESC
                ) AS rn
            FROM parsed
        )
        SELECT account_id, user_id, currency, opening_balance, current_balance,
               created_at, updated_at, event_ts
        FROM ranked
        WHERE rn = 1
    """


def clickstream_events_select(clickstream_table: str = BRONZE_CLICKSTREAM) -> str:
    return f"""
        WITH base AS (
            SELECT
                topic,
                partition_id,
                offset_value,
                event_id,
                session_id,
                user_id,
                event_type,
                page,
                element_id,
                x,
                y,
                dwell_ms,
                coalesce(event_ts, ingest_ts) AS event_ts,
                CAST(coalesce(to_date(event_ts), dt) AS DATE) AS dt
            FROM {clickstream_table}
            WHERE event_id IS NOT NULL
        ),
        sequenced AS (
            SELECT
                *,
                lag(event_ts) OVER (
                    PARTITION BY coalesce(session_id, event_id)
                    ORDER BY event_ts, topic, partition_id, offset_value
                ) AS previous_event_ts
            FROM base
        ),
        flagged AS (
            SELECT
                *,
                CASE
                    WHEN previous_event_ts IS NULL THEN 1
                    WHEN unix_timestamp(event_ts) - unix_timestamp(previous_event_ts) > {SESSION_IDLE_GAP_SECONDS} THEN 1
                    ELSE 0
                END AS new_session_flag
            FROM sequenced
        ),
        sessionized AS (
            SELECT
                *,
                sum(new_session_flag) OVER (
                    PARTITION BY coalesce(session_id, event_id)
                    ORDER BY event_ts, topic, partition_id, offset_value
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS session_number
            FROM flagged
        )
        SELECT
            event_id,
            session_id,
            CASE
                WHEN session_id IS NULL THEN NULL
                ELSE concat(session_id, '-', lpad(CAST(session_number AS STRING), 3, '0'))
            END AS analytics_session_id,
            user_id,
            event_type,
            page,
            element_id,
            x,
            y,
            dwell_ms,
            event_ts,
            dt
        FROM sessionized
    """


def sessions_select(clickstream_events_table: str = SILVER_CLICKSTREAM) -> str:
    return f"""
        SELECT
            analytics_session_id AS session_id,
            max(session_id) AS raw_session_id,
            max(user_id) AS user_id,
            min(event_ts) AS started_at,
            max(event_ts) AS ended_at,
            count(*) AS events_count,
            sum(coalesce(dwell_ms, 0)) AS total_dwell_ms,
            min(dt) AS dt
        FROM {clickstream_events_table}
        WHERE analytics_session_id IS NOT NULL
        GROUP BY analytics_session_id
    """


def current_balances_select(
    accounts_table: str = SILVER_ACCOUNTS,
    transactions_table: str = SILVER_TRANSACTIONS,
) -> str:
    # silver.transactions already keeps the latest version per transaction (Debezium c/r/u);
    # OLTP deletes (op='d') are not handled here and would drift this mart from accounts.current_balance.
    return f"""
        SELECT
            a.user_id,
            a.account_id,
            a.currency,
            CAST(
                coalesce(a.opening_balance, 0)
                + coalesce(sum(CASE WHEN t.direction = 'credit' THEN t.amount ELSE -t.amount END), 0)
                AS DECIMAL(18, 2)
            ) AS balance,
            count(t.transaction_id) AS transactions_count,
            current_timestamp() AS updated_at
        FROM {accounts_table} a
        LEFT JOIN {transactions_table} t
          ON a.account_id = t.account_id
        GROUP BY a.user_id, a.account_id, a.currency, a.opening_balance
    """


def transaction_volume_daily_select(transactions_table: str = SILVER_TRANSACTIONS) -> str:
    return f"""
        SELECT
            dt,
            user_id,
            currency,
            CAST(sum(CASE WHEN direction = 'credit' THEN amount ELSE 0 END) AS DECIMAL(18, 2)) AS credit_amount,
            CAST(sum(CASE WHEN direction = 'debit' THEN amount ELSE 0 END) AS DECIMAL(18, 2)) AS debit_amount,
            count(*) AS transaction_count
        FROM {transactions_table}
        GROUP BY dt, user_id, currency
    """


def page_engagement_daily_select(clickstream_events_table: str = SILVER_CLICKSTREAM) -> str:
    return f"""
        SELECT
            dt,
            page,
            count(DISTINCT analytics_session_id) AS sessions,
            count(*) AS events,
            sum(coalesce(dwell_ms, 0)) AS total_dwell_ms,
            avg(coalesce(dwell_ms, 0)) AS avg_dwell_ms
        FROM {clickstream_events_table}
        GROUP BY dt, page
    """


def button_clicks_daily_select(clickstream_events_table: str = SILVER_CLICKSTREAM) -> str:
    return f"""
        SELECT
            dt,
            page,
            element_id,
            count(*) AS clicks,
            count(DISTINCT user_id) AS users,
            count(DISTINCT analytics_session_id) AS sessions
        FROM {clickstream_events_table}
        WHERE event_type = 'click'
          AND element_id IS NOT NULL
        GROUP BY dt, page, element_id
    """


def conversion_funnel_daily_select(
    clickstream_events_table: str = SILVER_CLICKSTREAM,
    transactions_table: str = SILVER_TRANSACTIONS,
) -> str:
    cta_ids = ", ".join(FUNNEL_CTA_IDS)
    return f"""
        WITH days AS (
            SELECT dt FROM {clickstream_events_table}
            UNION
            SELECT dt FROM {transactions_table}
        ),
        views AS (
            SELECT dt, count(*) AS page_views
            FROM {clickstream_events_table}
            WHERE event_type = 'page_enter'
            GROUP BY dt
        ),
        clicks AS (
            SELECT dt, count(*) AS create_transaction_clicks
            FROM {clickstream_events_table}
            WHERE event_type = 'click'
              AND element_id IN ({cta_ids})
            GROUP BY dt
        ),
        tx AS (
            SELECT dt, count(*) AS transactions_created
            FROM {transactions_table}
            GROUP BY dt
        )
        SELECT
            d.dt,
            coalesce(v.page_views, 0) AS page_views,
            coalesce(c.create_transaction_clicks, 0) AS create_transaction_clicks,
            coalesce(tx.transactions_created, 0) AS transactions_created,
            CASE
                WHEN coalesce(v.page_views, 0) = 0 THEN 0
                ELSE CAST(coalesce(tx.transactions_created, 0) AS DOUBLE) / CAST(coalesce(v.page_views, 0) AS DOUBLE)
            END AS conversion_rate
        FROM days d
        LEFT JOIN views v ON d.dt = v.dt
        LEFT JOIN clicks c ON d.dt = c.dt
        LEFT JOIN tx ON d.dt = tx.dt
    """


def conversion_funnel_cta_daily_select(clickstream_events_table: str = SILVER_CLICKSTREAM) -> str:
    cta_ids = ", ".join(FUNNEL_CTA_IDS)
    return f"""
        SELECT
            dt,
            element_id,
            count(*) AS clicks,
            count(DISTINCT user_id) AS users,
            count(DISTINCT analytics_session_id) AS sessions
        FROM {clickstream_events_table}
        WHERE event_type = 'click'
          AND element_id IN ({cta_ids})
        GROUP BY dt, element_id
    """


def user_engagement_daily_select(
    clickstream_events_table: str = SILVER_CLICKSTREAM,
    transactions_table: str = SILVER_TRANSACTIONS,
) -> str:
    return f"""
        WITH behavior AS (
            SELECT
                dt,
                user_id,
                count(DISTINCT analytics_session_id) AS sessions,
                sum(coalesce(dwell_ms, 0)) AS total_dwell_ms,
                sum(CASE WHEN event_type = 'page_enter' THEN 1 ELSE 0 END) AS page_views,
                sum(CASE WHEN event_type = 'click' THEN 1 ELSE 0 END) AS clicks
            FROM {clickstream_events_table}
            WHERE user_id IS NOT NULL
            GROUP BY dt, user_id
        ),
        money AS (
            SELECT
                dt,
                user_id,
                count(*) AS transactions_count,
                CAST(sum(CASE WHEN direction = 'debit' THEN amount ELSE 0 END) AS DECIMAL(18, 2)) AS debit_amount,
                CAST(sum(CASE WHEN direction = 'credit' THEN amount ELSE 0 END) AS DECIMAL(18, 2)) AS credit_amount
            FROM {transactions_table}
            WHERE user_id IS NOT NULL
            GROUP BY dt, user_id
        )
        SELECT
            coalesce(b.dt, m.dt) AS dt,
            coalesce(b.user_id, m.user_id) AS user_id,
            coalesce(b.sessions, 0) AS sessions,
            coalesce(b.total_dwell_ms, 0) AS total_dwell_ms,
            coalesce(b.page_views, 0) AS page_views,
            coalesce(b.clicks, 0) AS clicks,
            coalesce(m.transactions_count, 0) AS transactions_count,
            CAST(coalesce(m.debit_amount, 0) AS DECIMAL(18, 2)) AS debit_amount,
            CAST(coalesce(m.credit_amount, 0) AS DECIMAL(18, 2)) AS credit_amount
        FROM behavior b
        FULL OUTER JOIN money m
          ON b.dt = m.dt AND b.user_id = m.user_id
    """


def gold_sessions_select(sessions_table: str = SILVER_SESSIONS) -> str:
    return f"""
        SELECT
            session_id,
            raw_session_id,
            user_id,
            started_at,
            ended_at,
            events_count,
            total_dwell_ms,
            dt
        FROM {sessions_table}
    """


def overwrite_table(
    spark,
    table_name: str,
    columns: tuple[str, ...],
    select_sql: str,
) -> None:
    column_list = ", ".join(columns)
    spark.sql(f"INSERT OVERWRITE TABLE {table_name} ({column_list})\n{select_sql}")


def main() -> None:
    spark = get_spark("microdp-build-marts")

    jobs = [
        (SILVER_TRANSACTIONS, SILVER_TRANSACTIONS_COLUMNS, transactions_select()),
        (SILVER_USERS, SILVER_USERS_COLUMNS, users_select()),
        (SILVER_ACCOUNTS, SILVER_ACCOUNTS_COLUMNS, accounts_select()),
        (SILVER_CLICKSTREAM, SILVER_CLICKSTREAM_COLUMNS, clickstream_events_select()),
        (SILVER_SESSIONS, SESSIONS_COLUMNS, sessions_select()),
        (GOLD_CURRENT_BALANCES, GOLD_CURRENT_BALANCES_COLUMNS, current_balances_select()),
        (
            GOLD_TRANSACTION_VOLUME_DAILY,
            GOLD_TRANSACTION_VOLUME_DAILY_COLUMNS,
            transaction_volume_daily_select(),
        ),
        (
            GOLD_PAGE_ENGAGEMENT_DAILY,
            GOLD_PAGE_ENGAGEMENT_DAILY_COLUMNS,
            page_engagement_daily_select(),
        ),
        (GOLD_BUTTON_CLICKS_DAILY, GOLD_BUTTON_CLICKS_DAILY_COLUMNS, button_clicks_daily_select()),
        (
            GOLD_CONVERSION_FUNNEL_DAILY,
            GOLD_CONVERSION_FUNNEL_DAILY_COLUMNS,
            conversion_funnel_daily_select(),
        ),
        (
            GOLD_CONVERSION_FUNNEL_CTA_DAILY,
            GOLD_CONVERSION_FUNNEL_CTA_DAILY_COLUMNS,
            conversion_funnel_cta_daily_select(),
        ),
        (
            GOLD_USER_ENGAGEMENT_DAILY,
            GOLD_USER_ENGAGEMENT_DAILY_COLUMNS,
            user_engagement_daily_select(),
        ),
        (GOLD_SESSIONS, SESSIONS_COLUMNS, gold_sessions_select()),
    ]

    try:
        for table_name, columns, select_sql in jobs:
            overwrite_table(spark, table_name, columns, select_sql)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
