# MicroDP — локальный демо-стенд платформы данных

Локальный end-to-end стенд платформы данных в сценарии «кошелёк». Запускается одной
командой через Docker Compose и показывает полный путь данных от пользовательского клика
в UI до витрины в BI.

## Быстрый старт

```bash
./scripts/run.sh
```

Вывод каждого контейнера дублируется в `logs/<service>.log`.

## Главные эндпоинты

| Сервис | URL |
|---|---|
| Wallet UI | http://localhost:3000 |
| Wallet API (OpenAPI) | http://localhost:8000/docs |
| Clickstream API (OpenAPI) | http://localhost:8001/docs |
| Airflow | http://localhost:8080 |
| Superset | http://localhost:8088 |
| Garage WebUI | http://localhost:3909 (S3 API — http://localhost:3900) |
| StarRocks FE UI | http://localhost:8030 |
| Redpanda Console | http://localhost:8089 |
| Nessie API | http://localhost:19120/api/v2 (Iceberg REST: http://localhost:19120/iceberg) |

## Что в составе

Прикладные сервисы:

- **Wallet UI** — интерфейс кошелька: баланс, история, форма операции, экран активности пользователя.
  Раздаётся через nginx, который проксирует `/api/*` на бэкенды.
- **Wallet API** — операционная часть кошелька: чтение баланса, истории, создание транзакций.
- **Clickstream API** — приём batch-событий поведения пользователя, публикация в Redpanda.
- **Transaction Emulator** — фоновый генератор демо-нагрузки. Отключается через `EMULATOR_ENABLED=false`.

Инфраструктура хранения и стриминга:

- **PostgreSQL** — OLTP-БД кошелька, плюс отдельные database/user для Airflow и Nessie.
  Включён logical replication для CDC.
- **Debezium Connect** — снимает CDC с PostgreSQL (plugin `pgoutput`, publication `wallet_publication`)
  и пишет в Kafka topics с префиксом `wallet.public.*`.
- **Redpanda** — Kafka-совместимый брокер.
- **Redpanda Console** — UI для просмотра топиков и Kafka Connect.

Lakehouse и аналитика:

- **Garage** — объектное S3-совместимое хранилище Iceberg-данных (`s3://warehouse/`).
- **Project Nessie** — Iceberg REST catalog.
- **Apache Airflow** — оркестратор. Один DAG `microdp_data_platform_e2e` запускается каждые 10 минут.
- **Apache Spark** — встроен в Airflow-образ, исполняется через `SparkSubmitOperator`
  в режиме `local[*]`. Job'ы пишут Bronze/Silver/Gold таблицы Iceberg.
- **StarRocks** — query engine. Подключается к Iceberg как external catalog `demo_lake` через Nessie REST.
- **Apache Superset** — BI поверх StarRocks через нативный SQLAlchemy dialect `starrocks`.

## Архитектура

```
   ┌──────────────────────┐             ┌──────────────────────┐
   │      Wallet UI       │             │ Transaction Emulator │
   └──────────┬───────────┘             └──────────┬───────────┘
              │                                    │
              │   HTTP (wallet + clickstream)      │
              └────────────────┬───────────────────┘
                               │
              ┌────────────────┴───────────────────┐
              ▼                                    ▼
   ┌──────────────────────┐             ┌──────────────────────┐
   │      Wallet API      │             │   Clickstream API    │
   └──────────┬───────────┘             └──────────┬───────────┘
              │ SQL                                │ produce
              ▼                                    ▼
   ┌──────────────────────┐             ┌──────────────────────┐
   │      PostgreSQL      │  Debezium   │       Redpanda       │
   │                      │   Connect   │   wallet.public.*    │
   │                      ├──── CDC ───►┤   clickstream.events │
   │                      │             │   *.dlq              │
   └──────────────────────┘             └──────────┬───────────┘
                                                   │
                                                   │ Spark Streaming
                                                   ▼
                                   ┌──────────────────────────────┐
                                   │           Airflow            │
                                   │  microdp_data_platform_e2e   │
                                   └───────────────┬──────────────┘
                                                   │ SparkSubmitOperator
                                                   ▼
                                   ┌──────────────────────────────┐
                                   │  Lakehouse (Apache Iceberg)  │
                                   │  данные: s3://warehouse      │
                                   │  метаданные: Project Nessie  │
                                   └───────────────┬──────────────┘
                                                   │ Iceberg REST catalog
                                                   ▼
                                   ┌──────────────────────────────┐
                                   │          StarRocks           │
                                   └───────────────┬──────────────┘
                                                   │ starrocks+pymysql://
                                                   ▼
                                   ┌──────────────────────────────┐
                                   │           Superset           │
                                   └──────────────────────────────┘
```

## Поток данных

1. **Пользователь** (или эмулятор) взаимодействует с UI. Wallet API принимает
   транзакции и пишет их в PostgreSQL атомарно. Clickstream API принимает batch
   событий поведения и публикует их в топик `clickstream.events`.
2. **Debezium** через logical replication слота `wallet_slot` (`pgoutput`) снимает
   изменения из таблиц `users`/`accounts`/`transactions` и пишет их в Kafka в топики
   `wallet.public.<table>`.
3. **Airflow DAG** раз в 10 минут (или вручную) выполняет полный e2e-цикл.
4. **Spark** пишет данные в `s3://warehouse` на Garage; коммиты и метаданные таблиц
   живут в Nessie.
5. **StarRocks** видит lakehouse через external catalog `demo_lake`, читает витрины
   `demo_lake.gold.*` напрямую из Iceberg без копирования.
6. **Superset** подключается к StarRocks через нативный SQLAlchemy dialect
   `starrocks+pymysql://` и строит дашборды поверх Gold-витрин.

Все Bronze-таблицы append-only; Silver/Gold пересобираются полным
`INSERT OVERWRITE` в dynamic-mode (затрагиваются только `dt`-партиции, присутствующие
в источнике). Невалидные сообщения отправляются в DLQ-таблицы Bronze
(`dlq_pg_cdc`, `dlq_clickstream`).

## Демо-credentials

Все creds — **demo-only**, в production не использовать.

| Сервис | Логин / пароль |
|---|---|
| Airflow | `admin` / `admin` |
| Superset | `admin` / `admin` |
| Garage S3 / WebUI | access key `microdp-admin` / secret из `.env` `S3_SECRET_KEY` |
| StarRocks UI | `admin` / `admin` |
| PostgreSQL (wallet) | `wallet` / `wallet` |

## Airflow DAG и обновление витрин

DAG `microdp_data_platform_e2e` крутится по расписанию `*/10 * * * *`. Для
немедленного обновления во время демо — запустить вручную через Airflow UI
(`Trigger DAG`). DAG автоматически дожидается Debezium Connect, регистрирует
коннектор (idempotent — POST при отсутствии, PUT при изменении), создаёт Iceberg
таблицы, дотягивает новые сообщения из Kafka в Bronze, пересобирает Silver/Gold
и проверяет наличие всех таблиц.

После прогона DAG-а данные доступны в StarRocks:

```sql
SET CATALOG demo_lake;
USE gold;
SELECT * FROM current_balances LIMIT 10;
SELECT * FROM user_engagement_daily ORDER BY dt DESC LIMIT 10;
SELECT * FROM conversion_funnel_daily LIMIT 10;
```
