# SQL Assets

SQL-файлы для bootstrap аналитического слоя и хранения справочных запросов. Не путать с DDL OLTP-схемы.

## Структура и владение

| Путь | Цель | Где исполняется | Кто отвечает |
|---|---|---|---|
| `sql/starrocks/` | Bootstrap StarRocks (external catalog `demo_lake` поверх Nessie Iceberg REST) | Compose-сервис `starrocks-init` прогоняет файлы через `mysql` клиент после старта FE | StarRocks/lakehouse |
| `infra/postgres/init/` | OLTP-схема `wallet` + seed демо-данных, ставится `postgres` контейнером на первом запуске volume `postgres-data` | Postgres entrypoint при пустом data dir | wallet-api / OLTP |
| `etl/spark/init_tables.py` | DDL Iceberg-таблиц Bronze/Silver/Gold через Spark SQL (это не файл `.sql`, но единый источник истины для lakehouse-схемы) | Airflow таска `init_lakehouse_tables` | etl/spark |

## Правила

- SQL для StarRocks лежит только в `sql/starrocks/` и должен быть совместим со StarRocks SQL dialect.
- Не смешивай PostgreSQL DDL и StarRocks DDL в одном файле.
- Любая правка имени catalog/database/table должна быть синхронизирована с: `infra/docker-compose.yml`, `etl/spark/*`, Superset datasource, runbook и `etl/spark/init_tables.py`.
- Lakehouse-таблицы (Bronze/Silver/Gold) держим в Spark, не дублируй их DDL в `sql/`.
- Если меняешь S3 credentials (Garage) — обнови `.env.example` (`S3_ACCESS_KEY` / `S3_SECRET_KEY`), Compose, `etl/spark/common.py` и `sql/starrocks/init_iceberg_catalog.sql`. Помни про ограничения Garage 2.x: access key id ≥ 8 символов, secret ≥ 16.
- Demo-UUID'ы пользователей (`00000000-0000-0000-0000-000000000001/2`) захардкожены в `infra/postgres/init/001_init.sql` и используются в `transaction-emulator`, runbook и smoke-скриптах — не меняй их без обновления всех точек.

## Миграции

- OLTP миграций нет: файлы в `infra/postgres/init/` исполняются один раз при пустом volume `postgres-data`. Чтобы переинициализировать схему, удали volume (`docker compose down -v`).
- Lakehouse evolution идёт через `init_tables.py` (`CREATE TABLE IF NOT EXISTS`) — изменения схемы (добавление колонок, переименования, удаления) требуют ручного `ALTER TABLE` через Spark.

## Проверки

- Postgres init применился: `docker compose exec postgres psql -U wallet wallet -c "\dt"`.
- StarRocks catalog поднят: `SHOW CATALOGS;` в FE → видна строка `demo_lake`.
