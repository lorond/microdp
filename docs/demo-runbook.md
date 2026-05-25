# Demo Runbook

## Start

```bash
./scripts/run.sh
```

Скрипт сам создаёт `.env` из `.env.example`, ротирует placeholder
`SUPERSET_SECRET_KEY` через `openssl rand -hex 32` и поднимает стенд через
`docker compose ... up --build` в foreground (Ctrl+C делает graceful shutdown).
Параллельно output каждого контейнера дублируется в `logs/<service>.log` без
префикса `service-1 |`. Повторные запуски идемпотентны.

Wait until the UI, Airflow, StarRocks, and Superset containers are healthy enough for the first demo pass. The first Airflow image build installs Java and Spark dependencies, so it can take a few minutes.

## Flow

1. Open the wallet UI at http://localhost:3000.
2. Create a deposit or payment. The wallet API writes it into PostgreSQL.
3. Move around the UI and change tabs. The browser sends clickstream batches to the clickstream API.
4. In Airflow, run `microdp_data_platform_e2e` or wait for its 10-minute scheduled run.
5. Query StarRocks:

```bash
docker compose -f infra/docker-compose.yml exec starrocks-fe \
  mysql -P 9030 -h 127.0.0.1 -u root
```

Example SQL:

```sql
SHOW CATALOGS;
SET CATALOG demo_lake;
USE gold;
SHOW TABLES;
SELECT * FROM current_balances LIMIT 10;
SELECT * FROM page_engagement_daily LIMIT 10;
SELECT * FROM user_engagement_daily LIMIT 10;
SELECT * FROM sessions LIMIT 10;
-- Альтернатива: использовать полный путь без SET/USE:
-- SELECT * FROM demo_lake.gold.current_balances LIMIT 10;
```

6. Open Superset at http://localhost:8088. Datasource `StarRocks Gold` уже зарегистрирован
   bootstrap-скриптом (URI `starrocks+pymysql://root@starrocks-fe:9030/demo_lake.gold`),
   так что можно сразу идти в `Datasets` / `SQL Lab` и работать с таблицами `current_balances`,
   `user_engagement_daily`, `page_engagement_daily` и т.д. — без ручной настройки подключения.

## Smoke Checks

```bash
curl http://localhost:8000/health
curl http://localhost:8001/health
curl http://localhost:8000/api/users/00000000-0000-0000-0000-000000000001/balance
```

Create a transaction:

```bash
curl -X POST http://localhost:8000/api/users/00000000-0000-0000-0000-000000000001/transactions \
  -H 'Content-Type: application/json' \
  -d '{"type":"deposit","amount":"25.00","currency":"USD","description":"Smoke deposit"}'
```

Send a clickstream event:

```bash
curl -X POST http://localhost:8001/api/clickstream/events \
  -H 'Content-Type: application/json' \
  -d '{"events":[{"event_id":"smoke-1","session_id":"session-smoke","user_id":"00000000-0000-0000-0000-000000000001","event_type":"click","page":"/","ts":"2026-05-22T12:00:00Z","element_id":"smoke-button","x":10,"y":20,"dwell_ms":0,"payload":{"source":"curl"}}]}'
```

The same API smoke path is available as:

```bash
sh scripts/smoke.sh
```

If you run the clickstream API directly on your host instead of through Compose, use
`KAFKA_BOOTSTRAP_SERVERS=localhost:19092`; inside Compose the default is `redpanda:9092`.

## Debug & Logs

Compose-родной способ — `docker compose -f infra/docker-compose.yml logs -f <service>`.

| Слой | Где смотреть |
|---|---|
| Wallet API / Clickstream API | `docker compose logs -f wallet-api`, `... logs -f clickstream-api` |
| PostgreSQL | `docker compose logs -f postgres`; `docker compose exec postgres psql -U wallet wallet` |
| Debezium Connect | `docker compose logs -f connect`; `curl http://localhost:8083/connectors/wallet-postgres-connector/status` |
| Kafka/Redpanda | Redpanda Console: http://localhost:8089; CLI: `docker compose exec redpanda rpk topic list`, `... rpk topic consume wallet.public.transactions --num 5` |
| Airflow | UI: http://localhost:8080 (логи каждой таски в табе Logs); CLI: `docker compose logs -f airflow-scheduler` |
| Spark | DAG-таски печатают stdout в Airflow Logs; для запуска вручную — `docker compose exec airflow-scheduler python /opt/airflow/spark/<job>.py` |
| Nessie (Iceberg catalog) | `docker compose logs -f nessie`; REST API: http://localhost:19120/api/v2; Iceberg endpoint: http://localhost:19120/iceberg; список таблиц — через Spark `SHOW TABLES IN lakehouse.bronze` |
| Garage (S3) | WebUI: http://localhost:3909 (access key `microdp-admin`, secret из `.env`); S3 API: http://localhost:3900; checkpoint'ы лежат под `s3://warehouse/_checkpoints/bronze/`. Ad-hoc: `docker compose exec s3 /garage bucket list`, `... /garage key info microdp-admin` |
| StarRocks | `docker compose exec starrocks-fe mysql -P 9030 -h 127.0.0.1 -u root`; FE-логи: `docker compose logs -f starrocks-fe` |
| Superset | `docker compose logs -f superset`; UI: http://localhost:8088 |

### Куда копать при типовых проблемах

- **DAG падает на `wait_for_connect`** — Debezium ещё не поднялся; подождать или `docker compose restart connect`.
- **`ingest_*` падает с Kafka `UnknownTopicOrPartition`** — Debezium не успел создать топики; убедиться, что коннектор `wallet-postgres-connector` в статусе RUNNING.
- **DAG `smoke_check_layers` падает** — это structural check (только наличие таблиц). Если падает — `init_lakehouse_tables` не прошёл; смотри его логи. Для проверки наличия данных запусти вручную `docker compose exec airflow-scheduler python /opt/airflow/spark/smoke_data.py`.
- **Дубликаты в Bronze после пересоздания стенда** — удалили checkpoint'ы в Garage; либо очистить и таблицы (`spark.sql("DELETE FROM lakehouse.bronze.*")`), либо принять дубликаты — Silver их схлопывает.
- **StarRocks не видит Iceberg-таблицу** — `REFRESH EXTERNAL TABLE demo_lake.gold.<table>` или пересоздать external catalog.
- **Superset не подключается к StarRocks** — проверить DSN `starrocks+pymysql://root@starrocks-fe:9030/demo_lake.gold` и что FE healthy.
