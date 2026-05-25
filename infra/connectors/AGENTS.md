# Debezium Connectors

Конфигурации Kafka Connect/Debezium для CDC из PostgreSQL.

## Назначение

- `wallet-postgres.json.template` — шаблон конфигурации; реальные значения подставляются Airflow DAG из env-переменных `WALLET_DB_*` через `string.Template.substitute()`.
- Connector регистрируется DAG через Kafka Connect REST API и пишет изменения `public.users`, `public.accounts`, `public.transactions` в Kafka topics с prefix `wallet`.

## Требования

- Хранить в Git только `.template`-файл с `${VAR}`-плейсхолдерами; финальный JSON c секретами не коммитим.
- `WALLET_DB_HOST/PORT/USER/PASSWORD/NAME` приходят из `infra/docker-compose.yml` (`airflow-common-env`), который зашивает `POSTGRES_*` из `.env`.
- Используй PostgreSQL `pgoutput`.
- Сохраняй `snapshot.mode=initial`, чтобы первый запуск наполнял Kafka начальными данными.
- Сохраняй `decimal.handling.mode=string`, потому что Spark jobs ожидают decimal как строку в JSON.
- Если добавляешь таблицы, обнови:
  - `table.include.list`;
  - PostgreSQL replica identity;
  - Spark Bronze/Silver transformation;
  - документацию demo flow.
- Не меняй `topic.prefix` без обновления Spark `subscribePattern`.

