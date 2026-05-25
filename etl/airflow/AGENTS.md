# Airflow DAGs

Оркестрация регистрации Debezium connector, инициализации Iceberg tables, ingestion и сборки витрин.

## Назначение

- DAG `microdp_data_platform_e2e` запускается каждые 10 минут.
- Регистрирует/обновляет Debezium connector через Kafka Connect REST API.
- Запускает Spark scripts из `/opt/airflow/spark`.

## Требования

- DAG import не должен требовать доступных внешних сервисов; сетевые вызовы должны быть внутри task body.
- Не используй top-level side effects кроме объявления DAG.
- Сохраняй `max_active_runs=1`, чтобы локальные Spark jobs не конкурировали за ресурсы.
- Если меняешь порядок задач, проверь, что init tables выполняется до ingestion/build marts.
- Не хардкодь секреты за пределами demo defaults.

## Проверки

- `python3 -m py_compile etl/airflow/dags/*.py`
- DAG import check из `infra/airflow/AGENTS.md`.

