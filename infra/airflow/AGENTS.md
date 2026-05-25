# Airflow Image

Кастомный образ Airflow для запуска DAG и Spark/Python dependencies.

## Назначение

- Расширяет официальный Airflow image.
- Добавляет Java runtime для PySpark.
- Добавляет `pyspark`, Kafka client, requests и Spark provider.

## Требования

- Версию Airflow base image держи закрепленной в Dockerfile/Compose.
- Не добавляй тяжелые provider-пакеты без необходимости; образ и так крупный.
- Если меняешь `requirements.txt`, проверь сборку `airflow-init` и import DAG.
- Java runtime нужен для локального Spark execution; не удаляй его, пока Spark jobs запускаются из Airflow tasks.
- Spark-задачи DAG используют `SparkSubmitOperator` с conn `spark_default`. Conn регистрируется через env `AIRFLOW_CONN_SPARK_DEFAULT` в `infra/docker-compose.yml` (`master=local[*]`, `spark-binary=spark-submit`). `spark-submit` приходит с `pyspark` через `~/.local/bin`.

## Проверки

- `docker compose -f infra/docker-compose.yml --env-file .env.example build airflow-init`
- DAG import + structure check:
  `docker run --rm -e PYTHONPATH=/opt/airflow -v "$PWD/etl/airflow:/opt/airflow/etl_airflow:ro" -v "$PWD/etl/airflow/dags:/opt/airflow/dags:ro" -v "$PWD/infra/connectors:/opt/airflow/connectors:ro" -w /opt/airflow/etl_airflow microdp-airflow:0.1.0 pytest -q tests/test_dag.py`

