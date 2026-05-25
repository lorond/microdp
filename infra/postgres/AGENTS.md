# PostgreSQL Bootstrap

Инициализация операционной базы кошелька и базы Airflow metadata.

## Назначение

- Создает database/user для Airflow.
- Создает database/user для Nessie (catalog version-store через JDBC2).
- Создает wallet schema: `users`, `accounts`, `transactions`.
- Включает начальные demo records.
- Debezium CDC использует `REPLICA IDENTITY DEFAULT` (по primary key) — текущий Spark normalization читает только `$.after.*`, поэтому BEFORE image для UPDATE/DELETE не нужен.

## Требования

- Сохраняй совместимость с PostgreSQL logical replication.
- Если потребуется захватывать полный BEFORE image (например для аудита удалений), верни `REPLICA IDENTITY FULL` точечно для нужных таблиц.
- Изменения таблиц должны быть отражены в Debezium connector config и Spark normalization jobs.
- Seed data должны оставаться детерминированными для demo user ids.
- Init SQL выполняется только при первом создании volume; документируй это, если меняешь bootstrap-поведение.

