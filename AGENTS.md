# MicroDP Data Platform Demo

Демонстрационный стенд платформы данных для сценария "кошелек/банк".
## Назначение

Стенд показывает полный E2E-поток данных:

- пользователь работает в React UI;
- wallet API пишет пользовательские транзакции в PostgreSQL;
- clickstream API собирает события поведения пользователя и отправляет их в Redpanda/Kafka;
- Debezium снимает CDC из PostgreSQL;
- Airflow запускает Spark jobs, которые пишут Bronze/Silver/Gold слои в Apache Iceberg (Parquet под капотом) на Garage (S3-совместимое объектное хранилище);
- StarRocks читает витрины через Iceberg external catalog;
- Superset подключается к StarRocks для BI-демо.

## Общие Требования

- Это локальный демонстрационный стенд, а не production-платформа. Не добавляй production-hardening, security framework или сложную оркестрацию без явной задачи.
- Основной способ запуска должен оставаться `docker compose -f infra/docker-compose.yml --env-file .env up --build`.
- Не используй Docker image tag `latest`; версии инфраструктурных образов и зависимостей должны быть закреплены.
- Сохраняй публичные контракты API:
  - `GET /api/users/{user_id}/balance`
  - `GET /api/users/{user_id}/transactions`
  - `POST /api/users/{user_id}/transactions`
  - `POST /api/clickstream/events`
  - `GET /health` (оба API — используется в smoke.sh, runbook и compose-healthcheck'ах)
- Сохраняй clickstream-схему: `event_id`, `session_id`, `user_id`, `event_type`, `page`, `ts`, `element_id`, `x`, `y`, `dwell_ms`, `payload`.
- CDC в lakehouse реализован append-only. Не добавляй Iceberg `MERGE`/upsert-логику без явного изменения архитектуры.
- Bronze хранит raw-события, Silver нормализует, Gold содержит витрины для StarRocks/Superset.
- Все демо-секреты в `.env.example` локальные и небезопасные; не переиспользуй их как production-рекомендации.
- При изменениях проверяй минимум:
  - `docker compose -f infra/docker-compose.yml --env-file .env.example config --quiet`
  - `python3 -m py_compile ...` для затронутых Python-файлов
  - unit-тесты соответствующего сервиса, если меняешь API или модели
  - production build UI, если меняешь frontend.

## Границы Ответственности

- `apps/` содержит пользовательские и эмуляционные сервисы.
- `infra/` содержит Docker Compose, init scripts и конфигурацию инфраструктуры.
- `etl/` содержит Airflow DAGs и Spark jobs.
- `sql/` содержит SQL bootstrap для аналитического доступа.
- `docs/` содержит инструкции для демонстрации и smoke checks.
- `scripts/` содержит вспомогательные локальные проверки.

## Порядок внесения изменений

Файл [FIXME.md](FIXME.md) описывает замечания и план по доработкам и исправлениям, может быть пустым, если задач нет. Каждый пункт пронумерован. Пункты сгурппированы по критичности.

Файл [GIT_POLICY.md](GIT_POLICY.md) описывает требования к работе с git репозиторием.

Файл [REQUIREMENTS.md](REQUIREMENTS.md) описывает изначальные требования к системе.
