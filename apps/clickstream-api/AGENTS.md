# Clickstream API

FastAPI-сервис для приема batch-событий пользовательского поведения и публикации их в Kafka-compatible topic Redpanda.

## Назначение

- Принимает события UI через `POST /api/clickstream/events`.
- Валидирует clickstream-схему Pydantic-моделями.
- Публикует события в topic `clickstream.events` или значение `CLICKSTREAM_TOPIC`.

## Схема события

Источник истины — `apps/clickstream-api/app/schemas.py` (`ClickstreamEvent`). Поля:

| Поле | Тип | Обязательность |
|---|---|---|
| `event_id` | string (1..120) | required |
| `session_id` | string (1..120) | required |
| `user_id` | UUID | optional |
| `event_type` | enum: `page_enter`, `page_leave`, `route_change`, `click`, `mouse_move` | required |
| `page` | string (1..240) | required |
| `ts` | ISO 8601 datetime | required |
| `element_id` | string (..160) | optional |
| `x` | int | optional |
| `y` | int | optional |
| `dwell_ms` | int ≥ 0 | optional |
| `payload` | object | default `{}` |

При изменении схемы синхронизируй: root `AGENTS.md`, smoke-пример в `docs/demo-runbook.md`, `wallet-ui` tracker и Spark `ingest_clickstream.py`.

## Требования

- Сохраняй batch endpoint и текущую схему события.
- События, не ушедшие в основной топик после `retries`, должны попадать в DLQ-топик (`CLICKSTREAM_DLQ_TOPIC`, по умолчанию `clickstream.events.dlq`); только при неуспехе DLQ публикация возвращает 503.
- Не добавляй тяжелую синхронную обработку в request path; сервис должен быстро принять и отправить события.
- Kafka key должен оставаться связанным с `session_id`, чтобы события одной сессии естественно группировались.
- Для Python 3.12 используй совместимую версию Kafka-клиента; текущая закреплённая — `kafka-python==2.3.1` (`requirements.txt`), не возвращайся на `kafka-python==2.0.2`.
- Unit-тесты не должны требовать запущенного Kafka.
- CORS открыт (`allow_origins=["*"]`) — это demo-only для UI/curl. Перед выходом за пределы стенда сузить до конкретных origin'ов.

## Проверки

- `docker run --rm -e PYTHONPATH=/app -v "$PWD/apps/clickstream-api/tests:/app/tests:ro" microdp-clickstream-api pytest -q`
- `python3 -m py_compile apps/clickstream-api/app/*.py apps/clickstream-api/tests/*.py`

