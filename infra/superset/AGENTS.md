# Superset Bootstrap

Кастомный Superset image и bootstrap для BI-доступа к StarRocks.

## Назначение

- Ставит нативный SQLAlchemy dialect `starrocks==1.3.3` (StarRocksEngineSpec) и DBAPI `PyMySQL==1.1.1` (через `uv pip` в `/app/.venv` — Superset 6.x живёт на uv-managed venv).
- Создает demo admin пользователя.
- Datasource `StarRocks Gold` к catalog `demo_lake.gold` регистрируется автоматически в `bootstrap.sh` через `superset set-database-uri` (URI: `starrocks+pymysql://root@starrocks-fe:9030/demo_lake.gold`). Демонстратору не нужно ничего настраивать в UI — можно сразу создавать чарты. Переопределить URI можно через env `STARROCKS_SQLALCHEMY_URI`. Старый CLI `superset import-datasources` удалён в Superset 6.0, поэтому `set-database-uri` — текущий механизм.

## Требования

- Superset credentials остаются demo-only.
- `bootstrap.sh` падает, если `SUPERSET_SECRET_KEY` пуст или равен плейсхолдеру `change-me-for-local-demo-only`. Реальный ключ выставляется в `.env` (например `openssl rand -hex 32`).
- Datasource должен указывать на StarRocks FE по Compose hostname `starrocks-fe:9030`.
- Если меняется имя Iceberg catalog, database или Gold namespace, обнови `docs/demo-runbook.md` и документацию.
- Bootstrap script должен быть idempotent: повторный запуск не должен ломать контейнер.

## Проверки

- `docker compose -f infra/docker-compose.yml --env-file .env.example build superset`

