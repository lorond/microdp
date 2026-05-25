# Infrastructure

Docker Compose и bootstrap-инфраструктура локального стенда.

## Назначение

Поднимает PostgreSQL, Redpanda, Debezium Connect, Garage (S3-совместимое объектное хранилище) + Garage WebUI, Nessie (Iceberg catalog), Spark, Airflow, StarRocks, Superset и прикладные сервисы.

## Garage

- Сервис `s3` (`dxflrs/garage:v2.3.0`) запускается с флагами `--single-node --default-bucket --default-access-key` — Garage сам создаёт layout (zone `dc1`, capacity `1G`), импортирует ключ из env (`GARAGE_DEFAULT_ACCESS_KEY/SECRET_KEY = ${S3_ACCESS_KEY}/${S3_SECRET_KEY}`) и заводит bucket `warehouse` с RWO-доступом. Отдельного init-контейнера нет.
- Конфиг лежит в `infra/garage/garage.toml`, монтируется read-only в `s3` и `s3-ui`. Секреты `rpc_secret`/`admin_token` подаются через env (`GARAGE_RPC_SECRET`, `GARAGE_ADMIN_TOKEN`) — в TOML их нет.
- Garage 2.x требует **access key id ≥ 8 символов и secret ≥ 16 символов**, иначе старт падает с `Invalid default access key`. Defaults в `.env.example`: `microdp-admin` / 32-hex.
- Image на `scratch` — внутри нет `sh`/`env`/`ls`/`curl`. Healthcheck идёт через сам бинарь (`/garage status`); ad-hoc операции — `docker exec microdp-s3-1 /garage <cmd>` (например, `/garage bucket list`, `/garage key info microdp-admin`).
- WebUI (`khairul169/garage-webui:1.1.0`, сервис `s3-ui`, порт хоста `3909`) требует mount'а того же `garage.toml`; без файла он не парсит конфиг и отдаёт ответы по HTTP/0.9 без заголовков.
- Внутри docker-сети endpoint у Garage — `http://s3:3900`. На хост проброшен только S3 API (`3900:3900`); RPC (`3901`), web (`3902`), admin (`3903`) остаются внутренними.

## Требования

- Основной файл запуска: `infra/docker-compose.yml`.
- Не используй `latest` для Docker images; закрепляй версии в `.env.example` и Compose fallback values.
- Сохраняй single-node профиль, рассчитанный на локальный demo/MVP.
- Не добавляй Kubernetes, Helm, Terraform или production secrets без явной задачи.
- Все bind mounts должны быть относительны структуре репозитория.
- При изменении сервисов обновляй `README.md` и `docs/demo-runbook.md`, если меняются порты, credentials или команды.
- Для локальной TLS/CA-среды Dockerfiles используют demo-friendly install flags; не представляй это как production security pattern.

## Запуск compose-команд

- Основной запуск (`./scripts/run.sh` и `README.md`) использует `docker compose -f infra/docker-compose.yml --env-file .env up --build` — флаг `--env-file .env` обязателен, потому что без него `${VAR:?...}`-проверки в compose (например, `AIRFLOW__API_AUTH__JWT_SECRET`) увидят пустые значения и упадут.
- Для ad-hoc команд из корня репозитория (`docker compose -f infra/docker-compose.yml exec/logs/ps ...`) `--env-file` опционален: compose автоматически подхватывает `.env` из cwd. В `docs/demo-runbook.md` примеры намеренно даны без `--env-file` — для краткости. Это работает только если cwd = корень репозитория; при запуске из другого каталога — добавляй флаг явно.

## Проверки

- `docker compose -f infra/docker-compose.yml --env-file .env.example config --quiet`
- Для измененных образов: `docker compose -f infra/docker-compose.yml --env-file .env.example build <service>`

