#!/bin/sh
set -eu

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

ENV_FILE=".env"
ENV_EXAMPLE=".env.example"
PLACEHOLDER_SECRET="change-me-for-local-demo-only"
COMPOSE="docker compose -f infra/docker-compose.yml --env-file $ENV_FILE"
LOG_DIR="logs"

if [ ! -f "$ENV_EXAMPLE" ]; then
  echo "ERROR: $ENV_EXAMPLE is missing — wrong working directory?" >&2
  exit 1
fi

if [ ! -f "$ENV_FILE" ]; then
  echo "Creating $ENV_FILE from $ENV_EXAMPLE..."
  cp "$ENV_EXAMPLE" "$ENV_FILE"
fi

if grep -q "^SUPERSET_SECRET_KEY=${PLACEHOLDER_SECRET}\$" "$ENV_FILE"; then
  if ! command -v openssl >/dev/null 2>&1; then
    echo "ERROR: openssl not found; set SUPERSET_SECRET_KEY in $ENV_FILE manually." >&2
    exit 1
  fi
  echo "Rotating SUPERSET_SECRET_KEY in $ENV_FILE (placeholder detected)..."
  NEW_SECRET="$(openssl rand -hex 32)"
  sed "s|^SUPERSET_SECRET_KEY=.*|SUPERSET_SECRET_KEY=${NEW_SECRET}|" "$ENV_FILE" > "${ENV_FILE}.tmp"
  mv "${ENV_FILE}.tmp" "$ENV_FILE"
fi

mkdir -p "$LOG_DIR"

# Compose остаётся в foreground (как обычный `docker compose up`).
# Параллельно в фоне per-service streamers пишут чистые логи (без префиксов)
# в logs/<service>.log. Streamers стартуют после того, как compose создал
# первый контейнер — раньше `docker compose logs -f` упадёт с "no containers".
$COMPOSE up --build "$@" &
COMPOSE_PID=$!

LOG_PIDS=""

cleanup() {
  for pid in $LOG_PIDS; do kill "$pid" 2>/dev/null || true; done
}
trap cleanup EXIT INT TERM

while [ -z "$($COMPOSE ps -q 2>/dev/null)" ]; do
  if ! kill -0 "$COMPOSE_PID" 2>/dev/null; then
    wait "$COMPOSE_PID"
    exit $?
  fi
  sleep 1
done

for svc in $($COMPOSE config --services); do
  # --no-color снимает только префиксы compose; ANSI от самого приложения
  # (например Garage/tracing) проходит насквозь — режем CSI-последовательности.
  $COMPOSE logs --no-color --no-log-prefix -f "$svc" 2>&1 \
    | perl -pe 's/\e\[[0-9;]*[a-zA-Z]//g' > "$LOG_DIR/${svc}.log" &
  LOG_PIDS="$LOG_PIDS $!"
done

echo ""
echo "Per-service logs duplicated to ${LOG_DIR}/<service>.log"
echo ""

wait "$COMPOSE_PID"
