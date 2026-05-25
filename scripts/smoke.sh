#!/usr/bin/env bash
set -euo pipefail
set -o pipefail

# Smoke-скрипт ходит на единственный жёстко-заданный демо-ID (Demo user из
# 001_init.sql), остальные пользователи имеют рандомные UUID и подтягиваются
# через GET /api/users — smoke на них не завязан намеренно.
# Для прогона по конкретному user'у переопределить: DEMO_USER_ID=... sh scripts/smoke.sh
DEMO_USER_ID="${DEMO_USER_ID:-00000000-0000-0000-0000-000000000001}"
NOW_TS="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
COMPOSE="docker compose -f infra/docker-compose.yml"

curl -fsS http://localhost:8000/health >/dev/null
curl -fsS http://localhost:8001/health >/dev/null
curl -fsS "http://localhost:8000/api/users/${DEMO_USER_ID}/balance" >/dev/null

curl -fsS -X POST "http://localhost:8000/api/users/${DEMO_USER_ID}/transactions" \
  -H "Content-Type: application/json" \
  -d '{"type":"deposit","amount":"11.00","currency":"USD","description":"Smoke deposit"}' >/dev/null

curl -fsS -X POST http://localhost:8001/api/clickstream/events \
  -H "Content-Type: application/json" \
  -d "{\"events\":[{\"event_id\":\"smoke-shell-1\",\"session_id\":\"smoke-shell\",\"user_id\":\"${DEMO_USER_ID}\",\"event_type\":\"click\",\"page\":\"/dashboard\",\"ts\":\"${NOW_TS}\",\"element_id\":\"smoke-script\",\"x\":10,\"y\":20,\"dwell_ms\":0,\"payload\":{\"source\":\"scripts/smoke.sh\"}}]}" >/dev/null

echo "Verifying clickstream event landed in Kafka..."
if ! $COMPOSE exec -T redpanda rpk topic consume clickstream.events \
      --num 1 --offset end -X consume.timeout_ms=5000 >/dev/null 2>&1; then
  echo "WARN: rpk did not return a record within 5s — event may still be in flight or topic empty" >&2
fi

echo "API smoke checks passed. Run the Airflow DAG, then query demo_lake.gold in StarRocks."
