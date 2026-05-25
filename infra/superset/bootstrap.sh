#!/bin/sh
set -e

PLACEHOLDER_SECRET="change-me-for-local-demo-only"
if [ -z "${SUPERSET_SECRET_KEY:-}" ] || [ "${SUPERSET_SECRET_KEY}" = "${PLACEHOLDER_SECRET}" ]; then
  echo "ERROR: SUPERSET_SECRET_KEY is unset or still the placeholder '${PLACEHOLDER_SECRET}'." >&2
  echo "       Set it in .env (e.g. SUPERSET_SECRET_KEY=\$(openssl rand -hex 32)) and restart." >&2
  exit 1
fi

superset db upgrade
superset fab create-admin \
  --username "${SUPERSET_ADMIN_USER:-admin}" \
  --firstname Demo \
  --lastname Admin \
  --email admin@example.com \
  --password "${SUPERSET_ADMIN_PASSWORD:-admin}" || true
superset init

# Используем нативный starrocks:// dialect (Superset StarRocksEngineSpec), а не
# mysql+pymysql:// — последний при обходе схем переписывает URI в database=<schema>
# и handshake падает с Unknown database (Iceberg-схемы живут в demo_lake catalog,
# не в default_catalog). starrocks-спек умеет catalog.schema и корректно собирает
# database при switch.
STARROCKS_DB_NAME="${STARROCKS_DB_NAME:-StarRocks Gold}"
STARROCKS_URI="${STARROCKS_SQLALCHEMY_URI:-starrocks+pymysql://root@starrocks-fe:9030/demo_lake.gold}"
echo "Registering Superset database '${STARROCKS_DB_NAME}' -> ${STARROCKS_URI}"
# set-database-uri идемпотентен (UPSERT по database_name) и не открывает соединение,
# поэтому flag «skip test connection» не нужен — раньше использовался --skip_test_conn,
# но в актуальной версии такого флага нет и команда падала молча из-за `|| echo`.
superset set-database-uri \
  --database_name "${STARROCKS_DB_NAME}" \
  --uri "${STARROCKS_URI}"

exec gunicorn --bind 0.0.0.0:8088 --workers 2 --timeout 120 'superset.app:create_app()'
