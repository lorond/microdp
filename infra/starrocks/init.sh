#!/bin/sh
set -e

echo 'Waiting for StarRocks FE SQL port...'
until mysql -h starrocks-fe -P 9030 -u root -e 'SELECT 1' >/dev/null 2>&1; do
  sleep 2
done

echo 'Registering backend (idempotent)...'
ADD_OUT=$(mysql -h starrocks-fe -P 9030 -u root -e "ALTER SYSTEM ADD BACKEND 'starrocks-be:9050';" 2>&1 || true)
if echo "$ADD_OUT" | grep -q 'already exists'; then
  echo 'backend already registered'
elif [ -n "$ADD_OUT" ]; then
  echo "$ADD_OUT" >&2
  exit 1
fi

echo 'Waiting for BE to register and become Alive...'
for i in $(seq 1 60); do
  mysql -h starrocks-fe -P 9030 -u root -e 'SHOW BACKENDS' 2>/dev/null | grep -q 'true' && break
  sleep 2
done
mysql -h starrocks-fe -P 9030 -u root -e 'SHOW BACKENDS' | grep -q 'true' || {
  echo 'BE not alive after 120s' >&2
  exit 1
}

echo 'Resetting demo_lake catalog (чтобы подхватились свежие S3 creds из .env)...'
mysql -h starrocks-fe -P 9030 -u root -e 'DROP CATALOG IF EXISTS demo_lake;'

echo 'Ensuring admin user with full privileges (idempotent)...'
# StarRocks 4.x: user без @host (в отличие от MySQL), role-grant без слова USER.
# CREATE/ALTER оба нужны: CREATE IF NOT EXISTS не меняет пароль существующему user.
mysql -h starrocks-fe -P 9030 -u root <<'SQL'
CREATE USER IF NOT EXISTS 'admin' IDENTIFIED BY 'admin';
ALTER USER 'admin' IDENTIFIED BY 'admin';
GRANT root TO 'admin';
SET DEFAULT ROLE root TO 'admin';
SQL

echo 'Initializing Iceberg catalog...'
# Подстановка через awk ENVIRON[] (а не sed | ...|) — значения попадают в скрипт
# как литералы, не как regex, поэтому любые спецсимволы в S3_SECRET_KEY
# (включая |, /, \, $, .) безопасны. Двойное экранирование & (\\\\& в awk-литерале
# даёт \\& для gsub, что превращается в литеральный & при подстановке) нужно
# потому, что в replacement-строке gsub «&» означает «найденный текст».
awk '
BEGIN {
  u = ENVIRON["S3_ACCESS_KEY"]
  p = ENVIRON["S3_SECRET_KEY"]
  gsub(/&/, "\\\\&", u)
  gsub(/&/, "\\\\&", p)
}
{
  gsub(/\$\{S3_ACCESS_KEY\}/, u)
  gsub(/\$\{S3_SECRET_KEY\}/, p)
  print
}
' /sql/init_iceberg_catalog.sql | mysql -h starrocks-fe -P 9030 -u root
