#!/bin/sh
# Генерируем DEMO_USERS_GENERATE_COUNT (default 10) случайных демо-пользователей
# с аккаунтами в USD. UUID'ы и имена — рандомные (имена из пула в SQL ниже,
# emails уникальны за счёт hex-суффикса), поэтому код приложения не должен
# знать заранее ни одного ID, кроме `Demo` (...001) из 001_init.sql. Эмулятор и
# UI читают актуальный список через GET /api/users.
set -e

COUNT="${DEMO_USERS_GENERATE_COUNT:-10}"

psql -v ON_ERROR_STOP=1 \
    --username "$POSTGRES_USER" \
    --dbname "$POSTGRES_DB" \
    --set "count=$COUNT" <<'SQL'
WITH name_pool AS (
    -- ORDER BY random() рандомизирует обход пула; при count <= размера пула
    -- имена в одной seed-итерации не повторятся. При count > 20 имена
    -- начнут повторяться, но email всё равно уникален (hex-суффикс).
    SELECT name, row_number() OVER (ORDER BY random()) AS rn
    FROM (VALUES
        ('Alice'), ('Bob'), ('Carla'), ('David'), ('Eva'),
        ('Frank'), ('Grace'), ('Henry'), ('Iris'), ('Jack'),
        ('Karen'), ('Lily'), ('Mike'), ('Nora'), ('Oliver'),
        ('Paul'), ('Quinn'), ('Rose'), ('Sam'), ('Tina')
    ) AS v(name)
),
inserted AS (
    INSERT INTO users (id, full_name, email)
    SELECT
        gen_random_uuid(),
        np.name,
        lower(np.name) || '-' || encode(gen_random_bytes(4), 'hex') || '@example.com'
    FROM generate_series(1, :count) AS s(n)
    JOIN name_pool np
        ON np.rn = ((s.n - 1) % (SELECT count(*) FROM name_pool)) + 1
    RETURNING id
)
INSERT INTO accounts (user_id, currency, opening_balance, current_balance)
SELECT
    id,
    'USD',
    round((random() * 1500 + 250)::numeric, 2) AS opening_balance,
    round((random() * 1500 + 250)::numeric, 2) AS current_balance
FROM inserted;
SQL

echo "Seeded ${COUNT} demo users (excluding the explicit Demo user from 001_init.sql)."
