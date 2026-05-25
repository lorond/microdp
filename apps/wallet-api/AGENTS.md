# Wallet API

FastAPI-сервис, который является операционной частью кошелька: читает баланс, отдает историю и создает пользовательские транзакции в PostgreSQL.

## Назначение

- PostgreSQL является source of truth для пользователей, счетов и транзакций.
- Баланс для UI читается из таблицы `accounts`.
- Создание транзакции обновляет `accounts.current_balance` и добавляет строку в `transactions` одной SQL-транзакцией.

## Требования

- Сохраняй API:
  - `GET /health`
  - `GET /api/users/{user_id}/balance`
  - `GET /api/users/{user_id}/transactions`
  - `POST /api/users/{user_id}/transactions`
- Денежные значения обрабатывай через `Decimal`, не через `float`.
- Не допускай отрицательный баланс для debit-операций.
- При записи JSONB metadata используй структурированную передачу параметров, не ручную сериализацию SQL-строк.
- Ошибки отсутствующего счета возвращай как `404`, конфликт баланса как `409`.
- При изменении схем Pydantic обновляй unit-тесты и Spark normalization logic.
- CORS открыт (`allow_origins=["*"]`) — это demo-only для локального UI и smoke-curl. Перед выходом за пределы стенда сузить до конкретных origin'ов.

## Проверки

- `docker run --rm -e PYTHONPATH=/app -v "$PWD/apps/wallet-api/tests:/app/tests:ro" microdp-wallet-api pytest -q`
- `python3 -m py_compile apps/wallet-api/app/*.py apps/wallet-api/tests/*.py`

