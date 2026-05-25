# Spark Jobs

Spark/PySpark scripts для Iceberg ingestion и построения витрин.

## Назначение

- `init_tables.py` создает namespaces и таблицы Bronze/Silver/Gold (включая DLQ-таблицы `bronze.dlq_pg_cdc` и `bronze.dlq_clickstream`).
- `ingest_pg_cdc.py` читает Debezium topics и пишет `bronze.pg_cdc_raw`; невалидные записи (null payload или отсутствует `op`) идут в `bronze.dlq_pg_cdc` с полями `raw_value`/`error`/`ingest_ts`.
- `ingest_clickstream.py` читает clickstream topic и пишет `bronze.clickstream_raw`; невалидные записи (null payload или отсутствует `event_id`) идут в `bronze.dlq_clickstream`.
- `build_marts.py` пересобирает Silver и Gold tables.

## Требования

- Используй `common.get_spark()` для конфигурации Nessie Iceberg REST catalog и Garage (S3). Эндпоинт и creds приходят из env `S3_ENDPOINT` / `S3_ACCESS_KEY` / `S3_SECRET_KEY`; дефолт endpoint — `http://s3:3900`.
- Bronze ingestion использует structured streaming с `trigger(availableNow=True)` и `checkpointLocation=s3a://warehouse/_checkpoints/bronze/<table>` — offset tracking берёт на себя Spark.
- При сбросе/удалении checkpoint директории в Garage будет повторное чтение с earliest и появятся дубликаты в Bronze; для очистки чекпойнтов держи их под `s3a://warehouse/_checkpoints/`.
- Не меняй имена namespaces `bronze`, `silver`, `gold` без обновления StarRocks/Superset и документации.
- В Gold оставляй витрины, понятные для демонстрации: balances, daily volume, engagement, clicks, conversion.
- Для денежных сумм используй `DECIMAL(18, 2)`.
- `silver.transactions` агрегирует Debezium op `c`/`r`/`u` (последняя версия по `event_ts`); удаления (`d`) в OLTP по-прежнему не учитываются.
- `gold.current_balances` считает `opening_balance + Σ(credit − debit)` поверх `silver.transactions`. При удалениях транзакций в OLTP витрина разъедется с `accounts.current_balance` — для демо это допустимо.
- **Авторитетность баланса**: для BI / Superset / StarRocks использовать `gold.current_balances.balance` — он считается из лога транзакций и не зависит от того, что CDC именно сейчас догнал по `accounts`. `silver.accounts.current_balance` — справочный снимок последнего CDC-апдейта `accounts`; он может временно расходиться с `gold.current_balances.balance` в окне между приходом CDC по `accounts` и приходом CDC по соответствующих `transactions` (или между прогонами DAG'а, который пересобирает Gold). В демо это редкие миллисекунды/минуты, но в чарте «Balance over time» лучше всегда брать `gold.current_balances`.
- **Ограничение `gold.conversion_funnel_daily.conversion_rate`**: формула `transactions_created / page_views` соединяет два источника с разным охватом. `transactions_created` берётся из `silver.transactions` (CDC из БД, учитывает ВСЕ транзакции, в т.ч. созданные напрямую через wallet API без UI — `scripts/smoke.sh`, ручные `curl`, эмулятор без clickstream-сессии). `page_views` берётся только из `silver.clickstream_events` (события `page_enter` из UI). При несбалансированных источниках `conversion_rate` может уйти > 1.0. Это soft macro-метрика «сколько транзакций приходится на одно посещение страницы», **не строгий conversion ratio**. Для честного «конверсия CTA → транзакция» нужно джойнить транзакции с сессиями по `user_id` + временное окно и считать долю сессий с click + последующей транзакцией; в Gold-витринах сейчас не реализовано (демо-ограничение).
- `INSERT OVERWRITE` в `build_marts.py` работает в dynamic-mode (`spark.sql.sources.partitionOverwriteMode=dynamic` в `common.get_spark()`). Это означает: при следующем прогоне затрагиваются только `dt`-партиции, присутствующие в источнике. Если Bronze когда-нибудь начнут чистить по TTL, исторические `dt`-партиции в `silver`/`gold` молча перестанут обновляться (но не обнулятся). Не включать static-mode без явного решения.
- `silver.users` и `silver.accounts` не партиционированы и пишутся **полным `INSERT OVERWRITE`** каждый прогон DAG'а. Это осознанное ограничение демо-стенда: справочники маленькие (двое пользователей seed'ом), full-rewrite на каждом прогоне даёт простой код и atomic snapshot. При росте справочников до десятков тысяч строк это перестанет масштабироваться — тогда стоит перевести их на `MERGE INTO` по `user_id`/`account_id` или партиционирование по `dt` Bronze-источника.

## Проверки

- `python3 -m py_compile etl/spark/*.py`
- После запуска стенда проверяй данные через StarRocks:
  `SELECT * FROM gold.current_balances LIMIT 10;`

