# ETL

Airflow DAGs и Spark jobs, которые наполняют lakehouse и витрины.

## Общие Требования

- Сохраняй трехслойную модель: Bronze raw, Silver normalized, Gold marts.
- CDC остается append-only; не добавляй destructive history rewrite в Bronze.
- Spark jobs должны быть idempotent настолько, насколько это возможно для локального demo.
- Все таблицы Iceberg должны быть доступны через catalog name `lakehouse` внутри Spark jobs.
- Изменения схем должны синхронно обновлять StarRocks/Superset docs и smoke queries.

