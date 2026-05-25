# StarRocks SQL

Bootstrap SQL для подключения StarRocks к Iceberg REST catalog, который реализует Nessie.

## Назначение

- Создает external catalog `demo_lake`.
- Настраивает доступ к Nessie (Iceberg REST endpoint) и Garage (S3).

## Требования

- Catalog должен указывать на Compose hostnames `nessie` (Iceberg REST endpoint `http://nessie:19120/iceberg`) и `s3` (Garage S3 API, `http://s3:3900`).
- Сохраняй S3 path-style access — Garage его требует.
- Не добавляй native StarRocks marts без явного изменения роли StarRocks; текущая роль - query Iceberg.
- Если меняешь S3 credentials (`S3_ACCESS_KEY` / `S3_SECRET_KEY`), обнови `.env.example`, Compose и этот SQL.

