FROM apache/superset:6.0.0

USER root
# Superset 6.x использует uv-managed venv в /app/.venv; обычный `pip install`
# кладёт в системный site-packages, который не подхватывается раннером.
# Ставим драйверы напрямую в venv через uv.
#
# starrocks: нативный SQLAlchemy dialect (URI starrocks+pymysql://...), нужен для
# работы Superset StarRocksEngineSpec с external catalogs (demo_lake.<schema>).
# Без него adjust_engine_params MySQL-спека пишет database=<schema> в handshake,
# pymysql шлёт `USE schema`, и StarRocks отвечает Unknown database (схема живёт
# в demo_lake, а handshake идёт в default_catalog).
#
# PyMySQL — DBAPI для starrocks+pymysql://. Без +pymysql суффикса dialect грузит
# MySQLdb (пакет mysqlclient, требует системный libmysqlclient-dev → лишний вес
# и build-стадия).
RUN uv pip install --python /app/.venv/bin/python --no-cache \
    starrocks==1.3.3 \
    PyMySQL==1.1.1

COPY infra/superset/superset_config.py /app/pythonpath/superset_config.py

USER superset
