import os

# StarRocksDialect (пакет `starrocks`) и default-driver `starrocks://` под капотом
# наследуют MySQLDialect_mysqldb и пытаются `import MySQLdb`. Пакет `mysqlclient`
# тяжёлый (нужен системный libmysqlclient-dev + build), поэтому регистрируем
# PyMySQL как MySQLdb через sys.modules: всё, что ожидает MySQLdb, получает PyMySQL.
# Это критично для SQL Lab — он где-то во внутренних code-path'ах строит engine
# без `+pymysql` суффикса, и без этого шима валится с "No module named 'MySQLdb'".
import pymysql

pymysql.install_as_MySQLdb()

_PLACEHOLDER_SECRET = "change-me-for-local-demo-only"
SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "")
if not SECRET_KEY or SECRET_KEY == _PLACEHOLDER_SECRET:
    raise RuntimeError(
        "SUPERSET_SECRET_KEY is unset or still the placeholder. "
        "Set it in .env (e.g. SUPERSET_SECRET_KEY=$(openssl rand -hex 32))."
    )

FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
}

