from __future__ import annotations

import os

from pyspark.sql import SparkSession


ICEBERG_VERSION = os.getenv("ICEBERG_VERSION", "1.8.1")
SPARK_VERSION = os.getenv("SPARK_VERSION", "3.5.5")


def get_spark(app_name: str) -> SparkSession:
    packages = ",".join(
        [
            f"org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:{ICEBERG_VERSION}",
            f"org.apache.iceberg:iceberg-aws-bundle:{ICEBERG_VERSION}",
            f"org.apache.spark:spark-sql-kafka-0-10_2.12:{SPARK_VERSION}",
        ]
    )

    s3_endpoint = os.getenv("S3_ENDPOINT", "http://s3:3900")
    s3_access_key = os.getenv("S3_ACCESS_KEY", "admin")
    s3_secret_key = os.getenv("S3_SECRET_KEY", "password")

    return (
        SparkSession.builder.appName(app_name)
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .config("spark.jars.packages", packages)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "rest")
        .config("spark.sql.catalog.lakehouse.uri", os.getenv("ICEBERG_CATALOG_URI", "http://nessie:19120/iceberg"))
        .config("spark.sql.catalog.lakehouse.warehouse", os.getenv("ICEBERG_WAREHOUSE", "warehouse"))
        # HadoopFileIO (а не Iceberg-native S3FileIO) — потому что Garage не
        # поддерживает chunked encoding (https://git.deuxfleurs.fr/Deuxfleurs/garage/issues/1019),
        # а AWS SDK v2, который тащит iceberg-aws-bundle, шлёт все PUT-ы chunked-ом
        # → 400 "Invalid payload signature". HadoopFileIO пишет через hadoop-aws/s3a
        # на SDK v1, у которого chunked encoding не дефолт.
        .config("spark.sql.catalog.lakehouse.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO")
        .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", s3_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", s3_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        # AWS4UnsignedPayloadSignerType шлёт `x-amz-content-sha256: UNSIGNED-PAYLOAD`
        # вместо chunked-stream — то же, что делает aws-cli, и что Garage понимает.
        .config("spark.hadoop.fs.s3a.signing-algorithm", "AWS4UnsignedPayloadSignerType")
        # Garage в ответе DeleteObjects кладёт несуществующие ключи в Errors[]
        # с NoSuchKey (AWS S3 / MinIO в этой ситуации возвращают пустой Errors[]
        # по спеке идемпотентности). hadoop-aws трактует NoSuchKey как retryable
        # и уходит в экспоненциальный бэкофф — каждый rename чекпоинта блокирует
        # стрим на минуты. `keep` отключает удаление directory markers, поэтому
        # лишних DELETE-запросов вообще не возникает. Дефолт в Hadoop ≥3.3.1
        # фактически тоже сместился к keep, для S3-совместимых хранилищ безопасно.
        .config("spark.hadoop.fs.s3a.directory.marker.retention", "keep")
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "4"))
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.session.timeZone", "UTC")
        # Debezium 3.5 в режиме time.precision.mode=connect шлёт timestamp как
        # `2026-05-24T14:19:26.000400Z` (6-digit микросекунды + литеральный Z).
        # Новый (CORRECTED) парсер Spark 3.x на этот формат спотыкается даже
        # при `XXX` zone offset; LEGACY-режим парсит ISO с любым количеством
        # дробных цифр и Z как UTC. Для демо-стенда это устойчивее, чем
        # перечислять все варианты форматов в каждом `timestamp_from_column`.
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .getOrCreate()
    )


def kafka_bootstrap_servers() -> str:
    return os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
