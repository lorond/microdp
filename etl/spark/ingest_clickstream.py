import os

from pyspark.sql import Column
from pyspark.sql import functions as F

from common import get_spark, kafka_bootstrap_servers


CHECKPOINT_LOCATION = "s3a://warehouse/_checkpoints/bronze/clickstream_raw"
TARGET_TABLE = "lakehouse.bronze.clickstream_raw"
DLQ_TABLE = "lakehouse.bronze.dlq_clickstream"


def parse_iso_timestamp(col: Column) -> Column:
    return F.coalesce(
        F.to_timestamp(col, "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),
        F.to_timestamp(col, "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX"),
        F.to_timestamp(col, "yyyy-MM-dd'T'HH:mm:ssXXX"),
        F.to_timestamp(col, "yyyy-MM-dd'T'HH:mm:ss.SSS"),
        F.to_timestamp(col, "yyyy-MM-dd'T'HH:mm:ss"),
        col.cast("timestamp"),
    )


def _write_batch(batch_df, _batch_id: int) -> None:
    parsed = (
        batch_df.withColumn("event_id", F.get_json_object("value_json", "$.event_id"))
        .withColumn("session_id", F.get_json_object("value_json", "$.session_id"))
        .withColumn("user_id", F.get_json_object("value_json", "$.user_id"))
        .withColumn("event_type", F.get_json_object("value_json", "$.event_type"))
        .withColumn("page", F.get_json_object("value_json", "$.page"))
        .withColumn("element_id", F.get_json_object("value_json", "$.element_id"))
        .withColumn("x", F.get_json_object("value_json", "$.x").cast("int"))
        .withColumn("y", F.get_json_object("value_json", "$.y").cast("int"))
        .withColumn("dwell_ms", F.get_json_object("value_json", "$.dwell_ms").cast("bigint"))
        .withColumn("event_ts", parse_iso_timestamp(F.get_json_object("value_json", "$.ts")))
        .withColumn("ingest_ts", F.current_timestamp())
    )

    is_valid = F.col("value_json").isNotNull() & F.col("event_id").isNotNull()

    valid = (
        parsed.filter(is_valid)
        .withColumn("dt", F.to_date(F.coalesce(F.col("event_ts"), F.col("ingest_ts"))))
        .select(
            "topic", "partition_id", "offset_value", "event_id", "session_id",
            "user_id", "event_type", "page", "element_id", "x", "y", "dwell_ms",
            "value_json", "event_ts", "ingest_ts", "dt",
        )
    )
    valid.writeTo(TARGET_TABLE).append()

    invalid = (
        parsed.filter(~is_valid)
        .withColumn(
            "error",
            F.when(F.col("value_json").isNull(), F.lit("null_value"))
            .otherwise(F.lit("missing_event_id")),
        )
        .withColumn("dt", F.to_date(F.col("ingest_ts")))
        .select(
            "topic", "partition_id", "offset_value",
            F.col("value_json").alias("raw_value"),
            "error", "ingest_ts", "dt",
        )
    )
    invalid.writeTo(DLQ_TABLE).append()


def main() -> None:
    spark = get_spark("microdp-ingest-clickstream")
    topic = os.getenv("CLICKSTREAM_TOPIC", "clickstream.events")

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers())
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
        .select(
            F.col("topic"),
            F.col("partition").alias("partition_id"),
            F.col("offset").alias("offset_value"),
            F.col("value").cast("string").alias("value_json"),
        )
    )

    query = (
        kafka_df.writeStream
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .trigger(availableNow=True)
        .foreachBatch(_write_batch)
        .start()
    )

    try:
        query.awaitTermination()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
