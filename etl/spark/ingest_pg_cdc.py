from pyspark.sql import functions as F

from common import get_spark, kafka_bootstrap_servers


CHECKPOINT_LOCATION = "s3a://warehouse/_checkpoints/bronze/pg_cdc_raw"
TARGET_TABLE = "lakehouse.bronze.pg_cdc_raw"
DLQ_TABLE = "lakehouse.bronze.dlq_pg_cdc"


def _write_batch(batch_df, _batch_id: int) -> None:
    parsed = (
        batch_df.withColumn("op", F.get_json_object("value_json", "$.op"))
        .withColumn("source_table", F.get_json_object("value_json", "$.source.table"))
        .withColumn(
            "event_ts",
            F.timestamp_millis(F.get_json_object("value_json", "$.ts_ms").cast("long")),
        )
        .withColumn("ingest_ts", F.current_timestamp())
    )

    is_valid = F.col("value_json").isNotNull() & F.col("op").isNotNull()

    valid = (
        parsed.filter(is_valid)
        .withColumn("dt", F.to_date(F.coalesce(F.col("event_ts"), F.col("ingest_ts"))))
        .select(
            "topic", "partition_id", "offset_value", "key_json", "value_json",
            "op", "source_table", "event_ts", "ingest_ts", "dt",
        )
    )
    valid.writeTo(TARGET_TABLE).append()

    invalid = (
        parsed.filter(~is_valid)
        .withColumn(
            "error",
            F.when(F.col("value_json").isNull(), F.lit("null_value"))
            .otherwise(F.lit("missing_debezium_op")),
        )
        .withColumn("dt", F.to_date(F.col("ingest_ts")))
        .select(
            "topic", "partition_id", "offset_value", "key_json",
            F.col("value_json").alias("raw_value"),
            "error", "ingest_ts", "dt",
        )
    )
    invalid.writeTo(DLQ_TABLE).append()


def main() -> None:
    spark = get_spark("microdp-ingest-pg-cdc")

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers())
        .option("subscribePattern", "wallet\\.public\\.(users|accounts|transactions)")
        .option("startingOffsets", "earliest")
        .load()
        .select(
            F.col("topic"),
            F.col("partition").alias("partition_id"),
            F.col("offset").alias("offset_value"),
            F.col("key").cast("string").alias("key_json"),
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
