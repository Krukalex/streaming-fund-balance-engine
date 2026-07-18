from pyspark.sql import DataFrame
from pyspark.sql.functions import from_json, col, when,  coalesce, unix_timestamp, lit, to_timestamp, sha2, concat_ws, desc, row_number, count, min, max
from pyspark.sql.window import Window


def parse_kafka(df: DataFrame, schema) -> DataFrame:
    return df.withColumn(
        "json", from_json(col("value"), schema)).select(
            "topic",
            "partition",
            "offset",
            "kafka_timestamp",
            "timestampType",
            "json.*"
    )


def flatten_df(df: DataFrame) -> DataFrame:
    return df.select(
        "transaction_id",
        to_timestamp(coalesce(col("event_timestamp"), lit(
            "1900-01-01T00:00:00"))).alias("event_timestamp"),
        to_timestamp(coalesce(col("transaction_timestamp"), lit(
            "1900-01-01T00:00:00"))).alias("transaction_timestamp"),
        "transaction_amount",
        "currency",
        "transaction_type",
        "status",
        col("fund.fund_id").alias("fund_id"),
        col("fund.fund_name").alias("fund_name"),
        col("deal.deal_id").alias("deal_id"),
        col("deal.deal_name").alias("deal_name"),
        "metadata",
        "kafka_timestamp",
        "timestampType",
        "offset",
        "topic",
        "partition"
    )


def add_lag(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "txn_age_sec",
        unix_timestamp(col("kafka_timestamp")) -
        unix_timestamp(col("transaction_timestamp"))
    )


def build_late_arriving_events(
    df: DataFrame,
    run_id: str,
    lag_threshold_sec: int = 900,
) -> DataFrame:
    return (
        df.filter(col("txn_age_sec") > lag_threshold_sec)
        .withColumn(
            "surrogate_pk",
            sha2(
                concat_ws("|", lit("LATE"), col("topic"),
                          col("partition"), col("offset")),
                256,
            ),
        )
        .withColumn("run_id", lit(run_id))
        .select(
            "surrogate_pk",
            "run_id",
            "transaction_id",
            "txn_age_sec",
            "transaction_timestamp",
            "event_timestamp",
            "transaction_amount",
            "currency",
            "transaction_type",
            "status",
            "fund_id",
            "fund_name",
            "deal_id",
            "deal_name",
            "kafka_timestamp",
            col("partition").alias("kafka_partition"),
            col("offset").alias("kafka_offset"),
            col("topic").alias("kafka_topic"),
        )
    )


def deduplicate_transactions(df: DataFrame) -> DataFrame:
    winner_window = Window.partitionBy("transaction_id").orderBy(
        desc("transaction_timestamp"),
        desc("kafka_timestamp"),
        desc("offset"),
    )
    return (
        df.withColumn("rn", row_number().over(winner_window))
        .filter(col("rn") == 1)
    )


def get_winning_rows(df: DataFrame) -> DataFrame:
    return df.select(
        "transaction_id",
        col("transaction_timestamp").alias("winner_transaction_timestamp"),
        col("kafka_timestamp").alias("winner_kafka_timestamp"),
        col("partition").alias("winner_partition"),
        col("offset").alias("winner_offset"),
        col("topic").alias("winner_topic")
    )


def build_duplicate_summary(df: DataFrame, winners_df: DataFrame, run_id) -> DataFrame:
    return (
        df.groupBy("transaction_id")
        .agg(
            count("*").alias("duplicate_count"),
            min("transaction_timestamp").alias("earliest_timestamp"),
            max("transaction_timestamp").alias("latest_timestamp"),
        )
        .join(winners_df, on="transaction_id", how="left")
        .filter(col("duplicate_count") > 1)
        .withColumn(
            "surrogate_pk",
            sha2(concat_ws("|", lit("DUP"), lit(
                run_id), col("transaction_id")), 256),
        )
        .withColumn("run_id", lit(run_id))
        .select(
            "surrogate_pk",
            "run_id",
            "transaction_id",
            "duplicate_count",
            col("earliest_timestamp").alias("earliest_transaction_timestamp"),
            col("latest_timestamp").alias("latest_transaction_timestamp"),
            "winner_transaction_timestamp",
            "winner_kafka_timestamp",
            "winner_partition",
            "winner_offset",
            "winner_topic",
        )
    )


def add_signed_amount(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "signed_amount",
        when(col("transaction_type") == "CREDIT", col("transaction_amount"))
        .when(col("transaction_type") == "DEBIT", -col("transaction_amount"))
        .otherwise(0),
    )


def get_subledger_fields(df: DataFrame) -> DataFrame:
    return df.select(
        "transaction_id",
        "transaction_timestamp",
        "signed_amount",
        "fund_id",
        "deal_id",
        "kafka_timestamp",
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("topic").alias("kafka_topic")
    )


def derive_run_metrics(df: DataFrame, run_id, run_ts, duplicate_count) -> DataFrame:
    return (
        df.agg(
            count(lit(1)).alias("record_count"),
            count(when(col("txn_age_sec") > 900, 1)).alias(
                "late_arrival_count")
        )
    ) \
        .withColumn("run_id", lit(run_id)) \
        .withColumn("duplicate_count", lit(duplicate_count)) \
        .withColumn("run_timestamp", lit(run_ts))  \
        .select(
            "run_id",
            "record_count",
            "duplicate_count",
            "late_arrival_count",
            "run_timestamp"
    )


def get_df_with_offsets(df: DataFrame, run_id, run_ts) -> DataFrame:
    return (
        df
        .groupBy("kafka_topic", "kafka_partition")
        .agg(
            lit(run_id).alias("run_id"),
            min(col("kafka_offset")).alias("start_offset"),
            (max(col("kafka_offset")) + 1).alias("end_offset"),
            lit(run_ts).alias("run_timestamp"),
        )
        .withColumnRenamed("kafka_topic", "topic")
        .withColumnRenamed("kafka_partition", "partition")
        .withColumn("status", lit("SUCCESS"))
        .select(
            "run_id",
            "topic",
            "partition",
            "start_offset",
            "end_offset",
            "status",
            "run_timestamp",
        )
    )
