from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, MapType
from datetime import datetime
from uuid import uuid4
import json
import os
from query_wrappers import read_postgres_query
from upsert_functions import upsert_transaction_ledger, upsert_transaction_balance, upsert_late_arriving_events, upsert_duplicate_events, upsert_run_metrics, upsert_kafka_offsets
from transforms import build_duplicate_summary, build_late_arriving_events, deduplicate_transactions, derive_run_metrics, flatten_df, get_df_with_offsets, get_winning_rows, parse_kafka, add_signed_amount, get_subledger_fields, add_lag


run_id = os.environ.get("RUN_ID") or str(uuid4()).replace("-", "_")
print(f"RUN ID: {run_id}")

# Define JDBC connection details for postgres
jdbc_url = "jdbc:postgresql://postgres:5432/fund_balance"
jdbc_props = {
    "user": "funduser",
    "password": "fundpass",
    "driver": "org.postgresql.Driver"
}

KAFKA_TOPIC = "transactions"

# Function to get the starting offset for the current batch based on postgres log for last visited offset


def get_starting_offsets_json(spark, jdbc_url, jdbc_props, topic: str) -> str:
    sql_query = """
        SELECT
            topic,
            partition,
            COALESCE(MAX(end_offset), -2) as last_processed_offset
        FROM 
            kafka_offsets
        WHERE
            status = 'SUCCESS'
        GROUP BY
            topic,
            partition
    """

    rows = read_postgres_query(
        spark, sql_query, jdbc_url, jdbc_props).collect()
    if not rows:
        # First run: from the beginning (partition 0 only; extend if you add partitions)
        return json.dumps({topic: {"0": 0}})
    # One entry per partition: { "0": 5, "1": 12, ... }
    partition_offsets = {
        str(row["partition"]): int(row["last_processed_offset"])
        for row in rows
        if row["topic"] == topic
    }
    if not partition_offsets:
        return json.dumps({topic: {"0": 0}})
    return json.dumps({topic: partition_offsets})


# Build Spark Session
spark = SparkSession.builder \
    .appName("KafkaStreaming") \
    .config("spark.ui.showConsoleProgress", "false")\
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")  # or "ERROR"

# Define schema of transaction event to be read from Kafka stream
schema = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("transaction_timestamp", StringType(), False),
    StructField("event_timestamp", StringType(), False),
    StructField("transaction_amount", DoubleType(), False),
    StructField("currency", StringType(), False),
    StructField("transaction_type", StringType(), False),
    StructField("status", StringType(), False),
    StructField("fund", StructType([
        StructField("fund_id", StringType(), False),
        StructField("fund_name", StringType(), False),
    ]), False),
    StructField("deal", StructType([
        StructField("deal_id", StringType(), False),
        StructField("deal_name", StringType(), False),
    ]), False),
    StructField("metadata", MapType(StringType(), StringType()), True),
])

# Determine run mode
run_mode = os.environ.get("RUN_MODE", "incremental")
print(f"run mode: {run_mode}")

# Get starting and ending offsets based on run mode
if run_mode == "incremental":
    starting_offsets = get_starting_offsets_json(
        spark, jdbc_url, jdbc_props, KAFKA_TOPIC)
    print(
        f"running in incremental mode with starting offset: {starting_offsets}")
    reader = (spark.read.format("kafka")
              .option("kafka.bootstrap.servers", "kafka:29092")
              .option("subscribe", "transactions")
              .option("startingOffsets", starting_offsets)
              )

elif run_mode == "backfill":
    start = int(os.environ["STARTING_OFFSET"])
    end = int(os.environ["ENDING_OFFSET"])
    starting_offsets = json.dumps({KAFKA_TOPIC: {"0": start}})
    ending_offsets = json.dumps({KAFKA_TOPIC: {"0": end}})
    print(
        f"running in backfill mode with starting offset: {starting_offsets} and ending offset: {ending_offsets}")
    reader = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", "kafka:29092")
        .option("subscribe", "transactions")
        .option("startingOffsets", starting_offsets)
        .option("endingOffsets", ending_offsets)
    )
else:
    raise ValueError(f"Unknown RUN_MODE: {run_mode}")

# Load event dataframe
raw_df = reader.load()

# Create base DF for event
events_df = raw_df.selectExpr(
    "CAST(value AS STRING) as value",
    "topic",
    "partition",
    "offset",
    "timestamp as kafka_timestamp",
    "timestampType"
)

# Parse event and add new column to raw_df for each event field
parsed_df = parse_kafka(events_df, schema)

# Flatten fund and deal object
# separate fund id and deal id into separate columns -> important for aggregations
# handle missing event/transaction timestamps to account for schema evolution
# convert timestamp fields from string to timestamp
flat_df = flatten_df(parsed_df)


# Detect late arriving events -> late arriving means that a transaction timestamp is more than 15 minutes before the time it was inserted into the kafka stream
df_with_lag = add_lag(flat_df)

# Late arriving events are older than 15 minutes
# Select and alias columns from late arriving df to prepare for upsert
late_arriving_df = build_late_arriving_events(
    df_with_lag, run_id, lag_threshold_sec=900)

# Idempotent upsert of late arriving events into log
upsert_late_arriving_events(late_arriving_df, jdbc_url, jdbc_props)


# Deduplicate: keep the most recent transaction. Use kafka_timestamp and offset to break ties.
deduped_df = deduplicate_transactions(df_with_lag)

# Winner row per transaction_id (single source of truth)
winners_df = get_winning_rows(deduped_df)


# Aggregate duplicate diagnostics
duplicate_summary = build_duplicate_summary(df_with_lag, winners_df, run_id)


# Idempotent upsert of dupulicate events into log
upsert_duplicate_events(duplicate_summary, jdbc_url, jdbc_props)

# Calculate signed amounts based on transaction type
signed_df = add_signed_amount(deduped_df)

# Select subset of columns to be used for transaction subledger
ledger_df = get_subledger_fields(signed_df)


# Upsert into transaction ledger table
upsert_transaction_ledger(ledger_df, jdbc_url, jdbc_props)

# Idempotent upsert of transaction ledger into stage table
upsert_transaction_balance(jdbc_url, jdbc_props)

# Derive run metrics
run_ts = datetime.now()
duplicate_count = duplicate_summary.count()

run_metrics_df = derive_run_metrics(
    df_with_lag, run_id, run_ts, duplicate_count)


# Idempotent upsert into run metrics table
upsert_run_metrics(run_metrics_df, jdbc_url, jdbc_props)

# Get new df with current offsets from kafka stream
offsets_df = get_df_with_offsets(ledger_df, run_id, run_ts)

if run_mode == "incremental":
    print("Updating kafka offsets")
    upsert_kafka_offsets(offsets_df, jdbc_url, jdbc_props)
else:
    print("Backfill mode running - skipping Kafka offset update")
