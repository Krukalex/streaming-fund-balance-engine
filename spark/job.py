from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, row_number, desc, when, sum as spark_sum, count, max, min, coalesce, unix_timestamp, lit, to_timestamp, current_timestamp, sha2, concat_ws
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType, MapType
from pyspark.sql.window import Window
from datetime import datetime
from pyspark import SparkContext
from uuid import uuid4


run_id = str(uuid4()).replace("-", "_")
print(f"RUN ID: {run_id}")

# Define JDBC connection details for postgres
jdbc_url = "jdbc:postgresql://postgres:5432/fund_balance"
jdbc_props = {
    "user": "funduser",
    "password": "fundpass",
    "driver": "org.postgresql.Driver"
}



# Reusable function to execute sql statement
def execute_sql(sql_text, jdbc_url, jdbc_props):
    gateway = SparkContext._gateway
    java_import = gateway.jvm.java.lang.Class
    driver_manager = gateway.jvm.java.sql.DriverManager
    conn = driver_manager.getConnection(jdbc_url, jdbc_props["user"], jdbc_props["password"])
    try:
        stmt = conn.createStatement()
        stmt.executeUpdate(sql_text)
        stmt.close()
    finally:
        conn.close()

# Upsert transaction balance into postgres
def upsert_transaction_ledger(df, jdbc_url, jdbc_props):
    sql_text = """
        INSERT INTO
            transaction_ledger (transaction_id, transaction_timestamp, transaction_amount, fund_id, deal_id, kafka_timestamp, kafka_partition, kafka_offset, kafka_topic, last_modified)
            select
                transaction_id,
                transaction_timestamp,
                signed_amount,
                fund_id,
                deal_id,
                kafka_timestamp,
                kafka_partition,
                kafka_offset,
                kafka_topic,
                now()
            from transaction_ledger_staging
        ON CONFLICT (transaction_id) DO NOTHING
    """
    try:
        df.write.jdbc(url=jdbc_url, table="transaction_ledger_staging", mode="overwrite", properties=jdbc_props)
        execute_sql(sql_text, jdbc_url, jdbc_props)
    except Exception:
        raise
    finally:
        execute_sql("DROP TABLE IF EXISTS transaction_ledger_staging", jdbc_url, jdbc_props)

def upsert_transaction_balance(jdbc_url, jdbc_props):
    # Upsert into canonical table
    sql_text = """
        INSERT 
            INTO transaction_balance (fund_id, deal_id, balance, last_modified)
            SELECT 
                fund_id, 
                deal_id, 
                SUM(transaction_amount), 
                NOW()
            FROM transaction_ledger
        GROUP BY 
            fund_id, 
            deal_id
        ON CONFLICT (fund_id, deal_id)
        DO UPDATE SET
            balance = EXCLUDED.balance,
            last_modified = NOW()
    """

    # Upsert into canonical table
    execute_sql(sql_text, jdbc_url, jdbc_props)


# Upsert late arriving event log into postgres
def upsert_late_arriving_events(df, jdbc_url, jdbc_props):
    sql_text = """
        INSERT INTO
            late_arriving_event_log (surrogate_pk, run_id, transaction_id, txn_age_sec, transaction_timestamp, event_timestamp, transaction_amount, currency, transaction_type, status, fund_id, fund_name, deal_id, deal_name, kafka_timestamp, kafka_partition, kafka_offset, kafka_topic)
            select
                surrogate_pk,
                run_id,
                transaction_id,
                txn_age_sec,
                transaction_timestamp,
                event_timestamp,
                transaction_amount,
                currency,
                transaction_type,
                status,
                fund_id,
                fund_name,
                deal_id,
                deal_name,
                kafka_timestamp,
                kafka_partition,
                kafka_offset,
                kafka_topic
            from late_arriving_event_staging
        ON CONFLICT (surrogate_pk)
        DO NOTHING
    """
    try:
        df.write.jdbc(url=jdbc_url, table="late_arriving_event_staging", mode="overwrite", properties=jdbc_props)
        execute_sql(sql_text, jdbc_url, jdbc_props)
    except Exception:
        raise
    finally:
        execute_sql("DROP TABLE IF EXISTS late_arriving_event_staging", jdbc_url, jdbc_props)


# Upsert duplicate event log into postgres
def upsert_duplicate_events(df, jdbc_url, jdbc_props):
    sql_text = """
        INSERT INTO
            duplicate_records_log (surrogate_pk, run_id, transaction_id, duplicate_count, earliest_transaction_timestamp, latest_transaction_timestamp, winner_transaction_timestamp, winner_kafka_timestamp, winner_partition, winner_offset, winner_topic)
            select
                surrogate_pk, 
                run_id, 
                transaction_id, 
                duplicate_count, 
                earliest_transaction_timestamp, 
                latest_transaction_timestamp, 
                winner_transaction_timestamp, 
                winner_kafka_timestamp, 
                winner_partition, 
                winner_offset, 
                winner_topic
            from duplicate_records_staging
        ON CONFLICT (surrogate_pk)
        DO NOTHING
    """
    try:
        df.write.jdbc(url=jdbc_url, table="duplicate_records_staging", mode="overwrite", properties=jdbc_props)
        execute_sql(sql_text, jdbc_url, jdbc_props)
    except Exception:
        raise
    finally:
        execute_sql("DROP TABLE IF EXISTS duplicate_records_staging", jdbc_url, jdbc_props)

# Upsert run metrics into postgres
def upsert_run_metrics(df, jdbc_url, jdbc_props):
    sql_text = """
        INSERT INTO
            run_metrics (run_id, record_count, duplicate_count, late_arrival_count, run_timestamp)
            select
                run_id,
                record_count,
                duplicate_count,
                late_arrival_count,
                run_timestamp
            from run_metrics_staging
        ON CONFLICT (run_id)
        DO UPDATE SET
            record_count = EXCLUDED.record_count,
            duplicate_count = EXCLUDED.duplicate_count,
            late_arrival_count = EXCLUDED.late_arrival_count,
            run_timestamp = EXCLUDED.run_timestamp
    """
    try:
        df.write.jdbc(url=jdbc_url, table="run_metrics_staging", mode="overwrite", properties=jdbc_props)
        execute_sql(sql_text, jdbc_url, jdbc_props)
    except Exception:
        raise
    finally:
        execute_sql("DROP TABLE IF EXISTS run_metrics_staging", jdbc_url, jdbc_props)

# Build Spark Session
spark = SparkSession.builder \
    .appName("KafkaStreaming") \
    .getOrCreate()

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

# Read from Kafka stream
raw_df = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "transactions") \
    .load()

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
parsed_df = events_df.withColumn(
    "json", from_json(col("value"), schema)).select(
        "topic",
        "partition",
        "offset",
        "kafka_timestamp",
        "timestampType",
        "json.*"
        )

# Flatten fund and deal object
# separate fund id and deal id into separate columns -> important for aggregations
# handle missing event/transaction timestamps to account for schema evolution
# convert timestamp fields from string to timestamp
flat_df = parsed_df.select(
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

# Detect late arriving events -> late arriving means that a transaction timestamp is more than 15 minutes before the time it was inserted into the kafka stream
df_with_lag = flat_df.withColumn(
    "txn_age_sec",
    unix_timestamp(col("kafka_timestamp")) - unix_timestamp(col("transaction_timestamp"))
)

# Late arriving events are older than 15 minutes
late_arriving_df = df_with_lag.filter(col("txn_age_sec") > 900)

# Select and alias columns from late arriving df to prepare for upsert
late_arriving_df = late_arriving_df \
    .withColumn(
        "surrogate_pk",
        sha2(concat_ws("|", lit("LATE"), col("topic"), col("partition"), col("offset")), 256),
        ) \
    .withColumn("run_id", lit(run_id)) \
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
        col("topic").alias("kafka_topic")        
    )

# Idempotent upsert of late arriving events into log
upsert_late_arriving_events(late_arriving_df, jdbc_url, jdbc_props)


# Deduplicate: keep the most recent transaction. Use kafka_timestamp and offset to break ties.
winner_window = Window.partitionBy("transaction_id").orderBy(
    desc("transaction_timestamp"),
    desc("kafka_timestamp"),
    desc("offset")
)

# Rank rows per transaction_id
ranked_df = df_with_lag.withColumn("rn", row_number().over(winner_window))

# Create a deduplicted df with only the winning rows for further processing
deduped_df = ranked_df.filter(col("rn")==1)

# Winner row per transaction_id (single source of truth)
winners_df = deduped_df.select(
    "transaction_id",
    col("transaction_timestamp").alias("winner_transaction_timestamp"),
    col("kafka_timestamp").alias("winner_kafka_timestamp"),
    col("partition").alias("winner_partition"),
    col("offset").alias("winner_offset"),
    col("topic").alias("winner_topic")
)
# Aggregate duplicate diagnostics
duplicate_stats_df = df_with_lag.groupBy("transaction_id").agg(
    count("*").alias("duplicate_count"),
    min("transaction_timestamp").alias("earliest_timestamp"),
    max("transaction_timestamp").alias("latest_timestamp")
)
# Final duplicate summary (only ids with actual duplicates)
duplicate_summary = (
    duplicate_stats_df
    .join(winners_df, on="transaction_id", how="left")
    .filter(col("duplicate_count") > 1)
)

# Select and alias columns from duplicate summary df to prepare for upsert
duplicate_summary = duplicate_summary \
    .withColumn(
        "surrogate_pk",
        sha2(concat_ws("|", lit("DUP"), lit(run_id), col("transaction_id")), 256),
        ) \
    .withColumn("run_id", lit(run_id)) \
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

# Idempotent upsert of dupulicate events into log
upsert_duplicate_events(duplicate_summary, jdbc_url, jdbc_props)

# Calculate signed amounts based on transaction type
signed_df = deduped_df.withColumn(
    "signed_amount",
    when(col("transaction_type") == "CREDIT", col("transaction_amount"))
    .when(col("transaction_type") == "DEBIT", -col("transaction_amount"))
    .otherwise(0)  # handle REVERSAL or other types
)

# Select subset of columns to be used for transaction subledger
ledger_df = signed_df.select(
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


# Upsert into transaction ledger table
upsert_transaction_ledger(ledger_df, jdbc_url, jdbc_props)

# Idempotent upsert of transaction ledger into stage table
upsert_transaction_balance(jdbc_url, jdbc_props)

# Derive run metrics
run_metrics_df = (
            df_with_lag.agg(
                count(lit(1)).alias("record_count"),
                spark_sum(when(col("txn_age_sec") > 900, 1).otherwise(0)).alias("late_arrival_count")
            )
        ) \
    .withColumn("run_id", lit(run_id)) \
    .withColumn("duplicate_count", lit(duplicate_summary.count())) \
    .withColumn("run_timestamp", current_timestamp())  \
    .select(
        "run_id",
        "record_count",
        "duplicate_count",
        "late_arrival_count",
        "run_timestamp"
    )

# Idempotent upsert into run metrics table
upsert_run_metrics(run_metrics_df, jdbc_url, jdbc_props)