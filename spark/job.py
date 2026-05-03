from pyspark.sql.functions import to_timestamp
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, row_number, desc, when, sum, count, max, min, coalesce, unix_timestamp, lit, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType, MapType
from pyspark.sql.window import Window

from pyspark.sql import SparkSession

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
        StructField("fund_id", IntegerType(), False),
        StructField("fund_name", StringType(), False),
    ]), False),
    StructField("deal", StructType([
        StructField("deal_id", IntegerType(), False),
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
    "timestamp"
)

# Parse event and add new column to raw_df for each event field
parsed_df = events_df.withColumn(
    "json", from_json(col("value"), schema)).select("json.*")

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
    "metadata"
)

# Detect if there are duplicate records in a batch
duplicate_summary = flat_df.groupBy("transaction_id").agg(
    count("*").alias("duplicate_count"),
    max("transaction_timestamp").alias("latest_timestamp"),
    min("transaction_timestamp").alias("earliest_timestamp")
)

duplicates = duplicate_summary.filter(col("duplicate_count") > 1)

## OPEN: Need to add logic to log this into db somewhere ##


# Deduplicate: keep the most recent transaction
window_spec = Window.partitionBy("transaction_id").orderBy(
    desc("transaction_timestamp"))

deduped_df = flat_df.withColumn(
    "rn", row_number().over(window_spec)
).filter(col("rn") == 1).drop("rn")

# Handle late arriving events
lagged_df = deduped_df.withColumn("ingest_delay_sec", unix_timestamp(
    "event_timestamp") - unix_timestamp("transaction_timestamp")
)

# Discard events that are more than 600 seconds old
filter_df = lagged_df.filter(col("ingest_delay_sec") <= 600)

# Ensure proper event order (for batch processing)
ordered_df = filter_df.orderBy("transaction_timestamp")

# Calculate signed amounts based on transaction type
signed_df = deduped_df.withColumn(
    "signed_amount",
    when(col("transaction_type") == "CREDIT", col("transaction_amount"))
    .when(col("transaction_type") == "DEBIT", -col("transaction_amount"))
    .otherwise(0)  # handle REVERSAL or other types
)

# Aggregate net cash flow by fund and deal
net_df = signed_df.groupBy("fund_id", "deal_id").agg(
    sum("signed_amount").alias("net_cash_flow")
)

net_df.show()
# Upsert into state table
