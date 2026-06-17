from pyspark import SparkContext

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

def read_postgres_query(spark, sql_query, jdbc_url, jdbc_props):
    reader = spark.read.format("jdbc") \
        .option("url", jdbc_url) \
        .option("user", jdbc_props["user"]) \
        .option("password", jdbc_props["password"]) \
        .option("driver", jdbc_props["driver"])

    return reader.option("dbtable", f"({sql_query}) as t").load()