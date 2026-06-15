import os

# Java path
os.environ["JAVA_HOME"] = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.19.10-hotspot"

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, window
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    TimestampType
)

print("SCRIPT STARTED")

# --------------------------------------------------
# 1. Start Spark
# --------------------------------------------------
spark = SparkSession.builder \
    .appName("HospitalHeartRateMonitor") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("Spark started successfully.")

# --------------------------------------------------
# 2. Define schema
# --------------------------------------------------
schema = StructType([
    StructField("patient_id", StringType(), True),
    StructField("heart_rate", IntegerType(), True),
    StructField("event_time", TimestampType(), True)
])

# --------------------------------------------------
# 3. Read CSV file
# --------------------------------------------------
df = spark.read \
    .option("header", "true") \
    .schema(schema) \
    .csv("heart_stream.csv")

print("\nSample Data:")
df.show(10, False)

# --------------------------------------------------
# 4. Window Aggregation
# --------------------------------------------------
windowed = df.groupBy(
    col("patient_id"),
    window(col("event_time"), "2 minutes")
).agg(
    avg("heart_rate").alias("avg_heart_rate")
)

# --------------------------------------------------
# 5. Detect Alerts
# --------------------------------------------------
alerts = windowed.filter(
    col("avg_heart_rate") > 100
).select(
    col("patient_id"),
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    col("avg_heart_rate")
)

# --------------------------------------------------
# 6. Show Results
# --------------------------------------------------
print("\n================ ALERTS ================\n")

alerts.show(100, False)

print("\nProcessing complete.")

spark.stop()
