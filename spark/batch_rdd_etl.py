from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, when, upper, trim, try_to_timestamp
import shutil
import yaml
import os 
import glob
import sys
from datetime import datetime

# Any validation, cleaning, deduplication to be done before we perform DataFrame transformations

# What should we do with Nulls? Probably fill with mean, or with 0

# Validate bad rows 
#   is a negative value allowed?
#   cant have string in a numerical col like temperature
# Cleaning
#   maybe convert dates if anything
#   not sure might have to come back after we know what analysis we are doing
# Deduplication
#   might not have to do this
#   if there are no duplicate primary keys first column

spark = (
    SparkSession.builder
    .appName("RDD_US_Accidents_Weather")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")
#json_files = glob.glob("/opt/spark-data/raw/*.json")

print(os.getcwd())
json_files = glob.glob("data/raw/*.json")

if not json_files:
    print("No JSON files found in /opt/spark-data/raw")
    spark.stop()
    sys.exit(0)

# Read from RAW (stream output)
df = spark.read.json(json_files)

if len(df.columns) == 0:
    print("JSON files found, but schema is empty")
    spark.stop()
    sys.exit(0)

# df.write.mode("overwrite").parquet("/opt/spark-data/silver")

# df = spark.read.parquet("/opt/spark-data/silver")
# df.show(5)
# df.printSchema()

# Load schema
with open("config/schema.yaml", "r") as f:
    schema = yaml.safe_load(f)["columns"]

# -----------------------------
# STEP 1: Normalize NULL values
# -----------------------------
for col_name in schema:
    df = df.withColumn(
        col_name,
        when(
            col(col_name).isNull() |
            (trim(col(col_name)) == "") |
            (upper(trim(col(col_name))) == "NULL"),
            None
        ).otherwise(col(col_name))
    )

# -----------------------------
# STEP 2: Apply transformations
# -----------------------------
for col_name, cfg in schema.items():

    # STRING handling
    if cfg["type"] == "string":
        if cfg.get("strip"):
            df = df.withColumn(col_name, trim(col(col_name)))
        if cfg.get("uppercase"):
            df = df.withColumn(col_name, upper(col(col_name)))

    # FLOAT handling
    elif cfg["type"] == "float":
        df = df.withColumn(col_name, col(col_name).cast("double"))

    
# -----------------------------
# STEP 3: Fill numeric NULLs with avg
# -----------------------------
numeric_cols = [
    col_name for col_name, cfg in schema.items()
    if cfg.get("type") == "float" and cfg.get("fillna") == "avg"
]

avg_exprs = [avg(col(c)).alias(c) for c in numeric_cols]
avg_values = df.select(avg_exprs).collect()[0].asDict()

df = df.fillna(avg_values)

# -----------------------------
# STEP 4: Apply validation rules
# -----------------------------
for col_name, cfg in schema.items():

    # Required fields
    if cfg.get("required"):
        df = df.filter(col(col_name).isNotNull())

    # Numeric ranges
    if cfg.get("type") == "float":
        if "min" in cfg:
            df = df.filter((col(col_name).isNull()) | (col(col_name) >= cfg["min"]))
        if "max" in cfg:
            df = df.filter((col(col_name).isNull()) | (col(col_name) <= cfg["max"]))

# -----------------------------
# STEP 5: Aggregation
# -----------------------------
weather_counts_df = (
    df.groupBy("Weather_Condition")
      .count()
      .orderBy(col("count").desc())
)

# -----------------------------
# STEP 6: Save output
# -----------------------------
#output_path = "/opt/spark-data/clean"
output_path = "data/silver"

if os.path.exists(output_path):
    shutil.rmtree(output_path)

(
    weather_counts_df
    .selectExpr("concat(Weather_Condition, '\t', count) as value")
    .write
    .mode("overwrite")
    .json(output_path)
)

spark.stop()