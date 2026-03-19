import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, to_timestamp, row_number, hour, count, avg, desc
from pyspark.sql.types import StringType, IntegerType, DoubleType, BooleanType, TimestampType, StructType, StructField
from pyspark.sql.window import Window
from pathlib import Path


# ----------------------------
# Initialize Spark
# ----------------------------
spark = SparkSession.builder \
    .appName("Batch JSON Processor") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# ----------------------------
# Load YAML schema
# ----------------------------
schema_path = Path("/opt/spark-config/schema.yaml")
print("Schema exists:", schema_path.exists())

with open(schema_path, "r") as f:
    schema_yaml = yaml.safe_load(f)
# with open("/opt/spark-config/schema.yaml", "r") as f:
#     schema_yaml = yaml.safe_load(f)

# ----------------------------
# Map YAML types → Spark types
# ----------------------------
type_mapping = {
    "string": StringType(),
    "integer": IntegerType(),
    "double": DoubleType(),
    "boolean": BooleanType(),
    "timestamp": TimestampType()
}

# ----------------------------
# Build Spark StructType
# ----------------------------
fields = []
for col_def in schema_yaml["columns"]:
    fields.append(
        StructField(
            col_def["name"],
            type_mapping[col_def["type"]],
            col_def["nullable"]
        )
    )

spark_schema = StructType(fields)

# ----------------------------
# Read multiple JSON files
# ----------------------------
# input_path = "data/raw/*.json"
input_path = "/opt/spark-data/raw/*.json"
df = spark.read \
    .schema(spark_schema) \
    .json(input_path)

# ----------------------------
# Cleaning transformations
# ----------------------------
for col_def in schema_yaml["columns"]:
    col_name = col_def["name"]

    # Trim strings
    if col_def.get("trim"):
        df = df.withColumn(col_name, trim(col(col_name)))

    # Convert timestamps
    if col_def["type"] == "timestamp":
        df = df.withColumn(col_name, to_timestamp(col(col_name)))

# ----------------------------
# Validation rules
# ----------------------------
for col_def in schema_yaml["columns"]:
    col_name = col_def["name"]

    # NOT NULL
    if not col_def["nullable"]:
        df = df.filter(col(col_name).isNotNull())

    # Min constraint
    if "min" in col_def:
        df = df.filter(col(col_name) >= col_def["min"])

    # Max constraint
    if "max" in col_def:
        df = df.filter(col(col_name) <= col_def["max"])

# ----------------------------
# Deduplication
# ----------------------------
dedup_conf = schema_yaml.get("deduplication")

if dedup_conf:
    keys = dedup_conf["keys"]
    order_col = dedup_conf["order_by"]

    window_spec = Window.partitionBy(*keys).orderBy(col(order_col).desc())

    df = df.withColumn("row_num", row_number().over(window_spec)) \
           .filter(col("row_num") == 1) \
           .drop("row_num")

# ----------------------------
# Write output
# ----------------------------

df.show()
df.write \
    .mode("overwrite") \
    .json("data/silver/")

# output_path = "data"
output_path = "/opt/spark-data"

# 1. Accident Information by Hour of Day: Group by hour, calculate cols: 'total_accidents', 'avg_serverity'
hour_of_day_summary = df.withColumn("hour_of_day", hour(col("Start_Time"))).groupBy("hour_of_day").agg(
    count("*").alias("total_accidents"),
    avg("Severity").alias("avg_severity")
).orderBy("hour_of_day")

hour_of_day_summary.write\
    .mode("overwrite")\
    .partitionBy("hour_of_day") \
    .parquet(f"{output_path}/analysis1")


# 2. Top 10 Weather Conditions: Identify Weather conditions with the highest accident concentrations using Spark SQL window functions
weather_counts = df.filter(col("Weather_Condition").isNotNull()).groupBy("Weather_Condition").agg(count("*").alias("accident_count"))
weather_window = Window.orderBy(desc("accident_count"))
top_10_weather = weather_counts.withColumn("rank", row_number().over(weather_window)).filter(col("rank") <= 10)

top_10_weather.write \
    .mode("overwrite") \
    .partitionBy("Weather_Condition") \
    .parquet(f"{output_path}/analysis2")

# 3. Average Weather Condition Statistics Per Severity of Accident
weather_conditions = df.filter(col("Severity").isNotNull()).groupBy("Severity").agg(
    avg("Precipitation(in)").alias("avg_precipitation(in)"),
    avg("Temperature(F)").alias("avg_temperature(F)"),
    avg("Visibility(mi)").alias("avg_visibility(mi)"),
)

weather_conditions.write \
    .mode("overwrite") \
    .partitionBy("Severity") \
    .parquet(f"{output_path}/analysis3")

# 4. Accident Count by State
state_accidents = df.filter(col("State").isNotNull()).groupBy("State").agg(count("*").alias("accident_count"))
state_window = Window.orderBy(desc("accident_count"))
top_10_states = state_accidents.withColumn("rank", row_number().over(state_window))

top_10_states.write \
    .mode("overwrite") \
    .partitionBy("State") \
    .parquet(f"{output_path}/analysis4")

spark.stop()