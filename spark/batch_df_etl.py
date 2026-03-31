import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, to_timestamp, row_number, hour, count, avg, desc, round
from pyspark.sql.types import StringType, IntegerType, DoubleType, BooleanType, TimestampType, StructType, StructField
from pyspark.sql.window import Window
from pathlib import Path


# Initialize Spark
spark = SparkSession.builder \
    .appName("Batch JSON Processor") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")


# Load YAML schema
schema_path = Path("/opt/spark-config/schema.yaml")
print("Schema exists:", schema_path.exists())

with open(schema_path, "r") as f:
    schema_yaml = yaml.safe_load(f)
# with open("/opt/spark-config/schema.yaml", "r") as f:
#     schema_yaml = yaml.safe_load(f)

# Map YAML types to Spark types
type_mapping = {
    "string": StringType(),
    "integer": IntegerType(),
    "double": DoubleType(),
    "boolean": BooleanType(),
    "timestamp": TimestampType()
}

# Build Spark StructType
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

#####
##### READ DATA FROM BRONZE LAYER
#####
print("============================= Reading =============================")

# LOCAL FILE: Read multiple JSON files
# input_path = "data/raw/*.json"
# input_path = "/opt/spark-data/raw/*.json"
# df = spark.read \
#     .schema(spark_schema) \
#     .json(input_path)

# SNOWFLAKE: Read bronze table
sfOptions = {
    "sfURL": "DGWMVPP-ZEC99782.snowflakecomputing.com",
    "sfUser": "JASONCHAY",
    "sfPassword": "2bbt2WXurJDwTxa",
    "sfDatabase": "ACCIDENT_DB",
    "sfSchema": "BRONZE",
    "sfWarehouse": "COMPUTE_WH"
}

df = spark.read \
    .format("snowflake") \
    .options(**sfOptions) \
    .option("dbtable", "RAW_ACCIDENTS") \
    .load()

# Fix column names with () inside of them
rename_map = {'"Distance(mi)"': "Distance_mi",
              '"Temperature(F)"': "Temperature_F",
              '"Wind_Chill(F)"': "Wind_Chill_F",
              '"Humidity(%)"': "Humidity",
              '"Pressure(in)"': "Pressure_in",
              '"Visibility(mi)"': "Visibility_mi",
              '"Wind_Speed(mph)"': "Wind_Speed_mph",
              '"Precipitation(in)"': "Precipitation_in"
              }
df = df.withColumnsRenamed(rename_map)

# Cleaning transformations
for col_def in schema_yaml["columns"]:
    col_name = col_def["name"]

    # Trim strings
    if col_def.get("trim"):
        df = df.withColumn(col_name, trim(col(col_name)))

    # Convert timestamps
    if col_def["type"] == "timestamp":
        df = df.withColumn(col_name, to_timestamp(col(col_name)))

# Validation rules
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

# Deduplication
dedup_conf = schema_yaml.get("deduplication")

if dedup_conf:
    keys = dedup_conf["keys"]
    order_col = dedup_conf["order_by"]

    window_spec = Window.partitionBy(*keys).orderBy(col(order_col).desc())

    df = df.withColumn("row_num", row_number().over(window_spec)) \
           .filter(col("row_num") == 1) \
           .drop("row_num")

#####
##### WRITING PROCESSED DATA
#####

print("============================= Writing =============================")

# LOCAL FILE: Saving to silver folder
# df.write \
#     .mode("overwrite") \
#     .parquet("/opt/spark-data/silver")

# SNOWFLAKE: Saving to silver table
sfOptions["sfSchema"] = "SILVER"

(
    df.write
    .format("snowflake")
    .options(**sfOptions)
    .option("dbtable", "cleaned_accidents")
    .mode("append")
    .save()
)



##### DataFrame Transformations #####

# output_path = "data"
output_path = "/opt/spark-data"

# 1. Accident Information by Hour of Day: Group by hour, calculate cols: 'total_accidents', 'avg_serverity'
hour_of_day_summary = df.withColumn("hour_of_day", hour(col("Start_Time"))).groupBy("hour_of_day").agg(
    count("*").alias("total_accidents"),
    round(avg("Severity"), 2).alias("avg_severity")
).orderBy("hour_of_day")

hour_of_day_summary.write\
    .mode("overwrite")\
    .partitionBy("hour_of_day") \
    .parquet(f"{output_path}/analysis1")

hour_of_day_summary.show(24)

# 2. Top 10 Weather Conditions: Identify Weather conditions with the highest accident concentrations using Spark SQL window functions
weather_counts = df.filter(col("Weather_Condition").isNotNull()).groupBy("Weather_Condition").agg(count("*").alias("accident_count"))
weather_window = Window.orderBy(desc("accident_count"))
top_10_weather = weather_counts.withColumn("rank", row_number().over(weather_window)).filter(col("rank") <= 10)

top_10_weather.write \
    .mode("overwrite") \
    .partitionBy("Weather_Condition") \
    .parquet(f"{output_path}/analysis2")

top_10_weather.show()

# 3. Average Weather Condition Statistics Per Severity of Accident
weather_conditions = df.filter(col("Severity").isNotNull()).groupBy("Severity").agg(
    round(avg("Precipitation_in"), 2).alias("avg_precipitation_in"),
    round(avg("Temperature_F"), 2).alias("avg_temperature_F"),
    round(avg("Visibility_mi"), 2).alias("avg_visibility_mi")
).orderBy("Severity")

weather_conditions.write \
    .mode("overwrite") \
    .partitionBy("Severity") \
    .parquet(f"{output_path}/analysis3")

weather_conditions.show()

# 4. Accident Count by State
state_accidents = df.filter(col("State").isNotNull()).groupBy("State").agg(count("*").alias("accident_count"))
state_window = Window.orderBy(desc("accident_count"))
top_10_states = state_accidents.withColumn("rank", row_number().over(state_window))

top_10_states.write \
    .mode("overwrite") \
    .partitionBy("State") \
    .parquet(f"{output_path}/analysis4")

top_10_states.show()

spark.stop()