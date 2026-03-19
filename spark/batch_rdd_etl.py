from pyspark.sql import SparkSession
from pyspark.sql.functions import avg
import shutil
import os 
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
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

#Load Parquet
df = spark.read.csv("generated_accidents.csv", header=True, inferSchema=True)
df.write.parquet("generated_accidents")
df = spark.read.parquet("generated_accidents")
df.show(5)
df.printSchema()

#Convert to RDD
rdd = df.rdd

numeric_cols = [
    "Temperature(F)",
    "Wind_Chill(F)",
    "Humidity(%)",
    "Pressure(in)",
    "Visibility(mi)",
    "Wind_Speed(mph)",
    "Precipitation(in)"
]

avg_values = (
    df.select([avg(col).alias(col) for col in numeric_cols])
      .first()
      .asDict()
)

df = df.fillna(avg_values)

# Helper functions
def parse_float(x):
    try:
        if x is None or str(x).strip() == "" or str(x).strip().upper() == "NULL":
            return None
        return float(x)
    except:
        return None

# Adds seconds to timestamp if it's missing
def standardize_timestamp(x):
    if x is None or str(x).strip() == "" or str(x).strip().upper() == "NULL":
        return None
    s = str(x).strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d %H:%M:%S")
        except:
            pass
    return None


# Filter out bad rows

filtered_rdd = rdd.filter(lambda row:
    row["ID"] is not None and                        
    row["Weather_Condition"] is not None and                   
    str(row["Weather_Condition"]).strip() != "" and
    str(row["Weather_Condition"]).strip().upper() != "NULL"
)



# Clean + validate rows

def clean_and_validate(row):
    accident_id = str(row["ID"]).strip()

    start_time = standardize_timestamp(row["Start_Time"])
    end_time = standardize_timestamp(row["End_Time"])
    weather_timestamp = standardize_timestamp(row["Weather_Timestamp"])

    temperature = parse_float(row["Temperature(F)"])
    wind_chill = parse_float(row["Wind_Chill(F)"])
    humidity = parse_float(row["Humidity(%)"])
    pressure = parse_float(row["Pressure(in)"])
    visibility = parse_float(row["Visibility(mi)"])
    wind_speed = parse_float(row["Wind_Speed(mph)"])
    precipitation = parse_float(row["Precipitation(in)"])

    weather_condition = str(row["Weather_Condition"]).strip().upper()

    # Validate quantitative values make sense
    if humidity is not None and not (0 <= humidity <= 100):
        return None
    if visibility is not None and visibility < 0:
        return None
    if wind_speed is not None and wind_speed < 0:
        return None
    if precipitation is not None and precipitation < 0:
        return None
    if temperature is not None and not (-80 <= temperature <= 140):
        return None
    if wind_chill is not None and not (-120 <= wind_chill <= 140):
        return None
    if pressure is not None and not (25 <= pressure <= 35):
        return None

    # Require valid standardized timestamps
    if start_time is None:
        return None

    return {
        "ID": accident_id,
        "Start_Time": start_time,
        "End_Time": end_time,
        "Weather_Timestamp": weather_timestamp,
        "Weather_Condition": weather_condition,
        "Temperature(F)": temperature,
        "Wind_Chill(F)": wind_chill,
        "Humidity(%)": humidity,
        "Pressure(in)": pressure,
        "Visibility(mi)": visibility,
        "Wind_Speed(mph)": wind_speed,
        "Precipitation(in)": precipitation
    }

cleaned_rdd = filtered_rdd.map(clean_and_validate).filter(lambda x: x is not None)


# Count accidents by weather condition

weather_counts = (
    cleaned_rdd
    .map(lambda row: (row["Weather_Condition"], 1))
    .reduceByKey(lambda a, b: a + b)
    .sortBy(lambda kv: kv[1], ascending=False)
)

# total accident using RDDs
weather_counts = (
    cleaned_rdd
    .map(lambda row: (row["Weather_Condition"], 1))
    .reduceByKey(lambda a, b: a + b)
)


# sort output is readable (most accidents first)
weather_counts_sorted = weather_counts.sortBy(lambda kv: kv[1], ascending=False)

# Output path
output_path = "outputs/weather_counts"

#Delete output folder if exist
if os.path.exists(output_path):
    shutil.rmtree(output_path)

#Save as a text file
output = weather_counts_sorted.map(lambda kv: f"{kv[0]}\t{kv[1]}")
output.saveAsTextFile(output_path)

spark.stop()