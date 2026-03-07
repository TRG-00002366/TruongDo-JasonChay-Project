from pyspark.sql import SparkSession
import shutil
import os 

spark = (
    SparkSession.builder
    .appName("RDD_US_Accidents_Weather")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

#Load Parquet
df = spark.read.parquet("/home/truongdo/TruongDo-JuanJose-Project/sampled_accidents")
df.show(5)
df.printSchema()

#Convert to RDD
rdd = df.rdd

# Helper functions
def parse_float(x):
    try:
        if x is None or str(x).strip() == "" or str(x).strip().upper() == "NULL":
            return None
        return float(x)
    except:
        return None

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
    row["_c0"] != "ID" and                        
    row["_c28"] is not None and                   
    str(row["_c28"]).strip() != "" and
    str(row["_c28"]).strip().upper() != "NULL"
)



# Clean + validate rows

def clean_and_validate(row):
    accident_id = str(row["_c0"]).strip()

    start_time = standardize_timestamp(row["_c3"])
    end_time = standardize_timestamp(row["_c4"])
    weather_timestamp = standardize_timestamp(row["_c19"])

    temperature = parse_float(row["_c20"])
    wind_chill = parse_float(row["_c21"])
    humidity = parse_float(row["_c22"])
    pressure = parse_float(row["_c23"])
    visibility = parse_float(row["_c24"])
    wind_speed = parse_float(row["_c26"])
    precipitation = parse_float(row["_c27"])

    weather_condition = str(row["_c28"]).strip().upper()

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
        "Weather_Condition": weather_condition
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
output_path = "/home/truongdo/TruongDo-JuanJose-Project/outputs/weather_counts"

#Delete output folder if exist
if os.path.exists(output_path):
    shutil.rmtree(output_path)

#Save as a text file
output = weather_counts_sorted.map(lambda kv: f"{kv[0]}\t{kv[1]}")
output.saveAsTextFile(output_path)

spark.stop()