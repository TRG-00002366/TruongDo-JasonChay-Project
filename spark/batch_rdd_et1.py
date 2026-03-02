from pyspark.sql import SparkSession
from datetime import datetime
import json 

spark = (
    SparkSession.builder
    .appName("Week1_RDD_US_Accidents_Weather")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

#Load Parquet
df = spark.read.parquet("data/accidents_clean")

#Convert to RDD
rdd = df.rdd

#Helper
def get(row, *candidates):
    """Get first existing column among candidates; return None if not found."""
    for c in candidates:
        if c in row.asDict(recursive=False):
            return row[c]
    return None

def set_field(d, name, value):
    d[name] = value
    return d

# filter out invalid records
valid_rdd = rdd.filter(lambda row:
        row["state_time"] is not None and
        row["state"] is not None and 
        row["severity"] in (1, 2, 3, 4) and
        row["weather_condition"] is not None and
        str(row["weather_condition"]).strip() != ""
        )

# total accidents per weather conditions
weather_counts = (
    valid_rdd
    .map(lambda row: (str(row["weather_condition"]).strip().upper(), 1))
    .reduceByKey(lambda a, b: a + b)
)

# sort output is readable (most accidents first)
weather_counts_sorted = weather_counts.sortBy(lambda kv: kv[1], ascending=False)

#Save as a text file
output = weather_counts_sorted.map(lambda kv: f"{kv[0]}\t{kv[1]}")
output.saveAsTextFile("outputs/weather_counts.txt")

spark.stop()