from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, count, avg, hour, row_number, desc, )
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("Batch DF ETL Process").master("local[*]").getOrCreate()

# TODO
filepath = "opt/spark-data/bronze"
hourly_output_path = "/home/truongdo/TruongDo-JasonChay-Project/outputs/hour_of_day_summary"
top_weather_output_path = "/home/truongdo/TruongDo-JasonChay-Project/outputs/top_weather_conditions"

# Read parquet into a DataFrame
df = spark.read.parquet(filepath)

# TODO
##### Transformations
    # Write each output to Parquet, partitioned and bucketed where appropriate.
    # Use **caching** on the base DataFrame to speed up multiple downstream transformations.

# 1. Accident Information by Hour of Day: Group by hour, calculate cols: 'total_accidents', 'avg_duration_mintues', 'avg_serverity'
hour_of_day_summary = (
    df.withColumn("hour_of_day", hour(col("Start_Time"))).groupBy("hour_of_day").agg(count("*").alias("total_accidents"),
          avg("duration_minutes").alias("avg_duration_minutes"),
          avg("Severity").alias("avg_severity"))
      .orderBy("hour_of_day"))

(hour_of_day_summary.write 
    .mode("overwrite") 
    .partitionBy("hour_of_day") 
    .parquet(hourly_output_path))
# 2. Top 10 Weather Conditions: Identify Weather conditions with the highest accident concentrations using Spark SQL window functions
weather_counts = (df.filter(col("Weather_Condition").isNotNull()).groupBy("Weather_Condition").agg(count("*").alias("accident_count")))

weather_window = Window.orderBy(desc("accident_count"))

top_10_weather = (weather_counts.withColumn("rank", row_number().over(weather_window)).filter(col("rank") <= 10))

(top_10_weather.write 
    .mode("overwrite") 
    .parquet(top_weather_output_path))

# 3. Weather Severity Impact: Join orders with a static 'weather.csv' reference, then aggregate severity statistics per weather condition

# 4. Weather Conditions Breakdown: Understand accident distribution across weather conditions

spark.stop()