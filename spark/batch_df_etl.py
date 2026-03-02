from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Batch DF ETL Process").master("local[*]").getOrCreate()

# TODO
filepath = ""

# Read parquet into a DataFrame
df = spark.read.parquet(filepath)

# TODO
##### Transformations
    # Write each output to Parquet, partitioned and bucketed where appropriate.
    # Use **caching** on the base DataFrame to speed up multiple downstream transformations.

# 1. Accident Information by Hour of Day: Group by hour, calculate cols: 'total_accidents', 'avg_duration_mintues', 'avg_serverity'

# 2. Top 10 Weather Conditions: Identify Weather conditions with the highest accident concentrations using Spark SQL window functions

# 3. Weather Severity Impact: Join orders with a static 'weather.csv' reference, then aggregate severity statistics per weather condition

# 4. Weather Conditions Breakdown: Understand accident distribution across weather conditions