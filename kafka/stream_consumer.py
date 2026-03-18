from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType
import time

spark = SparkSession.builder\
    .appName("SparkStreamingConsumer")\
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1")\
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Unbounded DataFrame reading from the traffic-accidents topic
kafka_df = spark.readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", "localhost:9094")\
    .option("subscribe", "traffic_accidents")\
    .option("startingOffsets", "earliest")\
    .load()

# Grab just the JSON row data 
rows = kafka_df.selectExpr("CAST(value AS STRING) as value")

# Create schema
schema = StructType([
    StructField("ID", StringType(), False),
    StructField("Source", StringType(), True),
    StructField("Severity", IntegerType(), True),
    StructField("Start_Time", StringType(), True),
    StructField("End_Time", StringType(), True),
    StructField("Start_Lat", DoubleType(), True),
    StructField("Start_Lng", DoubleType(), True),
    StructField("End_Lat", DoubleType(), True),
    StructField("End_Lng", DoubleType(), True),
    StructField("Distance(mi)", DoubleType(), True),
    StructField("Description", StringType(), True),
    StructField("Street", StringType(), True),
    StructField("City", StringType(), True),
    StructField("County", StringType(), True),
    StructField("State", StringType(), True),
    StructField("Zipcode", IntegerType(), True),
    StructField("Country", StringType(), True),
    StructField("Timezone", StringType(), True),
    StructField("Airport_Code", StringType(), True),
    StructField("Weather_Timestamp", StringType(), True),
    StructField("Temperature(F)", DoubleType(), True),
    StructField("Wind_Chill(F)", DoubleType(), True),
    StructField("Humidity(%)", DoubleType(), True),
    StructField("Pressure(in)", DoubleType(), True),
    StructField("Temperature(F)", DoubleType(), True),
    StructField("Visibility(mi)", DoubleType(), True),
    StructField("Wind_Direction", StringType(), True),
    StructField("Precipitation(in)", DoubleType(), True),
    StructField("Weather_Condition", StringType(), True),
    StructField("Amenity", BooleanType(), True),
    StructField("Bump", BooleanType(), True),
    StructField("Crossing", BooleanType(), True),
    StructField("Give_Way", BooleanType(), True),
    StructField("Junction", BooleanType(), True),
    StructField("No_Exit", BooleanType(), True),
    StructField("Railway", BooleanType(), True),
    StructField("Roundabout", BooleanType(), True),
    StructField("Station", BooleanType(), True),
    StructField("Stop", BooleanType(), True),
    StructField("Traffic_Calming", BooleanType(), True),
    StructField("Traffic_Signal", BooleanType(), True),
    StructField("Turning_Loop", BooleanType(), True),
    StructField("Sunrise_Sunset", StringType(), True),
    StructField("Civil_Twilight", StringType(), True),
    StructField("Nautical_Twilight", StringType(), True),
    StructField("Astronomical_Twilight", StringType(), True),
])

parsed_rows = rows.select(from_json(col("value"), schema).alias("data")).select("data.*")

query = parsed_rows.writeStream \
    .format("json") \
    .option("path", "tmp_output_json") \
    .option("checkpointLocation", "tmp_checkpoints") \
    .outputMode("append") \
    .start()

query.awaitTermination()