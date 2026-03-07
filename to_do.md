## 1. NOW : producer.py using Faker
    - we should use a subset of data and calculate ome statistics on it
    - we can use mean, std, probabilities, skew, etc. 
    - supply Faker with these parameters to make realistic distributions in its generated data

## 2. LATER : stream_consumer.py - PySpark streaming consumer
    - this will read the produced events via a stream
    - save raw data to Parquet sink partitioned by date
    - I believe we need to learn Kafka to use this

## 3. NOW : batch_rdd_etl.py - RDD processing
    - load raw Parquet data as RDD
    - figure out data schema
    - all data processing: validating, cleaning, dedup
    - split with DF processing if we feel like its needed
    - save results as text file

## 4. NOW : batch_df_etl.py - DF processing
    - load raw Parquet data as DF
    - perform transformations from proj specs
    - use caching on the base DF to speed up transformations
    - save each output as Parquet, partitioned and bucketed where appropriate

## 5. LATER : accident_dag.py - Airflow Orchestration
    - Create Airflow DAG accident_pipeline within accident_dag.py
    - need to learn Airflow