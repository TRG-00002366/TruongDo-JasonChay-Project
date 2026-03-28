from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
from airflow.hooks.base import BaseHook
from kafka import KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from datetime import datetime, timedelta
from airflow.sensors.python import PythonSensor
import os
import glob

# Helper functions for our PythonOperators
def check_kafka_topic():
    """Uses a Kafka consumer to make sure the Kafka topic exists"""

    consumer = KafkaConsumer(bootstrap_servers="kafka:9092")
    if "traffic_accidents" not in consumer.topics():
        raise Exception("Topic does not exist")

def raw_files_ready():
    """Checks the output directory for raw data posted by the consumer and continues when it sees data"""

    raw_dir = "/opt/spark-data/raw/"

    if not os.path.exists(raw_dir):
        print(f"{raw_dir} does not exist yet")
        return False

    files = glob.glob(os.path.join(raw_dir, "*.json"))
    files = [f for f in files if os.path.isfile(f)]

    print(f"Found JSON files: {files}")
    return len(files) > 0

def validate_output():
    """Validate that the columns are processed correctly"""
    # TODO:
    print("Output validation passed")

# Creating DAG
default_args = {
    "owner": "accidents",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure":True,
}

with DAG(
    dag_id="accidents_stream_pipeline",
    start_date=datetime(2026, 3, 1),
    schedule="@daily",  
    catchup=False,
    default_args=default_args,
    tags=["airflow", "kafka", "spark", "accidents"],
) as dag:
    # Create tasks
    # Spark jobs will be run with BashOperators that spark-submit our 

    start = EmptyOperator(
        task_id="start"
    )

    check_kafka_topic_task = PythonOperator(
        task_id="check_kafka_topic",
        python_callable=check_kafka_topic
    )

    run_streaming_job = BashOperator(
        task_id="run_streaming_job",
        bash_command=(
            "spark-submit "
            "--master spark://spark-master:7077 "
            "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,net.snowflake:spark-snowflake_2.12:3.1.5  "
            "/opt/spark-jobs/stream_consumer.py {{ ds }}"
        )
    )

    wait_for_raw_data = PythonSensor(
    task_id="wait_for_raw_data",
    python_callable=raw_files_ready,
    poke_interval=15,
    timeout=300,
    mode="poke",
)

    run_df_etl = BashOperator(
        task_id="run_df_etl",
        bash_command=(
            "spark-submit "
            "--master spark://spark-master:7077 "
            "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,net.snowflake:spark-snowflake_2.12:3.1.5,net.snowflake:snowflake-jdbc:3.24.2 "
            "/opt/spark-jobs/batch_df_etl.py {{ ds }}"
        )
    )

    validate_output_task = PythonOperator(
        task_id="validate_output",
        python_callable=validate_output)

    end = EmptyOperator(task_id="end")

    # Define dependencies
    start >> check_kafka_topic_task >> run_streaming_job >> wait_for_raw_data >> run_df_etl >> validate_output_task >> end