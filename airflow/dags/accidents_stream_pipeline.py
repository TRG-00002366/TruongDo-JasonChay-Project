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

default_args = {
    "owner": "accidents",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure":True,
}

# This needs to connect to Kafka broker to see if our topic is created
# maybe a KafkaAdminClient()
def check_kafka_topic():
    consumer = KafkaConsumer(bootstrap_servers="kafka:9092")
    if "traffic_accidents" not in consumer.topics():
        raise Exception("Topic does not exist")


def raw_files_ready():
    raw_dir = "/opt/spark-data/raw/"

    if not os.path.exists(raw_dir):
        print(f"{raw_dir} does not exist yet")
        return False

    files = glob.glob(os.path.join(raw_dir, "*.json"))
    files = [f for f in files if os.path.isfile(f)]

    print(f"Found JSON files: {files}")
    return len(files) > 0

# Needs to validate the columns in each row with some logic, maybe in an external file if the logic becomes long
def validate_output():
    # if not os.path.exists("/opt/airflow/outputs"):
    #     raise ValueError("Output Folder missing")
    print("Output validation passed")

with DAG(
    dag_id="accidents_stream_pipeline",
    start_date=datetime(2026, 3, 1),
    schedule="@daily",  
    catchup=False,
    default_args=default_args,
    tags=["airflow", "kafka", "spark", "accidents"],
) as dag:

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
            "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 "
            "/opt/spark-jobs/stream_consumer.py {{ ds }}"
            #"--duration 120 "
            #"--output-path /opt/airflow/outputs/raw/{{ ds }}"
        )
    )

    wait_for_raw_data = PythonSensor(
    task_id="wait_for_raw_data",
    python_callable=raw_files_ready,
    poke_interval=15,
    timeout=300,
    mode="poke",
)
    wait_for_files_to_settle = BashOperator(
    task_id="wait_for_files_to_settle",
    bash_command="sleep 10",
)

    run_df_etl = BashOperator(
        task_id="run_df_etl",
        bash_command=(
            "spark-submit "
            "--master spark://spark-master:7077 "
            "/opt/spark-jobs/batch_df_etl.py {{ ds }}"
        )
    )

    validate_output_task = PythonOperator(
        task_id="validate_output",
        python_callable=validate_output)

    end = EmptyOperator(task_id="end")

    start >> check_kafka_topic_task >> run_streaming_job >> wait_for_raw_data \
        >> wait_for_files_to_settle >> run_df_etl >> validate_output_task >> end