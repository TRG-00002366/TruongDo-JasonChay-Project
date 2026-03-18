from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
from datetime import datetime, timedelta
import os

default_args = {
    "owner": "accidents",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure":True,
}

# This needs to connect to Kafka broker to see if our topic is created
# maybe a KafkaAdminClient()
def check_kafka_topic():
    topic = "traffic_accidents"
    if not topic:
        raise ValueError("No Kafka Topic")
    print("Kafka topic passed")

# Needs to validate the columns in each row with some logic, maybe in an external file if the logic becomes long
def validate_output():
    if not os.path.exists("/opt/airflow/outputs"):
        raise ValueError("Output Folder missing")
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
            "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:4.1.1 "
            "/opt/airflow/spark/stream_consumer.py "
            "--bootstrap-servers kafka:9094 "
            "--duration 120 "
            "--output-path /opt/airflow/outputs/raw/{{ ds }}"
        )
    )

    wait_for_raw_data = FileSensor(
        task_id="wait_for_raw_data",
        filepath="outputs/raw",
        poke_interval=30,
        timeout=300
    )

    run_rdd_etl = BashOperator(
        task_id="run_rdd_etl",
        bash_command=(
            "spark-submit "
            "--master spark://spark-master:7077 "
            "/opt/airflow/spark/batch_rdd_etl.py"
        )
    )

    run_df_etl = BashOperator(
        task_id="run_df_etl",
        bash_command=(
            "spark-submit "
            "--master spark://spark-master:7077 "
            "/opt/airflow/spark/batch_df_etl.py"
        )
    )

    validate_output_task = PythonOperator(
        task_id="validate_output",
        python_callable=validate_output)

    end = EmptyOperator(task_id="end")

    start >> check_kafka_topic_task >> run_streaming_job >> wait_for_raw_data \
        >> run_rdd_etl >> run_df_etl >> validate_output_task >> end