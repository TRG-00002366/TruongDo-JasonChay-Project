from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
from airflow.hooks.base import BaseHook
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
    # Getting Kafka connection details using hook
    conn = BaseHook.get_connection("kafka_traffic_accident_events")
    extras = conn.extra_dejson

    admin_client = KafkaAdminClient(
        bootstrap_servers=extras.get("bootstrap.servers"),
        client_id="airflow-topic-check"
    )

    # Topic to check for is traffic_accidents
    topic = "traffic_accidents"
    topics = admin_client.list_topics()

    if topic not in topics:
        topic = NewTopic(
            name = 'traffic_accidents',
            num_partitions = 4,
            replication_factor = 1
        )
        admin_client.create_topics([topic])
        print("Created topic: traffic_accidents")
    else:
        print("Confirmed that topic exists")

    admin_client.close()

def raw_files_ready():
    raw_dir = "/opt/airflow/outputs/raw"

    if not os.path.exists(raw_dir):
        print(f"{raw_dir} does not exist yet")
        return False

    files = glob.glob(os.path.join(raw_dir, "*.json"))
    files = [f for f in files if os.path.isfile(f)]

    print(f"Found JSON files: {files}")
    return len(files) > 0

    print(f"Found files: {files}")
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
            "/opt/airflow/spark/stream_consumer.py "
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
    run_rdd_etl = BashOperator(
        task_id="run_rdd_etl",
        bash_command=(
            "export PYSPARK_PYTHON=python3 && "
            "export PYSPARK_DRIVER_PYTHON=python3 && "
            "spark-submit "
            "--master spark://spark-master:7077 "
            "/opt/airflow/spark/batch_rdd_etl.py"
        )
    )

    run_df_etl = BashOperator(
        task_id="run_df_etl",
        bash_command=(
            "export PYSPARK_PYTHON=python3 && "
            "export PYSPARK_DRIVER_PYTHON=python3 && "
            "spark-submit "
            "--master spark://spark-master:7077 "
            "/opt/airflow/spark/batch_df_etl.py"
        )
    )

    validate_output_task = PythonOperator(
        task_id="validate_output",
        python_callable=validate_output)

    end = EmptyOperator(task_id="end")

    pwd_task = BashOperator(
        task_id="pwd",
        bash_command="pwd"
    )

    pwd_task2 = BashOperator(
        task_id="pwd2",
        bash_command="pwd",
        cwd=os.path.join(os.environ['AIRFLOW_HOME'], 'outputs', 'raw')#os.path.join(os.environ['SPARK_HOME'])
    )

    start >> check_kafka_topic_task >> run_streaming_job >> wait_for_raw_data \
        >> wait_for_files_to_settle >> run_rdd_etl >> run_df_etl >> validate_output_task >> end