from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "accidents",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
}

# Parameterized values
PRODUCER_SCRIPT = "/opt/airflow/kafka/producer.py"
CONSUMER_SCRIPT = "/opt/airflow/spark/stream_consumer.py"
KAFKA_TOPIC = "us_accidents"
OUTPUT_PATH = "/opt/airflow/outputs"
BOOTSTRAP_SERVER = "kafka:9092"

with DAG(
    dag_id="accidents_stream_pipeline",
    start_date=datetime(2026, 3, 1),
    schedule="* * * * *",  
    catchup=False,
    default_args=default_args,
    tags=["airflow", "kafka", "spark", "accidents"],
) as dag:

    run_producer = BashOperator(
        task_id="run_producer",
        bash_command=(
            f"python {PRODUCER_SCRIPT} "
            f"--topic {KAFKA_TOPIC} "
            f"--bootstrap-servers {BOOTSTRAP_SERVER}"
        ),
    )

    run_consumer = BashOperator(
        task_id="run_stream_consumer",
        bash_command=(
            f"python {CONSUMER_SCRIPT} "
            f"--topic {KAFKA_TOPIC} "
            f"--bootstrap-servers {BOOTSTRAP_SERVER} "
            f"--output-path {OUTPUT_PATH}"
        ),
    )

    run_producer >> run_consumer