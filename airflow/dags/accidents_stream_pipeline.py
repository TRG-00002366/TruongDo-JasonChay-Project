from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="accidents_stream_pipeline",
    start_date=datetime(2026, 3, 1),
    schedule=None,
    catchup=False,
) as dag:

    run_consumer = BashOperator(
        task_id="run_stream_consumer",
        bash_command="python /opt/airflow/spark/stream_consumer.py",
    )

    run_producer = BashOperator(
        task_id="run_producer",
        bash_command="python /opt/airflow/kafka/producer.py",
    )

    run_consumer >> run_producer