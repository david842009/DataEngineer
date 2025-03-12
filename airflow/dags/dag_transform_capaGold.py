from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 3, 7),
    'retries': 1,
}

dag = DAG(
    dag_id='space_launch_pipeline',
    default_args=default_args,
    description='ETL de extraccion de datos de lanzamientos espaciales',
    schedule_interval='@daily',
)

extract_and_process = BashOperator(
    task_id='extract_and_process',
    bash_command='docker exec spark-master spark-submit --master spark://spark-master:7077 /opt/workspace/scripts/extract_and_process.py',
    dag=dag,
)

extract_and_process