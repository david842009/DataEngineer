from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 3, 7),
    'retries': 1,
}

dag = DAG(
    dag_id='dag_load_APIReports',
    default_args=default_args,
    description='ELT de extraccion de datos de reportes sobre lanzamientos espaciales',
    schedule_interval='@daily',
    catchup=False,
)

extract_and_process = BashOperator(
    task_id='extract_and_process',
    bash_command='docker exec spark-master spark-submit --master spark://spark-master:7077 /opt/workspace/scripts/ntbk_load_APIReports.py',
    dag=dag,
)

extract_and_process