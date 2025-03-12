from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 3, 8),
    'retries': 0,
}

dag = DAG(
    dag_id='test_pipeline_spark_dag',
    default_args=default_args,
    description='DAG de prueba para ejecutar y comprobar ecosistema de apache spark',
    schedule_interval=None,
)

run_spark_job = BashOperator(
    task_id='run_spark_test',
    bash_command='docker exec spark-master spark-submit --master spark://spark-master:7077 /opt/workspace/scripts/ntbk_transform_dimensiones.py',
    dag=dag,
)

run_spark_job