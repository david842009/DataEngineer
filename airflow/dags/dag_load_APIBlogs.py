from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 3, 7),
    'retries': 1,
}

dag = DAG(
    dag_id='dag_load_APIBlogs',
    default_args=default_args,
    description='ELT de extraccion de datos de blogs sobre lanzamientos espaciales',
    schedule_interval='@daily',
    catchup=False,
)

extract_and_process = BashOperator(
    task_id='extract_and_process',
    bash_command='docker exec spark-master spark-submit --master spark://spark-master:7077 /opt/workspace/scripts/ntbk_load_APIBlogs.py',
    dag=dag,
)
transform_content_analysis = BashOperator(
    task_id='transform_content_analysis',
    bash_command='docker exec spark-master spark-submit --master spark://spark-master:7077 /opt/workspace/scripts/ntbk_transform_AnalisisContenidoBlogs.py',
    dag=dag,
)

transform_trend_analysis = BashOperator(
    task_id='transform_trend_analysis',
    bash_command='docker exec spark-master spark-submit --master spark://spark-master:7077 /opt/workspace/scripts/ntbk_transform_AnalisisTendenciasBlogs.py',
    dag=dag,
)

# Configurar la ejecución en secuencia
extract_and_process >> transform_content_analysis >> transform_trend_analysis