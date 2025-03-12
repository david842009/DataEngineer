from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 3, 7),
    'retries': 1,
}

dag = DAG(
    dag_id='dag_load_APIArticles',
    default_args=default_args,
    description='ELT de extraccion y transformacion de datos de articulos sobre lanzamientos espaciales',
    schedule_interval='@daily',
)

extract_articles = BashOperator(
    task_id='extract_articles',
    bash_command='docker exec spark-master spark-submit --master spark://spark-master:7077 /opt/workspace/scripts/ntbk_load_APIArticles.py',
    dag=dag,
    catchup=False,
)

transform_content_analysis = BashOperator(
    task_id='transform_content_analysis',
    bash_command='docker exec spark-master spark-submit --master spark://spark-master:7077 /opt/workspace/scripts/ntbk_transform_AnalisisContenidoArticle.py',
    dag=dag,
)

transform_trend_analysis = BashOperator(
    task_id='transform_trend_analysis',
    bash_command='docker exec spark-master spark-submit --master spark://spark-master:7077 /opt/workspace/scripts/ntbk_transform_AnalisisTendenciasArticle.py',
    dag=dag,
)

# Configurar la ejecución en secuencia
extract_articles >> transform_content_analysis >> transform_trend_analysis