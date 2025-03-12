#==================================================================================================================================#
# Description: Este notebook realiza la extracción de los datos de la API spaceflightnewsapi.net y los guarda en formato parquet en#
#              el directorio /opt/workspace/parquet/brz_stage/tbl_brz_articles                                                     #
# Author:      Fabian David Carreño León                                                                                           #           
# Date:        2025-03-10                                                                                                          #       
# Version:     1.0                                                                                                                 #              
#==================================================================================================================================#

#==================================================================================================================================#
# Stage 0: Importar librerías necesarias                                                                                           #
#==================================================================================================================================#

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, monotonically_increasing_id

#==================================================================================================================================#
# Stage 1: Configurar sesion de Spark y lectura de tablas                                                                   #
#==================================================================================================================================#
# Iniciar sesión de Spark
spark = SparkSession.builder \
    .appName("Gold Layer Creation") \
    .enableHiveSupport() \
    .getOrCreate()

# Cargar las tablas Silver desde el metastore
df_analysis = spark.read.parquet("/opt/workspace/parquet/slv_stage/tbl_slv_analysis")  # Contiene temas y distribuciones
df_sources = spark.read.parquet("/opt/workspace/parquet/slv_stage/tbl_slv_sources")  # Contiene tendencias por mes y año
df_trends = spark.read.parquet("/opt/workspace/parquet/slv_stage/tbl_slv_trends")  # Contiene fuentes de noticias más activas

#==================================================================================================================================#
# Stage 2: Definicion esquema de datos                                                                                             #
#==================================================================================================================================#

df_dim_news_source = df_sources.select(
    monotonically_increasing_id().alias("source_id"),
    col("news_site").alias("name"),
    col("domain_url").alias("url"),
    col("count").alias("reliability_score"),
    col("source")
).distinct()

df_dim_topic = df_analysis.select(
    monotonically_increasing_id().alias("topic_id"),
    col("topic")
).distinct()

df_fact_article = df_analysis.alias("a") \
    .join(df_dim_news_source.alias("s"), col("a.news_site") == col("s.name"), "left") \
    .join(df_dim_topic.alias("t"), col("a.topic") == col("t.topic"), "left") \
    .select(
        col("a.id").alias("article_id"),
        col("s.source_id"),
        col("t.topic_id"),
        col("a.sentiment_score"),
        col("a.title"),
        col("a.summary"),
        col("a.clean_summary"),
        col("a.persons"),
        col("a.organizations"),
        col("a.locations"),
        col("a.topicDistribution"),
        col("a.topic"),
        col("a.source")
    )

#==================================================================================================================================#
# Stage 3: Almacenamiento de la información                                                                                        #
#==================================================================================================================================#
# Guardar en la capa Gold
df_dim_news_source.write.mode("overwrite").parquet("/opt/workspace/parquet/gld_stage/tbl_gld_dim_news_source")
df_dim_topic.write.mode("overwrite").parquet("/opt/workspace/parquet/gld_stage/tbl_gld_dim_topic")
df_fact_article.write.mode("overwrite").parquet("/opt/workspace/parquet/gld_stage/tbl_gld_fact_article")