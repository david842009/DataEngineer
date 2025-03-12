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

from pyspark.sql import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, ArrayType, TimestampType
from pyspark.sql.functions import to_timestamp, col, explode_outer, lit, current_timestamp, when
from pyspark.sql.utils import AnalysisException
from datetime import datetime
import requests
import os
import time
import urllib.parse 
os.environ["SPARK_LOCAL_IP"] = "192.168.1.95"

#==================================================================================================================================#
# Stage 1: Configurar sesion de Spark con Delta tables y workers                                                                   #
#==================================================================================================================================#
spark = SparkSession.builder \
    .appName("sessionELTAPIReports") \
    .getOrCreate()

def log_execution(stage, process_name, status, record_count=0):
    log_entry = [(datetime.now().isoformat(), process_name, stage, status, record_count)]
    schema = StructType([
        StructField("execution_date", StringType(), False),
        StructField("process_name", StringType(), False),
        StructField("stage", StringType(), False),
        StructField("status", StringType(), False),
        StructField("record_count", IntegerType(), False)
    ])
    return spark.createDataFrame(log_entry, schema=schema)

#==================================================================================================================================#
# Stage 2: Definicion esquema de datos                                                                                             #
#==================================================================================================================================#
process_name = "Extract_API_Reports"
log_data = log_execution("Inicio del proceso", process_name, "Running")

# Esquema para "Author"
author_schema = StructType([
    StructField("name", StringType(), False),
    StructField("socials", StructType([
        StructField("youtube", StringType(), True),
        StructField("instagram", StringType(), True),
        StructField("linkedin", StringType(), True),
        StructField("mastodon", StringType(), True),
        StructField("bluesky", StringType(), True)
    ]), True)
])

# Esquema para "Launch"
launch_schema = StructType([
    StructField("launch_id", StringType(), False),
    StructField("provider", StringType(), False)
])

# Esquema para "Event"
event_schema = StructType([
    StructField("event_id", IntegerType(), False),
    StructField("provider", StringType(), False)
])

# Esquema para "Report"
report_schema = StructType([
    StructField("id", StringType(), False),
    StructField("title", StringType(), False),
    StructField("authors", ArrayType(author_schema), False),
    StructField("url", StringType(), False),
    StructField("image_url", StringType(), False),
    StructField("news_site", StringType(), False),
    StructField("summary", StringType(), False),
    StructField("published_at", StringType(), False),
    StructField("updated_at", StringType(), False)
])

#==================================================================================================================================#
# Stage 3: Definicion de funciones Extraccion
#==================================================================================================================================#

def fetch_paginated_data(api_url, limit=100, updated_at_gt="1950-01-01T00:00:00Z"):
    all_results = []
    
    
    encoded_date = urllib.parse.quote(str(updated_at_gt))
    
    next_url = f"{api_url}?limit={limit}&updated_at_gt={encoded_date}"

    while next_url:
        response = requests.get(next_url)

        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", 5))
            print(f"Rate limit exceeded. Retrying after {retry_after} seconds...")
            time.sleep(retry_after)
            continue 

        data = response.json()
        results = data.get("results", [])
        all_results.extend(results)

        next_url = data.get("next")

    return all_results

def read_table_data():
    path = "/opt/workspace/parquet/brz_stage/tbl_brz_reports"
    if not os.path.exists(path) or not os.listdir(path):
        return "1950-01-01T00:00:00Z"
    else:
        df = spark.read.parquet(path)
        return df.select("updated_at").agg({"updated_at": "max"}).collect()[0][0]

#==================================================================================================================================#
# Stage 4: Limpieza y estadarización de datos                                                                                      #
#==================================================================================================================================#

API_URL = "https://api.spaceflightnewsapi.net/v4/reports"

dataReports = fetch_paginated_data(API_URL, updated_at_gt=read_table_data())
# Procesar y guardar datos Reports
df_reports = spark.createDataFrame(dataReports, schema=report_schema)
df_reports = df_reports.withColumn("published_at", to_timestamp("published_at", "yyyy-MM-dd'T'HH:mm:ssX"))
df_reports = df_reports.withColumn("updated_at", to_timestamp("updated_at", "yyyy-MM-dd'T'HH:mm:ss.SSSSSSX"))
df_reports = df_reports.withColumn("fecha_registro_dl", current_timestamp())
df_reports = df_reports.withColumn("estado_registro_dl", lit(1))

# Explodemos los arrays de estructuras
df_reports = df_reports.withColumn("authors_exploded", explode_outer("authors"))

# Expandimos los campos de Struct en columnas
df_reports = df_reports.select(
    col("id"), 
    col("title"),
    col("url"),
    col("image_url"),
    col("news_site"),
    col("summary"),
    col("published_at"),
    col("updated_at"),
    col("fecha_registro_dl"),
    col("estado_registro_dl"),
    col("authors_exploded.name").alias("authors_name"),
    col("authors_exploded.socials.youtube").alias("authors_socials_youtube"),
    col("authors_exploded.socials.instagram").alias("authors_socials_instagram"),
    col("authors_exploded.socials.linkedin").alias("authors_socials_linkedin"),
    col("authors_exploded.socials.mastodon").alias("authors_socials_mastodon"),
    col("authors_exploded.socials.bluesky").alias("authors_socials_bluesky")
)

log_data = log_data.union(log_execution("Transformacion de los datos", process_name, "Completed", df_reports.count()))
#==================================================================================================================================#
# Stage 5: Almacenamiento de la información                                                                                        #
#==================================================================================================================================#

path = "/opt/workspace/parquet/brz_stage/tbl_brz_reports"

# Si el directorio no existe o está vacío, simplemente guardamos df_reports
if not os.path.exists(path) or not os.listdir(path):
    df_reports.write.mode("overwrite").parquet(path)
    log_data = log_data.union(log_execution("Union de datos", process_name, "Completed", df_reports.count()))
    log_data.write.mode("overwrite").parquet(f"/opt/workspace/parquet/brz_stage/tbl_brz_monitoring/{process_name}_{datetime.now().strftime('%Y%m%d%H%M%S')}")
else:
    # Cargar datos existentes en Parquet
    df_stage = spark.read.parquet(path).cache()

    # Marcar como "inactivo" los registros en df_stage si su id existe en df_reports usando JOIN
    df_stage = df_stage.alias("stage").join(
        df_reports.select("id").alias("reports"),
        on="id",
        how="left"
    ).withColumn(
        "estado_registro_dl",
        when(col("reports.id").isNotNull(), lit(False))  # Si existe en df_reports, poner False
        .otherwise(col("stage.estado_registro_dl").cast("boolean"))  # Mantener el valor actual
    ).select("stage.*")

    # Filtrar registros nuevos que NO existen en df_stage
    df_reports_new = df_reports.alias("new").join(
        df_stage.alias("stage"), 
        on="id", 
        how="leftanti"  # Solo registros que no están en df_stage
    )

    # Unir datos
    df_final = df_stage.unionByName(df_reports_new, allowMissingColumns=True)

    # Ejecutar acción para persistencia en memoria
    df_final.cache()
    df_final.count()

    # Guardar solo si hay cambios
    if df_final.count() > 0:
        print(f"Se almacenarán {df_final.count()} nuevos registros")
        df_final.write.mode("overwrite").parquet(path)
    else:
        print("No hay datos nuevos para almacenar")

    # Liberar memoria
    df_stage.unpersist()
    df_final.unpersist()

    log_data = log_data.union(log_execution("Union de datos", process_name, "Completed", df_final.count()))
    log_data.write.mode("overwrite").parquet(f"/opt/workspace/parquet/brz_stage/tbl_brz_monitoring/{process_name}_{datetime.now().strftime('%Y%m%d%H%M%S')}")

print("Finalizó la extracción y procesamiento de datos")
spark.stop()