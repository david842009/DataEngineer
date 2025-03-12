#==================================================================================================================================#
# Description: Este notebook realiza la extracción de los datos de la API spaceflightnewsapi.net y los guarda en formato parquet en#
#              el directorio                                                     #
# Author:      Fabian David Carreño León                                                                                           #           
# Date:        2025-03-10                                                                                                          #       
# Version:     1.0                                                                                                                 #              
#==================================================================================================================================#

#==================================================================================================================================#
# Stage 0: Importar librerías necesarias                                                                                           #
#==================================================================================================================================#

from pyspark.sql import SparkSession
from pyspark.ml.feature import RegexTokenizer, StopWordsRemover
from pyspark.sql.functions import col, count, year, month, explode, lower, regexp_replace, split, lit, monotonically_increasing_id, regexp_extract

# Expresión regular para extraer solo el dominio principal de la URL
domain_pattern = r"(https?:\/\/[^\/]+)"

#==================================================================================================================================#
# Stage 1: Configurar sesion de Spark con Delta tables y workers                                                                   #
#==================================================================================================================================#

spark = SparkSession.builder.appName("AppAnalisisTendencias").getOrCreate()

#==================================================================================================================================#
# Stage 2: Cargue de datos                                                                                             #
#==================================================================================================================================#

# Cargar datos de la tabla Delta Lake
df_reports = spark.read.parquet("/opt/workspace/parquet/brz_stage/tbl_brz_reports")

# Limpiar texto y extraer palabras clave
df_text = df_reports.select(
    col("id"),
    col("title"),
    col("summary"),
    col("news_site"),
    col("url"),
    col("published_at")
).withColumn("text", lower(regexp_replace(col("title"), "[^a-zA-Z0-9 ]", "")))

#==================================================================================================================================#
# Stage 3: Definicion de funciones Extraccion                                                                                      #
#==================================================================================================================================#

# Tokenizar palabras
tokenizer = RegexTokenizer(inputCol="text", outputCol="words", pattern="\\s+")
df_tokenized = tokenizer.transform(df_text)

# Remover palabras vacías (stopwords)
stopwords_remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
df_filtered = stopwords_remover.transform(df_tokenized)

# Explode para obtener una palabra por fila
df_words = df_filtered.select("id", "news_site", "published_at", explode(col("filtered_words")).alias("word"))

# Agrupar por mes y palabra clave para analizar tendencias
df_trends = df_words.groupBy(
    year("published_at").alias("year"),
    month("published_at").alias("month"),
    "word"
).agg(count("id").alias("count")).orderBy(col("count").desc())

# Agrupar por fuente de noticias más activa
df_reports = df_reports.withColumn("domain_url", regexp_extract(col("url"), domain_pattern, 1))
 
df_sources = df_reports.groupBy("news_site", "domain_url").agg(count("id").alias("count")) \
    .withColumn("source", lit("articles"))

#==================================================================================================================================#
# Stage 5: Almacenamiento de la información                                                                                        #
#==================================================================================================================================#

# Guardar los resultados en Delta Lake (nivel silver)
df_trends = df_trends.withColumn("source", lit("reports"))
df_trends.write.mode("append").option("mergeSchema","true").parquet("/opt/workspace/parquet/slv_stage/tbl_slv_trends")
df_sources.write.mode("append").option("mergeSchema","true").parquet("/opt/workspace/parquet/slv_stage/tbl_slv_sources")
