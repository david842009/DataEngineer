from pyspark.sql import SparkSession
import logging
import sys

# Configurar el nivel de logs
logging.getLogger("py4j").setLevel(logging.ERROR)

# Deshabilitar logs de Spark
spark = SparkSession.builder.appName("TestSpark").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Crear un DataFrame de prueba
#path = "/Users/fabianengineer/space-launch-data/parquet/brz_stage/tbl_brz_monitoring/Extract_API_Articles_20250311212258"
path1 = "/Users/fabianengineer/space-launch-data/parquet/brz_stage/tbl_brz_articles"
#path2 = "/Users/fabianengineer/space-launch-data/parquet/brz_stage/tbl_brz_blogs"
#path3 = "/Users/fabianengineer/space-launch-data/parquet/brz_stage/tbl_brz_reports"
path4 = "/Users/fabianengineer/space-launch-data/parquet/slv_stage/tbl_slv_analysis"
path5 = "/Users/fabianengineer/space-launch-data/parquet/slv_stage/tbl_slv_trends"
path6 = "/Users/fabianengineer/space-launch-data/parquet/slv_stage/tbl_slv_sources"
df1 = spark.read.parquet(path4)
df2 = spark.read.parquet(path5)
df3 = spark.read.parquet(path6)

#df_final1 = df1.count()
#df_final2 = df2.count()
#df_final3 = df3.count()
#print("Cantidad de datos de articles ",df_final1)
#print("Cantidad de datos de blogs ",df_final2)
#print("Cantidad de datos de reports ",df_final3)

# Detener la sesión de Spark
#/Lectura de la tabla de analisis de articulos/
df1.printSchema()
df1.select("*").show(10)
df2.printSchema()
df2.select("*").show(10)
df3.printSchema()
df3.select("*").show(10)

spark.stop()