#==================================================================================================================================#
# Description: Este notebook realiza la extracción de los datos de la API spaceflightnewsapi.net y los guarda en formato parquet en#
#              el directorio                                                  #
# Author:      Fabian David Carreño León                                                                                           #           
# Date:        2025-03-10                                                                                                          #       
# Version:     1.0                                                                                                                 #              
#==================================================================================================================================#

# ================================================================
# Stage 0. Preparación ambiente
# ================================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, array_max, array_position, lower, regexp_replace, expr, udf, lit
from pyspark.sql.types import ArrayType, StringType
from pyspark.ml.feature import Tokenizer, StopWordsRemover, CountVectorizer, IDF
from pyspark.sql.types import StructType, StructField
from pyspark.ml.clustering import LDA
from pyspark.ml.linalg import DenseVector, VectorUDT
from pyspark.sql.types import ArrayType, DoubleType
import spacy
from nltk.corpus import stopwords
from nltk.sentiment import SentimentIntensityAnalyzer
import os
os.environ["SPARK_LOCAL_IP"] = "192.168.1.95"

# Inicializar Spark
spark = SparkSession.builder.appName("BlogsContentAnalysis").getOrCreate()

# Cargar datos desde Parquet
df = spark.read.parquet("/opt/workspace/parquet/brz_stage/tbl_brz_blogs").select("id", "title", "summary")

# ================================================================
# Stage 1. Extracción de palabras clave con TF-IDF
# ================================================================

# Limpiar texto
df_clean = df.withColumn("clean_summary", lower(col("summary")))
df_clean = df_clean.withColumn("clean_summary", regexp_replace("clean_summary", "[^a-zA-Z\s]", ""))

# Tokenización
tokenizer = Tokenizer(inputCol="clean_summary", outputCol="words")
df_words = tokenizer.transform(df_clean)

# Remover Stopwords
stopwords_list = stopwords.words("english")

remover = StopWordsRemover(inputCol="words", outputCol="filtered_words", stopWords=stopwords_list)
df_filtered = remover.transform(df_words)

# TF-IDF
cv = CountVectorizer(inputCol="filtered_words", outputCol="raw_features", vocabSize=1000)
cv_model = cv.fit(df_filtered)
df_features = cv_model.transform(df_filtered)

idf = IDF(inputCol="raw_features", outputCol="features")
idf_model = idf.fit(df_features)
df_tfidf = idf_model.transform(df_features)

# ================================================================
# Stage 2. Identificación de entidades (Personas, Empresas, Lugares)
# ================================================================

nlp = spacy.load("en_core_web_md")

def extract_entities(text):
    if text is None or text.strip() == "":
        return [], [], []
    doc = nlp(text)
    persons = list(set(ent.text for ent in doc.ents if ent.label_ == "PERSON"))
    organizations = list(set(ent.text for ent in doc.ents if ent.label_ == "ORG"))
    locations = list(set(ent.text for ent in doc.ents if ent.label_ == "GPE"))
    return persons, organizations, locations

entity_schema = StructType([
    StructField("persons", ArrayType(StringType()), True),
    StructField("organizations", ArrayType(StringType()), True),
    StructField("locations", ArrayType(StringType()), True)
])

extract_entities_udf = udf(lambda text: extract_entities(text), entity_schema)

df_entities = df_clean.withColumn("entities", extract_entities_udf(col("summary")))
df_entities = df_entities.select("id", "title", "summary", "clean_summary", 
                                 col("entities.persons"), 
                                 col("entities.organizations"), 
                                 col("entities.locations"))

# ================================================================
# Stage 3. Clasificación de artículos en temas con LDA
# ================================================================

# Inicializar el analizador de sentimiento
sia = SentimentIntensityAnalyzer()

def get_sentiment_score(text):
    if text is None or text.strip() == "":
        return 0.0
    return sia.polarity_scores(text)["compound"]

sentiment_udf = udf(get_sentiment_score, DoubleType())

df_entities = df_entities.withColumn("sentiment_score", sentiment_udf(col("summary")))

lda = LDA(k=5, maxIter=10, featuresCol="features", topicDistributionCol="topicDistribution")
lda_model = lda.fit(df_tfidf)
df_topics = lda_model.transform(df_tfidf)

vector_to_array_udf = udf(lambda v: v.toArray().tolist() if isinstance(v, DenseVector) else [], ArrayType(DoubleType()))
df_topics = df_topics.withColumn("topic_array", vector_to_array_udf(col("topicDistribution")))
df_topics = df_topics.withColumn("topic", expr("array_position(topic_array, array_max(topic_array)) - 1"))
df_topics = df_topics.drop("topic_array")

# ================================================================
# Stage 4. Guardar resultados
# ================================================================

df_final = df_entities.join(df_topics.select("id", "topicDistribution","topic"), on="id", how="left")
df_final = df_final.withColumn("source", lit("blogs"))
df_final.write.mode("append").option("mergeSchema","true").parquet("/opt/workspace/parquet/slv_stage/tbl_slv_analysis")

print("Análisis de contenido finalizado")
spark.stop()
