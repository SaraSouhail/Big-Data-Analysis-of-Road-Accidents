from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType

# Schéma pour lire les messages Kafka (adapté à ton CSV)
schema = StructType() \
    .add("ID", StringType()) \
    .add("Severity", IntegerType()) \
    .add("Start_Time", StringType()) \
    .add("City", StringType()) \
    .add("Weather_Condition", StringType()) \
    .add("Temperature(F)", DoubleType()) \
    .add("Humidity(%)", DoubleType()) \
    .add("Visibility(mi)", DoubleType())

# Démarre la session Spark
spark = SparkSession.builder \
    .appName("KafkaAccidentConsumer") \
    .getOrCreate()

# Lit le topic Kafka
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "accidents") \
    .load()

# Le message est dans "value" (bytes → string → JSON)
df_parsed = df_kafka.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*")

# Affiche les résultats en continu
query = df_parsed.select("City", "Severity", "Weather_Condition") \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
