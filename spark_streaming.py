from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Configuration Kafka
KAFKA_SERVER = "kafka:9092"
STORAGE_PATH = "/app/data"  # Chemin de sauvegarde pour Parquet

# Initialisation de la session Spark
spark = SparkSession.builder \
    .appName("KafkaToParquet") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1") \
    .getOrCreate()

# Schéma pour le topic "flights"
flight_schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("data", StructType([
        StructField("id", StringType(), True),
        StructField("icao_24bit", StringType(), True),
        StructField("latitude", StringType(), True),
        StructField("longitude", StringType(), True),
        StructField("heading", StringType(), True),
        StructField("altitude", StringType(), True),
        StructField("ground_speed", StringType(), True),
        StructField("aircraft_code", StringType(), True),
        StructField("registration", StringType(), True),
        StructField("time", TimestampType(), True),  
        StructField("origin_airport_iata", StringType(), True),
        StructField("destination_airport_iata", StringType(), True),
        StructField("number", StringType(), True),
        StructField("airline_iata", StringType(), True),
        StructField("on_ground", StringType(), True),
        StructField("vertical_speed", StringType(), True),
        StructField("callsign", StringType(), True),
        StructField("airline_icao", StringType(), True)
    ]))
])

# Schéma pour le topic "airline"
airline_schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("data", StructType([
        StructField("name", StringType(), True),
        StructField("code", StringType(), True),
        StructField("icao", StringType(), True)
    ]))
])

# Schéma pour le topic "zone"
zone_schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("data", StructType([
        StructField("zone", StringType(), True),
        StructField("subzone", StringType(), True),
        StructField("tl_y", DoubleType(), True),
        StructField("tl_x", DoubleType(), True),
        StructField("br_y", DoubleType(), True),
        StructField("br_x", DoubleType(), True)
    ]))
])
# Schéma pour le topic "airport"
airport_schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("data", StructType([
        StructField("name", StringType(), True),
        StructField("iata", StringType(), True),
        StructField("icao", StringType(), True),
        StructField("altitude", StringType(), True),
        StructField("latitude", StringType(), True),
        StructField("longitude", StringType(), True),


    ]))
])

# Fonction pour lire depuis Kafka et écrire en Parquet avec partitionnement pour "flights"
def kafka_to_parquet_partitioned(topic, schema):
    # Lecture en continu des données depuis Kafka
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_SERVER) \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .load()

    # Conversion de la valeur du message Kafka en colonnes structurées
    df_processed = df.selectExpr("CAST(value AS STRING)") \
                     .select(from_json(col("value"), schema).alias("parsed_data")) \
                     .select("parsed_data.data.*")
    df_processed = df_processed.withColumn("year", year("time")) \
                               .withColumn("month", month("time")) \
                               .withColumn("day", dayofmonth("time")) \
                               .withColumn("hour", hour("time"))

    # Écriture en continu des données dans un répertoire Parquet partitionné
    query = df_processed.writeStream \
        .format("parquet") \
        .option("path", f"{STORAGE_PATH}/{topic}") \
        .option("checkpointLocation", f"{STORAGE_PATH}/{topic}_checkpoint") \
        .partitionBy("year", "month", "day", "hour") \
        .outputMode("append") \
        .start()

    return query

# Fonction pour lire depuis Kafka et écrire en Parquet sans partitionnement pour "airline", "zone", "airport"
def kafka_to_parquet_one_shot(topic, schema):
    # Lecture des données depuis Kafka
    df = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_SERVER) \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .load()

    # Conversion de la valeur du message Kafka en colonnes structurées
    df_processed = df.selectExpr("CAST(value AS STRING)") \
                     .select(from_json(col("value"), schema).alias("parsed_data")) \
                     .select("parsed_data.data.*")

    # Écriture des données dans un répertoire Parquet
    df_processed.write \
        .format("parquet") \
        .mode("overwrite") \
        .option("path", f"{STORAGE_PATH}/{topic}") \
        .save()

# Lancement du flux pour les topics spécifiés
query_flights = kafka_to_parquet_partitioned("flights", flight_schema)

# Lancer les traitements one-shot pour "airline", "zone", et "airport"
kafka_to_parquet_one_shot("airlines", airline_schema)
kafka_to_parquet_one_shot("zones", zone_schema)
kafka_to_parquet_one_shot("airports", airport_schema)

# Attente de la terminaison du flux pour "flights"
query_flights.awaitTermination()

# Arrêt de la session Spark
spark.stop()

