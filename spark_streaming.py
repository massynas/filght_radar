from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, dayofmonth, hour, current_timestamp
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json

KAFKA_SERVER = "kafka:9092"
TOPICS = ["flight_data_flights", "flight_data_airports", "flight_data_airlines", "flight_data_zones"]
STORAGE_PATH = "/data/flight_data/"

spark = SparkSession.builder.appName("FlightDataStreaming").getOrCreate()
ssc = StreamingContext(spark.sparkContext, 10)  # 10-second batches

kafka_streams = [KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": KAFKA_SERVER}) for topic in TOPICS]

def process(rdd, data_type):
    if not rdd.isEmpty():
        df = spark.read.json(rdd.map(lambda x: x[1]))
        df = df.withColumn("year", year(current_timestamp())) \
               .withColumn("month", month(current_timestamp())) \
               .withColumn("day", dayofmonth(current_timestamp())) \
               .withColumn("hour", hour(current_timestamp()))
        df.write.partitionBy("year", "month", "day", "hour").parquet(f"{STORAGE_PATH}{data_type}")

for kafka_stream, data_type in zip(kafka_streams, ["flights", "airports", "airlines", "zones"]):
    kafka_stream.foreachRDD(lambda rdd: process(rdd, data_type))

ssc.start()
ssc.awaitTermination()
