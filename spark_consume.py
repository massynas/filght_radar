from pyspark.sql import SparkSession

STORAGE_PATH = "/data/flight_data/"

spark = SparkSession.builder.appName("FlightDataConsumption").getOrCreate()

def consume_data(data_type):
    df = spark.read.parquet(f"{STORAGE_PATH}{data_type}")
    df.show()

if __name__ == "__main__":
    for data_type in ["flights", "airports", "airlines", "zones"]:
        consume_data(data_type)
