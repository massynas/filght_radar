from FlightRadar24 import FlightRadar24API
from kafka import KafkaProducer
import json
import datetime
import time
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Configuration Kafka
KAFKA_SERVER = "kafka:9092"
FLIGHT_TOPIC = "flights"
AIRLINE_TOPIC = "airlines"
AIRPORT_TOPIC = "airports"
ZONE_TOPIC = "zones"

# Configuration logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_flights(fr_api):
    logging.info("Fetching flight data")
    flights = fr_api.get_flights()
    flights_to_dict = [
        {
            "id": flight.id,
            "icao_24bit": flight.icao_24bit,
            "latitude": flight.latitude,
            "longitude": flight.longitude,
            "heading": flight.heading,
            "altitude": flight.altitude,
            "ground_speed": flight.ground_speed,
            "aircraft_code": flight.aircraft_code,
            "registration": flight.registration,
            "time": flight.time,
            "origin_airport_iata": flight.origin_airport_iata,
            "destination_airport_iata": flight.destination_airport_iata,
            "number": flight.number,
            "airline_iata": flight.airline_iata,
            "on_ground": flight.on_ground,
            "vertical_speed": flight.vertical_speed,
            "callsign": flight.callsign,
            "airline_icao": flight.airline_icao
        }
        for flight in flights
    ]
    logging.info(f"Fetched {len(flights_to_dict)} flight records")
    return flights_to_dict

def get_airlines(fr_api):
    logging.info("Fetching airline data")
    airlines = fr_api.get_airlines()
    airlines_to_dict = [
        {"name": airline["Name"], "code": airline["Code"], "icao": airline["ICAO"]}
        for airline in airlines
    ]
    logging.info(f"Fetched {len(airlines_to_dict)} airline records")
    return airlines_to_dict

def get_airports(fr_api):
    logging.info("Fetching airport data")
    airports = fr_api.get_airports()
    airports_to_dict = [
        {
            "name": airport.name,
            "iata": airport.iata,
            "icao": airport.icao,
            "altitude": airport.altitude,
            "latitude": airport.latitude,
            "longitude": airport.longitude
        }
        for airport in airports
    ]
    logging.info(f"Fetched {len(airports_to_dict)} airport records")
    return airports_to_dict

def extract_zones(zones):
    zone_list = []
    for zone_name, zone_data in zones.items():
        if "subzones" in zone_data:
            for subzone_name, subzone_data in zone_data["subzones"].items():
                zone_list.append({
                    "zone": zone_name,
                    "subzone": subzone_name,
                    "tl_y": subzone_data.get("tl_y"),
                    "tl_x": subzone_data.get("tl_x"),
                    "br_y": subzone_data.get("br_y"),
                    "br_x": subzone_data.get("br_x")
                })
        else:
            zone_list.append({
                "zone": zone_name,
                "subzone": None,
                "tl_y": zone_data.get("tl_y"),
                "tl_x": zone_data.get("tl_x"),
                "br_y": zone_data.get("br_y"),
                "br_x": zone_data.get("br_x")
            })
    return zone_list

def get_zones(fr_api):
    logging.info("Fetching zone data")
    zones = fr_api.get_zones()
    zones_to_dict = extract_zones(zones)
    logging.info(f"Fetched {len(zones_to_dict)} zone records")
    return zones_to_dict

def send_to_kafka(producer, data, topic):
    timestamp = datetime.datetime.now().isoformat()
    for record in data:
        message = {"timestamp": timestamp, "data": record}
        producer.send(topic, value=message)
    logging.info(f"Sent {len(data)} records to Kafka topic '{topic}'")


if __name__ == "__main__":
    logging.info("Starting Flight Data to Kafka script")

    # Initialize FlightRadar24API
    fr_api = FlightRadar24API()

    # Initialize Spark session
    spark = SparkSession.builder.appName("FlightRadar24ToParquet").getOrCreate()

    # Kafka configuration
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    # Send zones data once
    zones = get_zones(fr_api)
    send_to_kafka(producer, zones, ZONE_TOPIC)

    # Get and send airlines data
    airlines = get_airlines(fr_api)
    send_to_kafka(producer, airlines, AIRLINE_TOPIC)

    # Get and send airports data
    airports = get_airports(fr_api)
    send_to_kafka(producer, airports, AIRPORT_TOPIC)

    # Function to fetch and send flights data
    def fetch_and_send_data():
        flights = get_flights(fr_api)
        send_to_kafka(producer, flights, FLIGHT_TOPIC)

    # Fetch and send flights data every minute
    while True:
        fetch_and_send_data()
        logging.info("Sleeping for 1 minute")
        time.sleep(60)  # Wait for 1 minute

    # Clean up
    producer.flush()
    producer.close()
    spark.stop()
    logging.info("Stopped Flight Data to Kafka script")

