# utf-8
from  flightradar24 import FlightRadar24API
from kafka import KafkaProducer
import json
import datetime

def get_flight_data():
    fr_api = FlightRadar24API()
    flights = fr_api.get_flights()
    airports = fr_api.get_airports()
    airlines = fr_api.get_airlines()
    zones = fr_api.get_zones()

    return {"flights": flights, "airports": airports, "airlines": airlines, "zones": zones}

def send_to_kafka(data, topic):
    producer = KafkaProducer(
        bootstrap_servers='kafka:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    producer.send(topic, data)
    producer.flush()

if __name__ == "__main__":
    data = get_flight_data()
    print(data)
    timestamp = datetime.datetime.now().isoformat()
    for data_type, records in data.items():
        send_to_kafka({"timestamp": timestamp, "data": records}, f"flight_data_{data_type}")
