import json
import time

import requests
from dotenv import load_dotenv
import os
from kafka import KafkaProducer

load_dotenv()
OPEN_WEATHER_API = os.getenv("OPENWEATHER_API_KEY")
TOPIC_KAFKA = "weather-raw"
url_base = "https://api.openweathermap.org/data/2.5/weather"

producer = KafkaProducer(
    bootstrap_servers=['kafka:29092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    api_version=(2, 5, 0)
)


def on_success(record_metadata):
    print(f"¡Confirmado en Kafka! Ciudad guardada en el offset {record_metadata.offset}")

def on_error(excp):
    print(f"Error real de Kafka: {excp}")

def get_weather(city, api_key):
    
    parameters = {
        "q": city,
        "appid": api_key,
        "units": "imperial",
    }
    
    try:
        response = requests.get(url_base,params=parameters)

        response.raise_for_status()

        data_weather = response.json()
        producer.send(TOPIC_KAFKA, value=data_weather) \
                .add_callback(on_success) \
                .add_errback(on_error)
        
    except Exception as e:
        print(f"An error has occurred: {e}")
        


cities = [
    "New York",
    "Los Angeles",
    "Chicago",
    "Houston",
    "Phoenix",
    "Philadelphia",
    "San Antonio",
    "San Diego",
    "Dallas",
    "Miami"
]

for city in cities:
    get_weather(city=city,api_key=OPEN_WEATHER_API)
    #1 second pause for rate limiting
    time.sleep(1)

producer.flush()
producer.close()

print("\n--- Ingesting has finalized ---")
