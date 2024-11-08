# -*- coding: utf-8 -*-
"""
Created on Wed Oct 16 14:36:24 2024

@author: cassi
"""
# import relevant libraries
import time
from kafka import KafkaProducer
from kafka import KafkaConsumer
import json

# define broker url
message_broker_url = "localhost:9092"

# create the publisher class, connecting it to the kafka message broker
class Publisher:
    def __init__(self, broker, topics):
        self.producer = KafkaProducer(
            bootstrap_servers=broker,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.topics = topics
    
    def generate_data(self):
        while True:
            data = self.collect_data(topic)
            self.publish_to_broker(topic, data)
            time.sleep(2)
    
    def collect_data(self, topic):
        """Simulates data collection for each topic"""
        if topic == "energy-production":
             return {"energy_output": 500}  # Example data
        elif topic == "equipment-status":
             return {"status": "operational"}  # Example data
        elif topic == "weather-data":
             return {"temperature": 24.5, "wind_speed": 12.3}  # Example data
        elif topic == "grid-status":
             return {"grid_load": 75}  # Example data
        else:
             return {"default_data": "value"}  # Default data
    
    def publish_to_broker(self, topic, data):
        # Send data to the broker
        self.producer.send(topic, data)
        print(f"Data published to topic '{topic}': {data}")
        
# create the subscriber class, connecting it to the kafka message broker
class Subscriber:
    def __init__(self, broker, topic):
        self.broker = broker
        self.topic = topic
    
    def subscribe(self):
        while True:
            messages = self.pull_data_from_broker(self.topic)
            for message in messages:
                self.process_message(message)
            time.sleep(1)
    
    def pull_data_from_broker(self, topic):
        # Simulate pulling data from the broker
        return ["message1", "message2"]

    def process_message(self, message):
        print(f"Processing message: {message}")
        
# Instantiate the publishers for the renewable energy streaming data system
solar_panel = Publisher(broker=message_broker_url, topics=["energy-production", "equipment-status"])
wind_turbine = Publisher(broker=message_broker_url, topics=["energy-production", "equipment-status"])
weather_station = Publisher(broker=message_broker_url, topics=["weather-data", "equipment-status"])
grid_database = Publisher(broker=message_broker_url, topics=["energy-production", "grid-status"])
weather_database = Publisher(broker=message_broker_url, topics=["weather-data"])

solar_panel.generate_data()

# Instantiate the subscribers for the renewable energy streaming data system

# Create a visual of the streaming data

