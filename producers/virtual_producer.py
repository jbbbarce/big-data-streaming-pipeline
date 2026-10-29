"""Simulated traffic sensor that publishes one vehicle-count reading per second to Kafka.

prep_producers.sh copies this file into ten producers, each with its own sensor_id.
"""
from datetime import datetime
from json import dumps
import random
import time

import time_uuid
from kafka import KafkaProducer

KAFKA_TOPIC_NAME_CONS = "counts"
KAFKA_BOOTSTRAP_SERVERS_CONS = "127.0.0.1:9092"

if __name__ == "__main__":
    print("Kafka Producer Application Started ... ")

    kafka_producer_obj = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS_CONS,
        value_serializer=lambda x: dumps(x).encode("utf-8"),
    )

    for i in range(1, 3601):
        date_today = datetime.now()
        print("Preparing message: " + str(i))

        car = random.randint(0, 4)
        bus = random.randint(0, 2)
        truck = random.randint(0, 2)
        jeepney = random.randint(0, 2)
        bike = random.randint(0, 5)
        tryke = random.randint(0, 3)
        others = random.randint(0, 2)
        total = car + bus + truck + jeepney + bike + tryke + others

        message = {}
        message["timeuuid_id"] = str(time_uuid.utctime())
        message["lgu_code"] = "1200"
        message["sensor_id"] = "sensor_01"
        message["date_saved"] = date_today.strftime("%m/%d/%Y")
        message["time_saved"] = date_today.strftime("%X")
        message["total"] = total
        message["car"] = car
        message["bus"] = bus
        message["truck"] = truck
        message["jeepney"] = jeepney
        message["bike"] = bike
        message["tryke"] = tryke
        message["others"] = others

        print("Message: ", message)
        kafka_producer_obj.send(KAFKA_TOPIC_NAME_CONS, message)
        time.sleep(1)

    kafka_producer_obj.flush()
    print("Kafka Producer Application Completed. ")
