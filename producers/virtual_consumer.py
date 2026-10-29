# -*- coding: utf-8 -*-
"""Prints every message on the 'counts' topic, to check that the producers are reaching Kafka."""
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "counts",
    bootstrap_servers=["127.0.0.1:9092"],
    auto_offset_reset="latest",
    enable_auto_commit=True,
)

for message in consumer:
    print(message.value)
