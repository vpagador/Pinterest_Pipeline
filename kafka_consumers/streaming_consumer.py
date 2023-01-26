from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "pinterestPosts",
    bootstrap_servers="localhost:9092",
    value_deserializer= lambda x: json.loads(x.decode("utf-8")),
    api_version=(0,10)
)

if '__main__'==__name__:
    for msg in consumer:
        print(msg.value)