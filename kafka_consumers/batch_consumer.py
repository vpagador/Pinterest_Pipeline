from kafka import KafkaConsumer
import boto3
import json
import uuid

s3 = boto3.client('s3')

consumer = KafkaConsumer(
    "pinterestPosts",
    bootstrap_servers="localhost:9092",
    value_deserializer= lambda x: json.loads(x.decode("utf-8")),
    api_version=(0,10)
)

if '__main__'==__name__:
    for msg in consumer:
        json_object = json.dumps(msg.value,indent=4)
        file_name = str(uuid.uuid4())
        response = s3.put_object(Body= json_object, 
                        Bucket='pinterest-data-1d58b717-eb31-4af7-bfd6-3f103a337965', 
                        Key=f'pinterest_post-{file_name}.json')
        print(msg.value)