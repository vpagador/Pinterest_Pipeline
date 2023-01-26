from fastapi import FastAPI
from pydantic import BaseModel
import uvicorn
import json
from kafka import KafkaProducer

app = FastAPI()

producer = KafkaProducer(
    bootstrap_servers = "localhost:9092",
    value_serializer = lambda x: json.dumps(x).encode("utf-8"),
    api_version =(0,10)
)

class Data(BaseModel):
    category: str
    index: int
    unique_id: str
    title: str
    description: str
    follower_count: str
    tag_list: str
    is_image_or_video: str
    image_src: str
    downloaded: int
    save_location: str


@app.post("/pin/")
def get_db_row(item: Data):
    data = dict(item)
    producer.send("pinterestPosts", value = data)
    producer.flush()


if __name__ == '__main__':
    uvicorn.run("project_pin_API:app", host="localhost", port=8000)
