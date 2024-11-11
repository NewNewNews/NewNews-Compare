import json
import os
import re

from confluent_kafka import Consumer, KafkaError
from confluent_kafka.schema_registry.protobuf import ProtobufDeserializer
from confluent_kafka.serialization import (
    StringDeserializer,
    SerializationContext,
    MessageField,
)
from dotenv import load_dotenv
from openai import OpenAI

from protos import news_message_pb2, compare_pb2  # generated from .proto
from database import ComparisonDatabase

class NewsEvent: 
    def __init__(self, id, date):
        self.id = id
        self.date = date
        self.content = dict()
        
    def add_content(self, source, content):
        self.content[source] = content
        
    def __str__(self):
        return f"NewsEvent(id={self.id}, date={self.date}, content={self.content})"
    
    def __repr__(self):
        return str(self)
    
    def create_comparison(self):
        print(f"Comparing news event {self}")
        
        system_prompt = """
        You are a helpful assistant that can compare news from multiple sources. 
        You will highlight similarities, differences between sources only. 
        Output in json as Thai only doesn't answer as English. ```json{'similarities': [], 'differences': []}```"
        """.replace("\n", " ").strip()

        news_contents = "\n".join([f"[{publisher}]\n{content}" for publisher, content in self.content.items()])

        XAI_API_KEY = os.getenv("XAI_API_KEY")
        client = OpenAI(
            api_key=XAI_API_KEY,
            base_url="https://api.x.ai/v1",
        )

        completion = client.chat.completions.create(
            model="grok-beta",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": news_contents},
            ],
        )

        print(completion.usage)
        # print(completion.choices[0].message)

        # Extract the JSON part using regex
        json_str = re.search(r'```json\n({.*?})\n```', completion.choices[0].message.content, re.DOTALL).group(1)

        # Convert the JSON string to a Python dictionary
        comparison = json.loads(json_str)
        
        return comparison

    
def main():
    # schema_registry_conf = {
    #     "url": "http://localhost:8081"
    # }  # Change to your schema registry URL
    # schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    protobuf_deserializer = ProtobufDeserializer(
        news_message_pb2.NewsMessage,
        # schema_registry_client,
        conf={"use.deprecated.format": True},
    )
    string_deserializer = StringDeserializer("utf_8")

    consumer_conf = {
        "bootstrap.servers": os.environ.get("KAFKA_BROKER", "localhost:9092"),
        "group.id": "news_group",
        "auto.offset.reset": "earliest",
    }

    consumer = Consumer(consumer_conf)
    consumer.subscribe(["news_event_topic"])
    
    db = ComparisonDatabase(os.getenv("MONGODB_URI"), os.getenv("MONGODB_DB"), os.getenv("MONGODB_COLLECTION"))
    
    print("Consuming messages...")
    event = None
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            if event is not None:
                comparison = event.create_comparison()
                db.save_comparison(event_id, news_message.date, comparison)
                event = None
            continue

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(msg.error())
                break

        key = string_deserializer(
            msg.key(), SerializationContext(msg.topic(), MessageField.KEY)
        )
        news_message = protobuf_deserializer(
            msg.value(), SerializationContext(msg.topic(), MessageField.VALUE)
        )
        
        if news_message is not None:
            print(f"Consumed record with key: {key}")
        
        event_id = key.split("_")[0]
        
        if int(event_id) == -1:
            continue   
            
        if event is None:
            event = NewsEvent(event_id, news_message.date)
        
        if event_id != event.id:
            comparison = event.create_comparison()
            db.save_comparison(event.id, news_message.date, comparison)
            event = NewsEvent(event_id, news_message.date)
            
        event.add_content(news_message.publisher, news_message.data)

    consumer.close()


if __name__ == "__main__":
    load_dotenv()
    main()
