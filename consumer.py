# kafka, insert consumed data to mongo
import json
import logging
import os

from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Kafka configuration
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "news_topic"
KAFKA_GROUP_ID = "comparison_processor"

# Set up MongoDB client
mongo_client = MongoClient(os.getenv("MONGO_URI"))
mongo_db = mongo_client[os.getenv("MONGO_DB")]
mongo_collection = mongo_db[os.getenv("MONGO_COLLECTION")]


def summarize(content):
    # Mock summarize function
    return f"Summary of: {content[:100]}"


def compare(content):
    # Mock compare function
    return f"Comparison of: {content[:100], content[-100:]}"


def process_news(news_data):
    """
    Process the news data.
    This is where you'd implement your specific logic for handling the news.
    """
    logger.info(f"Received news: {news_data}")
    logger.info(f"news type: {type(news_data)}")
    logger.info(f"news keys: {news_data.keys()}")

    # Mock processing
    summary = summarize(news_data["data"])
    comparison = compare(news_data["data"])

    logger.info(summary)
    logger.info(comparison)

    # Prepare data to insert into MongoDB
    processed_data = {
        "title": news_data["title"],
        "summary": summary,
        "comparison": comparison,
        "original": news_data["content"],
    }

    # Insert processed data into MongoDB
    mongo_collection.insert_one(processed_data)
    logger.info(f"Inserted processed news into MongoDB: {news_data['title']}")


def consume_news():
    """
    Consume news data from Kafka and process it.
    """
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BROKER,
            "group.id": KAFKA_GROUP_ID,
            "auto.offset.reset": "earliest",
        }
    )

    consumer.subscribe([KAFKA_TOPIC])

    logger.info(f"Starting to consume from topic: {KAFKA_TOPIC}")

    try:
        while True:
            msg = consumer.poll(1.0)  # Poll for messages

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.info(f"End of partition reached {msg.partition()}")
                elif msg.error():
                    logger.error(f"Error consuming Kafka messages: {msg.error()}")
                    break
            else:
                news_data = json.loads(msg.value().decode("utf-8"))
                process_news(news_data)
    except Exception as e:
        logger.error(f"Error consuming Kafka messages: {e}")
    finally:
        consumer.close()


if __name__ == "__main__":
    consume_news()
