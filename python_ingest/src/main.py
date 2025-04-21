import os
import json
import logging

from confluent_kafka import Producer, KafkaException, KafkaError
from python_ingest.src.helper import default_value

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
logger = logging.getLogger(__name__)


def delivery_report(err, msg):
    """Callback invoked on message delivery success or failure"""
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        pass


def ingest_to_kafka():
    logger.info("Ingesting data to Kafka")

    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    topic = os.getenv("INGEST_TOPIC")
    file_path = os.getenv("FILE_TO_READ")
    max_messages = int(os.getenv("MAX_MESSAGES", 2))

    logger.info(f"Connecting to Kafka broker: {bootstrap}, topic: {topic}")

    producer = Producer({
        "bootstrap.servers": bootstrap,
        "client.id": "test_nico",
        "compression.type": "gzip"
    })

    messages_produced = 0
    with open(file_path, "r", encoding="utf-8") as f:
        for i, line in enumerate(f, start=1):
            if i > max_messages:
                logger.info(f"Reached max of {max_messages} messages, stopping ingest.")
                break

            try:
                record = json.loads(line)
                payload = json.dumps(record, default=default_value).encode("utf-8")
                producer.produce(
                    topic=topic,
                    value=payload,
                    callback=delivery_report
                )
                messages_produced += 1

            except KafkaException as e:
                err = e.args[0]
                if err.code() == KafkaError.MSG_SIZE_TOO_LARGE:
                    logger.warning(f"Skipping message {i}: size too large ({len(payload)} bytes)")
                    continue
                else:
                    raise

            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON on line {i}: {e}")
                continue

            if i % 100 == 0:
                producer.poll(0)

            if i % 1000 == 0:
                logger.info(f"Sent {i} messages to Kafka")

    producer.poll(0)

    try:
        remaining = producer.flush(timeout=30)
        if remaining > 0:
            logger.warning(f"{remaining} messages were not delivered within timeout period")
        else:
            logger.info(f"Successfully delivered all {messages_produced} messages")
    except KafkaException as e:
        logger.error(f"Error on flush: {e}")
    else:
        logger.info("Finished ingesting data to Kafka")


if __name__ == "__main__":
    ingest_to_kafka()