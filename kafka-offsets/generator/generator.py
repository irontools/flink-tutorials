import json
import os
import random
import time
import uuid

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

BROKER = "redpanda:9092"
TOPIC = "loadtest.json"
MAX_MESSAGES = int(os.environ.get("MAX_MESSAGES", "100"))

JOB_TITLES = [
    "Software Engineer",
    "Data Scientist",
    "Product Manager",
    "DevOps Engineer",
    "UX Designer",
    "QA Engineer",
    "Solutions Architect",
    "Technical Writer",
]


def ensure_topic_exists():
    admin = AdminClient({"bootstrap.servers": BROKER})
    futures = admin.create_topics([NewTopic(TOPIC, num_partitions=3, replication_factor=1)])
    for topic, future in futures.items():
        try:
            future.result()
            print(f"Created topic: {topic}")
        except Exception as e:
            if "TOPIC_ALREADY_EXISTS" in str(e):
                print(f"Topic already exists: {topic}")
            else:
                raise


def generate_message():
    return {
        "createdAt": int(time.time() * 1000),
        "latitude": round(random.uniform(-90.0, 90.0), 6),
        "jobTitle": random.choice(JOB_TITLES),
        "randomIdentifier": random.randint(0, 2**63 - 1),
        "active": random.choice([True, False]),
        "id": str(uuid.uuid4()),
    }


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")


def main():
    # Wait for Redpanda to be ready
    time.sleep(5)

    ensure_topic_exists()

    producer = Producer({"bootstrap.servers": BROKER})

    print(f"Producing {MAX_MESSAGES} messages to {TOPIC}...")
    for i in range(MAX_MESSAGES):
        message = generate_message()
        producer.produce(
            TOPIC,
            key=message["id"],
            value=json.dumps(message).encode("utf-8"),
            callback=delivery_report,
        )
        producer.poll(0)
        time.sleep(0.5)

    producer.flush()
    print(f"Done. Produced {MAX_MESSAGES} messages.")


if __name__ == "__main__":
    main()
