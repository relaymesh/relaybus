from __future__ import annotations

import os
import sys

from relaybus_core import OutgoingMessage
from relaybus_kafka import (
    KafkaPublisher,
    KafkaPublisherConnectConfig,
    KafkaSubscriber,
    KafkaSubscriberConnectConfig,
)


def main() -> None:
    mode = sys.argv[1] if len(sys.argv) > 1 else "sub"
    broker = os.getenv("KAFKA_BROKER", "localhost:29092")
    topic = os.getenv("TOPIC", "relaybus.alpha")
    ensure_topic(broker, topic)

    if mode == "sub":
        subscriber = KafkaSubscriber.connect(
            KafkaSubscriberConnectConfig(
                brokers=broker,
                on_message=lambda msg: print(
                    f"received id={msg.id} topic={msg.topic} payload={msg.payload.decode()}"
                ),
                group_id="relaybus-e2e",
                max_messages=1,
                timeout=30.0,
            )
        )
        subscriber.start(topic)
        return

    publisher = KafkaPublisher.connect(
        KafkaPublisherConnectConfig(brokers=broker, topic_prefix="")
    )
    publisher.publish(
        topic,
        OutgoingMessage(
            topic=topic, payload=b"hello from python", id="py-1", meta={"lang": "py"}
        ),
    )
    publisher.close()
    print("published")


def ensure_topic(broker: str, topic: str) -> None:
    try:
        from kafka.admin import KafkaAdminClient, NewTopic
        from kafka.errors import TopicAlreadyExistsError
    except Exception:
        return

    admin = KafkaAdminClient(bootstrap_servers=broker, client_id="relaybus-e2e")
    try:
        admin.create_topics(
            new_topics=[NewTopic(name=topic, num_partitions=1, replication_factor=1)]
        )
    except TopicAlreadyExistsError:
        pass
    finally:
        admin.close()


if __name__ == "__main__":
    main()
