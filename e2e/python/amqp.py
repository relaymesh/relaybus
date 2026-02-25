from __future__ import annotations

import os
import sys

from relaybus_amqp import (
    AmqpPublisher,
    AmqpPublisherConnectConfig,
    AmqpSubscriber,
    AmqpSubscriberConnectConfig,
)
from relaybus_core import OutgoingMessage


def main() -> None:
    mode = sys.argv[1] if len(sys.argv) > 1 else "sub"
    url = os.getenv("AMQP_URL", "amqp://guest:guest@localhost:5672/")
    topic = os.getenv("TOPIC", "relaybus.alpha")
    exchange = os.getenv("AMQP_EXCHANGE", "relaybus.events")
    exchange_type = os.getenv("AMQP_EXCHANGE_TYPE", "topic")

    if mode == "sub":
        subscriber = AmqpSubscriber.connect(
            AmqpSubscriberConnectConfig(
                url=url,
                exchange=exchange,
                exchange_type=exchange_type,
                on_message=lambda msg: print(
                    f"received id={msg.id} topic={msg.topic} payload={msg.payload.decode()}"
                ),
                queue=topic,
            )
        )
        subscriber.start(topic)
        subscriber.close()
        return

    publisher = AmqpPublisher.connect(
        AmqpPublisherConnectConfig(url=url, exchange=exchange, exchange_type=exchange_type)
    )
    publisher.publish(topic, OutgoingMessage(topic=topic, payload=b"hello from python", meta={"lang": "py"}))
    publisher.close()
    print("published")


if __name__ == "__main__":
    main()
