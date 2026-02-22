from __future__ import annotations

import os
import sys

from relaybus_core import OutgoingMessage
from relaybus_nats import (
    NatsPublisher,
    NatsPublisherConnectConfig,
    NatsSubscriber,
    NatsSubscriberConnectConfig,
)


def main() -> None:
    mode = sys.argv[1] if len(sys.argv) > 1 else "sub"
    url = os.getenv("NATS_URL", "nats://localhost:4222")
    prefix = os.getenv("NATS_PREFIX", "relaybus")
    topic = os.getenv("TOPIC", "alpha")

    if mode == "sub":
        subscriber = NatsSubscriber.connect(
            NatsSubscriberConnectConfig(
                url=url,
                on_message=lambda msg: print(
                    f"received id={msg.id} topic={msg.topic} payload={msg.payload.decode()}"
                ),
                subject_prefix=prefix,
                timeout=30.0,
            )
        )
        subscriber.start(topic)
        return

    publisher = NatsPublisher.connect(NatsPublisherConnectConfig(url=url, subject_prefix=prefix))
    publisher.publish(topic, OutgoingMessage(topic=topic, payload=b"hello from python", meta={"lang": "py"}))
    print("published")


if __name__ == "__main__":
    main()
