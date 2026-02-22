from __future__ import annotations

import os
import sys

from relaybus_core import OutgoingMessage
from relaybus_http import (
    HttpPublisher,
    HttpPublisherConnectConfig,
    HttpSubscriber,
    HttpSubscriberConfig,
    HttpSubscriberListenConfig,
)


def main() -> None:
    mode = sys.argv[1] if len(sys.argv) > 1 else "sub"
    port = int(os.getenv("HTTP_PORT", "8088"))
    endpoint = os.getenv("HTTP_ENDPOINT", f"http://localhost:{port}/{{topic}}")
    topic = os.getenv("TOPIC", "relaybus.alpha")

    if mode == "sub":
        subscriber = HttpSubscriber(
            HttpSubscriberConfig(
                on_message=lambda msg: print(
                    f"received id={msg.id} topic={msg.topic} payload={msg.payload.decode()}"
                )
            )
        )
        subscriber.listen(HttpSubscriberListenConfig(port=port, timeout=30.0))
        return

    publisher = HttpPublisher.connect(HttpPublisherConnectConfig(endpoint=endpoint))
    publisher.publish(topic, OutgoingMessage(topic=topic, payload=b"hello from python", meta={"lang": "py"}))
    print("published")


if __name__ == "__main__":
    main()
