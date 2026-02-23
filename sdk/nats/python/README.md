# relaybus-nats (Python)

NATS publisher and subscriber utilities for Relaybus.

## Install

```
pip install relaybus-nats
```

## Example

```python
from relaybus_core import OutgoingMessage
from relaybus_nats import (
    NatsPublisher,
    NatsPublisherConnectConfig,
    NatsSubscriber,
    NatsSubscriberConnectConfig,
)

publisher = NatsPublisher.connect(NatsPublisherConnectConfig(url="nats://localhost:4222"))
publisher.publish("alpha", OutgoingMessage(topic="alpha", payload=b"hello"))

subscriber = NatsSubscriber.connect(
    NatsSubscriberConnectConfig(
        url="nats://localhost:4222",
        subject_prefix="relaybus",
        on_message=lambda msg: print(msg.topic, msg.payload),
    )
)
subscriber.start("alpha")
```
