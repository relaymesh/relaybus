# relaybus-amqp (Python)

AMQP publisher and subscriber utilities for Relaybus.

## Install

```
pip install relaybus-amqp
```

## Example

```python
from relaybus_amqp import (
    AmqpPublisher,
    AmqpPublisherConnectConfig,
    AmqpSubscriber,
    AmqpSubscriberConnectConfig,
)
from relaybus_core import OutgoingMessage

publisher = AmqpPublisher.connect(
    AmqpPublisherConnectConfig(url="amqp://guest:guest@localhost:5672/")
)
publisher.publish("relaybus.demo", OutgoingMessage(topic="relaybus.demo", payload=b"hello"))
publisher.close()

subscriber = AmqpSubscriber.connect(
    AmqpSubscriberConnectConfig(
        url="amqp://guest:guest@localhost:5672/",
        on_message=lambda msg: print(msg.topic, msg.payload),
    )
)
subscriber.start("relaybus.demo")
subscriber.close()
```
