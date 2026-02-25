# relaybus-amqp (Python)

AMQP publisher and subscriber utilities for Relaybus.

## Install

```
pip install relaybus-amqp
```

## Example

Publisher will assert the exchange/queue on first publish (defaults: `exchange_type="topic"`, `queue=topic`).

```python
from relaybus_amqp import (
    AmqpPublisher,
    AmqpPublisherConnectConfig,
    AmqpSubscriber,
    AmqpSubscriberConnectConfig,
)
from relaybus_core import OutgoingMessage

exchange = "relaybus.events"
queue = "relaybus.demo"

publisher = AmqpPublisher.connect(
    AmqpPublisherConnectConfig(
        url="amqp://guest:guest@localhost:5672/",
        exchange=exchange,
        exchange_type="topic",
        queue=queue,
    )
)
publisher.publish("relaybus.demo", OutgoingMessage(topic="relaybus.demo", payload=b"hello"))
publisher.close()

subscriber = AmqpSubscriber.connect(
    AmqpSubscriberConnectConfig(
        url="amqp://guest:guest@localhost:5672/",
        exchange=exchange,
        queue=queue,
        on_message=lambda msg: print(msg.topic, msg.payload),
    )
)
subscriber.start("relaybus.demo")
subscriber.close()
```
