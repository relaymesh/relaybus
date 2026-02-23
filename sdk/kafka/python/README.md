# relaybus-kafka (Python)

Kafka publisher and subscriber utilities for Relaybus.

## Install

```
pip install relaybus-kafka
```

## Example

```python
from relaybus_core import OutgoingMessage
from relaybus_kafka import (
    KafkaPublisher,
    KafkaPublisherConnectConfig,
    KafkaSubscriber,
    KafkaSubscriberConnectConfig,
)

publisher = KafkaPublisher.connect(
    KafkaPublisherConnectConfig(brokers="localhost:29092")
)
publisher.publish("relaybus.demo", OutgoingMessage(topic="relaybus.demo", payload=b"hello"))
publisher.close()

subscriber = KafkaSubscriber.connect(
    KafkaSubscriberConnectConfig(
        brokers="localhost:29092",
        group_id="relaybus",
        on_message=lambda msg: print(msg.topic, msg.payload),
    )
)
subscriber.start("relaybus.demo")
```
