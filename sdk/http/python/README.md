# relaybus-http (Python)

HTTP publisher and subscriber utilities for Relaybus.

## Install

```
pip install relaybus-http
```

## Example

```python
from relaybus_core import OutgoingMessage
from relaybus_http import (
    HttpPublisher,
    HttpPublisherConnectConfig,
    HttpSubscriber,
    HttpSubscriberConfig,
    HttpSubscriberListenConfig,
)

subscriber = HttpSubscriber(
    HttpSubscriberConfig(
        on_message=lambda msg: print(msg.topic, msg.payload),
    )
)
subscriber.listen(HttpSubscriberListenConfig(port=8088))

publisher = HttpPublisher.connect(
    HttpPublisherConnectConfig(endpoint="http://localhost:8088/{topic}")
)
publisher.publish("relaybus.demo", OutgoingMessage(topic="relaybus.demo", payload=b"hello"))
```
