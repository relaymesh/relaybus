# relaybus-core (Python)

Core envelope encode/decode utilities for Relaybus.

## Install

```
pip install relaybus-core
```

## Example

```python
from relaybus_core import OutgoingMessage, decode_envelope, encode_envelope

encoded = encode_envelope(
    OutgoingMessage(topic="alpha", payload=b"hello", meta={"source": "py"})
)
decoded = decode_envelope(encoded)

print(decoded.topic, decoded.payload)
```
