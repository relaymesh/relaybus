from __future__ import annotations

import json

import pytest

from relaybus_core import OutgoingMessage, decode_envelope
from relaybus_nats import NatsPublisher, NatsPublisherConfig, NatsSubscriber, NatsSubscriberConfig


def test_nats_publisher_uses_subject_prefix():
    calls = []

    class FakeClient:
        def publish(self, subject, data):
            calls.append({"subject": subject, "data": data})

    publisher = NatsPublisher(NatsPublisherConfig(client=FakeClient(), subject_prefix="events"))
    publisher.publish("alpha", OutgoingMessage(topic="alpha", payload=b"hi", id="id-1"))

    assert len(calls) == 1
    assert calls[0]["subject"] == "events.alpha"
    decoded = decode_envelope(calls[0]["data"])
    assert decoded.id == "id-1"


def test_nats_subscriber_decodes_message():
    payload = json.dumps(
        {
            "v": "v1",
            "id": "id",
            "topic": "alpha",
            "ts": "2024-01-01T00:00:00Z",
            "content_type": "text/plain",
            "payload_b64": "aGVsbG8=",
            "meta": {},
        }
    ).encode()

    seen = {}

    def handler(msg):
        seen["id"] = msg.id

    subscriber = NatsSubscriber(NatsSubscriberConfig(on_message=handler))
    subscriber.handle_message(payload)

    assert seen["id"] == "id"


def test_nats_subscriber_rejects_invalid_base64():
    subscriber = NatsSubscriber(NatsSubscriberConfig(on_message=lambda msg: None))
    payload = json.dumps(
        {
            "v": "v1",
            "id": "id",
            "topic": "alpha",
            "ts": "2024-01-01T00:00:00Z",
            "content_type": "text/plain",
            "payload_b64": "???",
            "meta": {},
        }
    )
    with pytest.raises(ValueError, match="invalid payload_b64"):
        subscriber.handle_message(payload)
