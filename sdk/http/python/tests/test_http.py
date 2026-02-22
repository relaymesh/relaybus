from __future__ import annotations

import json

import pytest

from relaybus_core import OutgoingMessage, decode_envelope
from relaybus_http import HttpPublisher, HttpPublisherConfig, HttpSubscriber, HttpSubscriberConfig


def test_http_publisher_posts_envelope():
    calls = []

    def doer(method, url, headers, body):
        calls.append({"method": method, "url": url, "headers": headers, "body": body})
        return 204

    publisher = HttpPublisher(
        HttpPublisherConfig(endpoint="https://example.test/{topic}", doer=doer)
    )
    publisher.publish("alpha", OutgoingMessage(topic="alpha", payload=b"hi", id="id-1"))

    assert len(calls) == 1
    call = calls[0]
    assert call["url"] == "https://example.test/alpha"
    assert call["headers"]["Content-Type"] == "application/json"
    assert call["headers"]["Idempotency-Key"] == "id-1"
    decoded = decode_envelope(call["body"])
    assert decoded.id == "id-1"


def test_http_publisher_rejects_non_2xx():
    def doer(method, url, headers, body):
        return 500

    publisher = HttpPublisher(
        HttpPublisherConfig(endpoint="https://example.test/{topic}", doer=doer)
    )
    with pytest.raises(ValueError, match="http status 500"):
        publisher.publish("alpha", OutgoingMessage(topic="alpha", payload=b"hi"))


def test_http_subscriber_decodes_body():
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

    subscriber = HttpSubscriber(HttpSubscriberConfig(on_message=handler))
    subscriber.handle(payload)

    assert seen["id"] == "id"


def test_http_subscriber_rejects_invalid_json():
    subscriber = HttpSubscriber(HttpSubscriberConfig(on_message=lambda msg: None))
    with pytest.raises(ValueError, match="invalid json"):
        subscriber.handle("{")
