from __future__ import annotations

import json

import pytest

from relaybus_core import OutgoingMessage, decode_envelope
from relaybus_kafka import (
    KafkaPublisher,
    KafkaPublisherConfig,
    KafkaSubscriber,
    KafkaSubscriberConfig,
)


def test_kafka_publisher_sends_envelope():
    calls = []

    class FakeProducer:
        def send(self, topic, key, value):
            calls.append({"topic": topic, "key": key, "value": value})

    publisher = KafkaPublisher(
        KafkaPublisherConfig(producer=FakeProducer(), topic_prefix="rb-")
    )
    publisher.publish("alpha", OutgoingMessage(topic="alpha", payload=b"hi", id="id-1"))

    assert len(calls) == 1
    call = calls[0]
    assert call["topic"] == "rb-alpha"
    assert call["key"] == b"id-1"
    decoded = decode_envelope(call["value"])
    assert decoded.id == "id-1"


def test_kafka_subscriber_decodes():
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
        seen["topic"] = msg.topic

    subscriber = KafkaSubscriber(KafkaSubscriberConfig(on_message=handler))
    subscriber.handle_message(payload)

    assert seen["topic"] == "alpha"


def test_kafka_subscriber_rejects_missing_fields():
    subscriber = KafkaSubscriber(KafkaSubscriberConfig(on_message=lambda msg: None))
    with pytest.raises(ValueError, match="invalid v"):
        subscriber.handle_message("{}")


def test_kafka_subscriber_start_uses_topic_prefix():
    subscribed = []

    class FakeRecord:
        value = b'{"v":"v1","id":"id","topic":"alpha","ts":"2024-01-01T00:00:00Z","content_type":"text/plain","payload_b64":"aGVsbG8=","meta":{}}'

    class FakeConsumer:
        def subscribe(self, topics):
            subscribed.extend(topics)

        def poll(self, timeout_ms=1000):
            return {"tp": [FakeRecord()]}

        def close(self):
            pass

    seen = []
    subscriber = KafkaSubscriber(
        KafkaSubscriberConfig(on_message=lambda msg: seen.append(msg.id))
    )
    subscriber._consumer = FakeConsumer()
    subscriber._prefix = "rb-"
    subscriber._max_messages = 1
    subscriber._timeout = 1

    subscriber.start("alpha")

    assert subscribed == ["rb-alpha"]
    assert seen == ["id"]


def test_kafka_prefix_without_delimiter_uses_dot_join():
    calls = []

    class FakeProducer:
        def send(self, topic, key, value):
            calls.append(topic)

    publisher = KafkaPublisher(
        KafkaPublisherConfig(producer=FakeProducer(), topic_prefix="rb")
    )
    publisher.publish("alpha", OutgoingMessage(topic="alpha", payload=b"hi"))

    assert calls == ["rb.alpha"]
