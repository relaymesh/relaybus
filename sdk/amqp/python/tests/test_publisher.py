from __future__ import annotations

from relaybus_amqp import AmqpPublisher, AmqpPublisherConfig
from relaybus_core import OutgoingMessage, decode_envelope


def test_publish_sends_envelope_with_routing_key():
    calls = []

    class FakeChannel:
        def publish(self, exchange, routing_key, body, properties=None):
            calls.append(
                {
                    "exchange": exchange,
                    "routing_key": routing_key,
                    "body": body,
                    "properties": properties,
                }
            )

    publisher = AmqpPublisher(
        AmqpPublisherConfig(
            channel=FakeChannel(), exchange="ex", routing_key_template="events.{topic}"
        )
    )

    publisher.publish(
        "alpha",
        OutgoingMessage(topic="alpha", payload=b"hi", id="id-1", meta={"k": "v"}),
    )

    assert len(calls) == 1
    call = calls[0]
    assert call["exchange"] == "ex"
    assert call["routing_key"] == "events.alpha"
    decoded = decode_envelope(call["body"])
    assert decoded.id == "id-1"
    assert decoded.topic == "alpha"
    assert decoded.payload == b"hi"


def test_publish_rejects_topic_mismatch():
    class FakeChannel:
        def publish(self, exchange, routing_key, body, properties=None):
            pass

    publisher = AmqpPublisher(AmqpPublisherConfig(channel=FakeChannel()))
    message = OutgoingMessage(topic="beta", payload=b"hi")
    try:
        publisher.publish("alpha", message)
        raise AssertionError("expected ValueError")
    except ValueError as exc:
        assert "topic mismatch" in str(exc)
