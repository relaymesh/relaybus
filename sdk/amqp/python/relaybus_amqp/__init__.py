from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Optional, Union
import time

from relaybus_core import Message, OutgoingMessage, decode_envelope, encode_envelope


@dataclass
class Delivery:
    body: Union[bytes, str]


@dataclass
class AmqpSubscriberConfig:
    on_message: Callable[[Message], None]


@dataclass
class AmqpSubscriberConnectConfig:
    url: str
    on_message: Callable[[Message], None]
    exchange: str = ""
    routing_key_template: str = "{topic}"
    queue: Optional[str] = None


@dataclass
class AmqpPublisherConfig:
    channel: object
    exchange: str = ""
    routing_key_template: str = "{topic}"


@dataclass
class AmqpPublisherConnectConfig:
    url: str
    exchange: str = ""
    routing_key_template: str = "{topic}"


class AmqpSubscriber:
    def __init__(self, config: AmqpSubscriberConfig) -> None:
        self._on_message = config.on_message
        self._channel = None
        self._connection = None
        self._exchange = ""
        self._routing_key_template = "{topic}"
        self._queue = None

    @classmethod
    def connect(cls, config: AmqpSubscriberConnectConfig) -> "AmqpSubscriber":
        channel, connection = _connect_channel(config.url)
        subscriber = cls(AmqpSubscriberConfig(on_message=config.on_message))
        subscriber._channel = channel
        subscriber._connection = connection
        subscriber._exchange = config.exchange
        subscriber._routing_key_template = config.routing_key_template
        subscriber._queue = config.queue
        return subscriber

    def handle_delivery(self, delivery: Delivery) -> None:
        message = decode_envelope(delivery.body)
        self._on_message(message)

    def start(self, topic: str, timeout: float = 30.0) -> None:
        if self._channel is None:
            raise ValueError("channel is not initialized")
        queue_name = self._queue or topic
        self._channel.queue_declare(queue=queue_name, durable=False, auto_delete=True)
        if self._exchange:
            routing_key = _build_routing_key(self._routing_key_template, topic)
            self._channel.queue_bind(queue=queue_name, exchange=self._exchange, routing_key=routing_key)

        deadline = time.time() + timeout
        while time.time() < deadline:
            method_frame, _properties, body = self._channel.basic_get(queue=queue_name, auto_ack=False)
            if method_frame:
                self.handle_delivery(Delivery(body=body))
                self._channel.basic_ack(method_frame.delivery_tag)
                return
            time.sleep(0.1)
        raise TimeoutError("timeout waiting for message")

    def close(self) -> None:
        if self._channel is not None:
            try:
                self._channel.close()
            except Exception:
                pass
        if self._connection is not None:
            try:
                self._connection.close()
            except Exception:
                pass


class AmqpPublisher:
    def __init__(self, config: AmqpPublisherConfig) -> None:
        if config.channel is None:
            raise ValueError("channel is required")
        self._channel = config.channel
        self._exchange = config.exchange
        self._routing_key_template = config.routing_key_template
        self._connection = None

    @classmethod
    def connect(cls, config: AmqpPublisherConnectConfig) -> "AmqpPublisher":
        channel, connection = _connect_channel(config.url)
        publisher = cls(
            AmqpPublisherConfig(
                channel=_PikaChannelAdapter(channel),
                exchange=config.exchange,
                routing_key_template=config.routing_key_template,
            )
        )
        publisher._connection = connection
        return publisher

    def publish(self, topic: str, message: OutgoingMessage) -> None:
        resolved = _resolve_topic(topic, message.topic)
        envelope = encode_envelope(
            OutgoingMessage(
                topic=resolved,
                payload=message.payload,
                id=message.id,
                ts=message.ts,
                content_type=message.content_type,
                meta=message.meta,
            )
        )
        routing_key = _build_routing_key(self._routing_key_template, resolved)
        properties = {
            "content_type": "application/json",
            "message_id": message.id,
            "headers": message.meta or None,
        }
        publish = getattr(self._channel, "publish", None)
        if not callable(publish):
            raise ValueError("channel must define publish")
        publish(self._exchange, routing_key, envelope, properties)

    def close(self) -> None:
        if self._connection is not None:
            try:
                self._connection.close()
            except Exception:
                pass


def _resolve_topic(argument_topic: str, message_topic: Optional[str]) -> str:
    topic = message_topic or argument_topic
    if not topic:
        raise ValueError("topic is required")
    if argument_topic and message_topic and argument_topic != message_topic:
        raise ValueError(f"topic mismatch: {message_topic} vs {argument_topic}")
    return topic


def _build_routing_key(template: str, topic: str) -> str:
    if not template:
        return topic
    if "{topic}" in template:
        return template.replace("{topic}", topic)
    return template


class _PikaChannelAdapter:
    def __init__(self, channel: object) -> None:
        self._channel = channel

    def publish(self, exchange: str, routing_key: str, body: bytes, properties: Optional[dict] = None) -> None:
        pika = _require_pika()
        props = pika.BasicProperties(
            content_type=properties.get("content_type") if properties else None,
            message_id=properties.get("message_id") if properties else None,
            headers=properties.get("headers") if properties else None,
        )
        self._channel.basic_publish(exchange=exchange, routing_key=routing_key, body=body, properties=props)


def _connect_channel(url: str) -> tuple[object, object]:
    pika = _require_pika()
    params = pika.URLParameters(url)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    return channel, connection


def _require_pika() -> object:
    try:
        import pika  # type: ignore
    except Exception as exc:  # pragma: no cover - only triggered when pika is missing
        raise RuntimeError("pika is required for AMQP connect") from exc
    return pika


__all__ = [
    "AmqpSubscriber",
    "AmqpSubscriberConfig",
    "AmqpSubscriberConnectConfig",
    "AmqpPublisher",
    "AmqpPublisherConfig",
    "AmqpPublisherConnectConfig",
    "Delivery",
]
