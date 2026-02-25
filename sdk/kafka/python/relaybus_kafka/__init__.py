from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Optional, Sequence
import time
import sys
import types

from relaybus_core import Message, OutgoingMessage, decode_envelope, encode_envelope


@dataclass
class KafkaPublisherConfig:
    producer: object
    topic_prefix: str = ""
    admin: Optional[object] = None


@dataclass
class KafkaPublisherConnectConfig:
    brokers: Sequence[str] | str
    topic_prefix: str = ""


class KafkaPublisher:
    def __init__(self, config: KafkaPublisherConfig) -> None:
        if config.producer is None:
            raise ValueError("producer is required")
        self._producer = config.producer
        self._prefix = config.topic_prefix
        self._admin = config.admin
        self._owns_admin = False
        self._ensured_topics: set[str] = set()
        self._raw_producer = None

    @classmethod
    def connect(cls, config: KafkaPublisherConnectConfig) -> "KafkaPublisher":
        _ensure_kafka_vendor_six()
        brokers = config.brokers
        if isinstance(brokers, str):
            broker_list = brokers
        else:
            broker_list = ",".join(brokers)
        from kafka import KafkaProducer
        from kafka.admin import KafkaAdminClient

        producer = KafkaProducer(bootstrap_servers=broker_list)
        admin = KafkaAdminClient(bootstrap_servers=broker_list, client_id="relaybus")
        publisher = cls(
            KafkaPublisherConfig(
                producer=_KafkaPythonProducerAdapter(producer),
                topic_prefix=config.topic_prefix,
                admin=admin,
            )
        )
        publisher._raw_producer = producer
        publisher._owns_admin = True
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
        send = getattr(self._producer, "send", None)
        if not callable(send):
            raise ValueError("producer must define send")
        full_topic = f"{self._prefix}{resolved}"
        self._ensure_topic(full_topic)
        send(full_topic, message.id.encode() if message.id else None, envelope)

    def close(self) -> None:
        if self._raw_producer is not None:
            try:
                self._raw_producer.flush()
            finally:
                self._raw_producer.close()
        if self._admin is not None:
            if self._owns_admin:
                try:
                    self._admin.close()
                except Exception:
                    pass

    def _ensure_topic(self, topic: str) -> None:
        if self._admin is None:
            return
        if topic in self._ensured_topics:
            return
        create_topics = getattr(self._admin, "create_topics", None)
        if not callable(create_topics):
            return
        try:
            from kafka.admin import NewTopic
            from kafka.errors import TopicAlreadyExistsError
        except Exception:
            return
        try:
            create_topics(new_topics=[NewTopic(name=topic, num_partitions=1, replication_factor=1)])
        except TopicAlreadyExistsError:
            pass
        self._ensured_topics.add(topic)


@dataclass
class KafkaSubscriberConfig:
    on_message: Callable[[Message], None]


@dataclass
class KafkaSubscriberConnectConfig:
    brokers: Sequence[str] | str
    on_message: Callable[[Message], None]
    group_id: str = "relaybus"
    max_messages: int = 1
    timeout: float = 30.0
    auto_offset_reset: str = "earliest"


class KafkaSubscriber:
    def __init__(self, config: KafkaSubscriberConfig) -> None:
        self._on_message = config.on_message
        self._consumer = None
        self._max_messages = 1
        self._timeout = 30.0

    @classmethod
    def connect(cls, config: KafkaSubscriberConnectConfig) -> "KafkaSubscriber":
        _ensure_kafka_vendor_six()
        brokers = config.brokers
        if isinstance(brokers, str):
            broker_list = brokers
        else:
            broker_list = ",".join(brokers)
        from kafka import KafkaConsumer

        consumer = KafkaConsumer(
            bootstrap_servers=broker_list,
            group_id=config.group_id,
            auto_offset_reset=config.auto_offset_reset,
            enable_auto_commit=True,
        )
        subscriber = cls(KafkaSubscriberConfig(on_message=config.on_message))
        subscriber._consumer = consumer
        subscriber._max_messages = config.max_messages
        subscriber._timeout = config.timeout
        return subscriber

    def handle_message(self, data: bytes | str) -> None:
        decoded = decode_envelope(data)
        self._on_message(decoded)

    def start(self, topic: str, timeout: Optional[float] = None) -> None:
        if self._consumer is None:
            raise ValueError("consumer is not initialized")
        self._consumer.subscribe([topic])
        deadline = time.time() + (timeout or self._timeout)
        count = 0
        while time.time() < deadline:
            records = self._consumer.poll(timeout_ms=1000)
            if not records:
                continue
            for _tp, messages in records.items():
                for message in messages:
                    self.handle_message(message.value)
                    count += 1
                    if self._max_messages and count >= self._max_messages:
                        self._consumer.close()
                        return
        self._consumer.close()
        if count == 0:
            raise TimeoutError("timeout waiting for message")

    def close(self) -> None:
        if self._consumer is not None:
            self._consumer.close()


def _resolve_topic(argument_topic: str, message_topic: Optional[str]) -> str:
    topic = message_topic or argument_topic
    if not topic:
        raise ValueError("topic is required")
    if argument_topic and message_topic and argument_topic != message_topic:
        raise ValueError(f"topic mismatch: {message_topic} vs {argument_topic}")
    return topic


class _KafkaPythonProducerAdapter:
    def __init__(self, producer: object) -> None:
        self._producer = producer

    def send(self, topic_name: str, key: Optional[bytes], value: bytes) -> None:
        self._producer.send(topic_name, key=key, value=value)
        self._producer.flush()


def _ensure_kafka_vendor_six() -> None:
    try:
        import kafka.vendor.six.moves  # type: ignore
        return
    except Exception:
        pass

    try:
        import six  # type: ignore
    except Exception:
        return

    sys.modules.setdefault("kafka.vendor", types.ModuleType("kafka.vendor"))
    sys.modules.setdefault("kafka.vendor.six", six)
    sys.modules.setdefault("kafka.vendor.six.moves", six.moves)


__all__ = [
    "KafkaPublisher",
    "KafkaPublisherConfig",
    "KafkaPublisherConnectConfig",
    "KafkaSubscriber",
    "KafkaSubscriberConfig",
    "KafkaSubscriberConnectConfig",
]
