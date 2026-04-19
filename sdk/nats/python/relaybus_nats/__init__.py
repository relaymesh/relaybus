from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Optional
import asyncio

from relaybus_core import (
    Message,
    OutgoingMessage,
    decode_envelope,
    encode_envelope,
    join_prefixed_topic,
    resolve_topic_or_raise,
)


@dataclass
class NatsPublisherConfig:
    client: object
    subject_prefix: str = ""


@dataclass
class NatsPublisherConnectConfig:
    url: str
    subject_prefix: str = ""


class NatsPublisher:
    def __init__(self, config: NatsPublisherConfig) -> None:
        if config.client is None:
            raise ValueError("client is required")
        self._client = config.client
        self._prefix = config.subject_prefix

    @classmethod
    def connect(cls, config: NatsPublisherConnectConfig) -> "NatsPublisher":
        client = _SyncNatsClient(config.url)
        return cls(
            NatsPublisherConfig(client=client, subject_prefix=config.subject_prefix)
        )

    def publish(self, topic: str, message: OutgoingMessage) -> None:
        resolved = resolve_topic_or_raise(topic, message.topic)
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
        subject = join_prefixed_topic(self._prefix, resolved)
        publish = getattr(self._client, "publish", None)
        if not callable(publish):
            raise ValueError("client must define publish")
        publish(subject, envelope)


@dataclass
class NatsSubscriberConfig:
    on_message: Callable[[Message], None]


@dataclass
class NatsSubscriberConnectConfig:
    url: str
    on_message: Callable[[Message], None]
    subject_prefix: str = ""
    timeout: float = 30.0


class NatsSubscriber:
    def __init__(self, config: NatsSubscriberConfig) -> None:
        self._on_message = config.on_message
        self._url: Optional[str] = None
        self._prefix: str = ""
        self._timeout: float = 30.0

    @classmethod
    def connect(cls, config: NatsSubscriberConnectConfig) -> "NatsSubscriber":
        subscriber = cls(NatsSubscriberConfig(on_message=config.on_message))
        subscriber._url = config.url
        subscriber._prefix = config.subject_prefix
        subscriber._timeout = config.timeout
        return subscriber

    def handle_message(self, data: bytes | str) -> None:
        decoded = decode_envelope(data)
        self._on_message(decoded)

    def start(self, topic: str, timeout: Optional[float] = None) -> None:
        if not self._url:
            raise ValueError("url is not configured")
        subject = join_prefixed_topic(self._prefix, topic)
        data = asyncio.run(
            _subscribe_once(self._url, subject, timeout or self._timeout)
        )
        self.handle_message(data)


class _SyncNatsClient:
    def __init__(self, url: str) -> None:
        self._url = url

    def publish(self, subject: str, data: bytes) -> None:
        asyncio.run(_publish_once(self._url, subject, data))


async def _publish_once(url: str, subject: str, data: bytes) -> None:
    from nats.aio.client import Client as NATS

    nc = NATS()
    await nc.connect(servers=[url])
    await nc.publish(subject, data)
    await nc.flush()
    await nc.close()


async def _subscribe_once(url: str, subject: str, timeout: float) -> bytes:
    from nats.aio.client import Client as NATS

    nc = NATS()
    await nc.connect(servers=[url])
    loop = asyncio.get_running_loop()
    future: asyncio.Future[bytes] = loop.create_future()

    async def handler(msg):
        if not future.done():
            future.set_result(msg.data)

    sub = await nc.subscribe(subject, cb=handler)
    try:
        return await asyncio.wait_for(future, timeout=timeout)
    finally:
        try:
            await sub.unsubscribe()
        except Exception:
            pass
        await nc.drain()


__all__ = [
    "NatsPublisher",
    "NatsPublisherConfig",
    "NatsPublisherConnectConfig",
    "NatsSubscriber",
    "NatsSubscriberConfig",
    "NatsSubscriberConnectConfig",
]
