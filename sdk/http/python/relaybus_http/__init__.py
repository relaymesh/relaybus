from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Dict, Optional
from urllib.parse import quote, urlparse
import http.client
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

from relaybus_core import Message, OutgoingMessage, decode_envelope, encode_envelope


@dataclass
class HttpPublisherConfig:
    endpoint: str
    doer: Callable[[str, str, Dict[str, str], bytes], int]
    headers: Optional[Dict[str, str]] = None
    idempotency_header: str = "Idempotency-Key"


@dataclass
class HttpPublisherConnectConfig:
    endpoint: str
    headers: Optional[Dict[str, str]] = None
    idempotency_header: str = "Idempotency-Key"


class HttpPublisher:
    def __init__(self, config: HttpPublisherConfig) -> None:
        if not config.endpoint:
            raise ValueError("endpoint is required")
        if config.doer is None:
            raise ValueError("doer is required")
        self._endpoint = config.endpoint
        self._doer = config.doer
        self._headers = config.headers or {}
        self._idempotency_header = config.idempotency_header

    def publish(self, topic: str, message: OutgoingMessage) -> None:
        resolved = _resolve_topic(topic, message.topic)
        body = encode_envelope(
            OutgoingMessage(
                topic=resolved,
                payload=message.payload,
                id=message.id,
                ts=message.ts,
                content_type=message.content_type,
                meta=message.meta,
            )
        )
        url = _build_endpoint(self._endpoint, resolved)
        headers = {"Content-Type": "application/json", **self._headers}
        if message.id:
            headers[self._idempotency_header] = message.id
        status = self._doer("POST", url, headers, body)
        if status < 200 or status >= 300:
            raise ValueError(f"http status {status}")

    @classmethod
    def connect(cls, config: HttpPublisherConnectConfig) -> "HttpPublisher":
        def doer(method: str, url: str, headers: Dict[str, str], body: bytes) -> int:
            parsed = urlparse(url)
            if parsed.scheme == "https":
                conn = http.client.HTTPSConnection(parsed.hostname, parsed.port)
            else:
                conn = http.client.HTTPConnection(parsed.hostname, parsed.port)
            path = parsed.path or "/"
            conn.request(method, path, body=body, headers=headers)
            response = conn.getresponse()
            response.read()
            status = response.status
            conn.close()
            return status

        return cls(
            HttpPublisherConfig(
                endpoint=config.endpoint,
                doer=doer,
                headers=config.headers,
                idempotency_header=config.idempotency_header,
            )
        )


@dataclass
class HttpSubscriberConfig:
    on_message: Callable[[Message], None]


@dataclass
class HttpSubscriberListenConfig:
    port: int
    host: str = ""
    timeout: float = 30.0


class HttpSubscriber:
    def __init__(self, config: HttpSubscriberConfig) -> None:
        self._on_message = config.on_message

    def handle(self, body: bytes | str) -> None:
        decoded = decode_envelope(body)
        self._on_message(decoded)

    def listen(self, config: HttpSubscriberListenConfig) -> None:
        stop_event = threading.Event()

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):  # noqa: N802
                length = int(self.headers.get("Content-Length", "0"))
                body = self.rfile.read(length)
                try:
                    self.server.subscriber.handle(body)
                    self.send_response(204)
                    stop_event.set()
                except Exception:
                    self.send_response(400)
                self.end_headers()

        server = HTTPServer((config.host, config.port), Handler)
        server.subscriber = self

        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        if not stop_event.wait(timeout=config.timeout):
            server.shutdown()
            server.server_close()
            thread.join(timeout=1)
            raise TimeoutError("timeout waiting for message")
        server.shutdown()
        server.server_close()
        thread.join(timeout=1)


def _resolve_topic(argument_topic: str, message_topic: Optional[str]) -> str:
    topic = message_topic or argument_topic
    if not topic:
        raise ValueError("topic is required")
    if argument_topic and message_topic and argument_topic != message_topic:
        raise ValueError(f"topic mismatch: {message_topic} vs {argument_topic}")
    return topic


def _build_endpoint(endpoint: str, topic: str) -> str:
    if "{topic}" in endpoint:
        return endpoint.replace("{topic}", quote(topic))
    if not topic:
        return endpoint
    return endpoint.rstrip("/") + "/" + quote(topic)


__all__ = [
    "HttpPublisher",
    "HttpPublisherConfig",
    "HttpPublisherConnectConfig",
    "HttpSubscriber",
    "HttpSubscriberConfig",
    "HttpSubscriberListenConfig",
]
