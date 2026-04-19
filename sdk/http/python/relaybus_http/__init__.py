from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Dict, Optional, Union
from urllib.parse import quote, urlparse
import http.client
import hmac
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

from relaybus_core import (
    Message,
    OutgoingMessage,
    decode_envelope,
    encode_envelope,
    resolve_topic_or_raise,
)


@dataclass
class HttpPublisherConfig:
    endpoint: str
    doer: Callable[
        [str, str, Dict[str, str], bytes], Union[int, tuple[int, Union[bytes, str]]]
    ]
    headers: Optional[Dict[str, str]] = None
    idempotency_header: str = "Idempotency-Key"


@dataclass
class HttpPublisherConnectConfig:
    endpoint: str
    headers: Optional[Dict[str, str]] = None
    idempotency_header: str = "Idempotency-Key"
    timeout: float = 10.0


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
        resolved = resolve_topic_or_raise(topic, message.topic)
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
        response = self._doer("POST", url, headers, body)
        status, error_body = _normalize_response(response)
        if status < 200 or status >= 300:
            detail = f"http status {status}"
            if error_body:
                detail = f"{detail}: {error_body}"
            raise ValueError(detail)

    @classmethod
    def connect(cls, config: HttpPublisherConnectConfig) -> "HttpPublisher":
        parsed_endpoint = urlparse(config.endpoint)
        if (
            parsed_endpoint.scheme not in {"http", "https"}
            or not parsed_endpoint.hostname
        ):
            raise ValueError("endpoint must be a valid http(s) URL with host")

        def doer(
            method: str, url: str, headers: Dict[str, str], body: bytes
        ) -> Union[int, tuple[int, Union[bytes, str]]]:
            parsed = urlparse(url)
            if parsed.scheme == "https":
                conn = http.client.HTTPSConnection(
                    parsed.hostname, parsed.port, timeout=config.timeout
                )
            else:
                conn = http.client.HTTPConnection(
                    parsed.hostname, parsed.port, timeout=config.timeout
                )
            path = parsed.path or "/"
            if parsed.query:
                path = f"{path}?{parsed.query}"
            try:
                conn.request(method, path, body=body, headers=headers)
                response = conn.getresponse()
                payload = response.read()
                return response.status, payload
            finally:
                conn.close()

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
    max_body_bytes: int = 1_048_576
    auth_header: str = "Authorization"
    auth_token: Optional[str] = None


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
                if config.auth_token is not None:
                    provided = (self.headers.get(config.auth_header) or "").strip()
                    expected = config.auth_token.strip()
                    if not hmac.compare_digest(provided, expected):
                        self.send_response(401)
                        self.end_headers()
                        return

                content_type = self.headers.get("Content-Type", "")
                if content_type and not content_type.startswith("application/json"):
                    self.send_response(415)
                    self.end_headers()
                    return

                length_header = self.headers.get("Content-Length")
                if length_header is None:
                    self.send_response(411)
                    self.end_headers()
                    return
                try:
                    length = int(length_header)
                except ValueError:
                    self.send_response(400)
                    self.end_headers()
                    return
                if length < 0:
                    self.send_response(400)
                    self.end_headers()
                    return
                if length > config.max_body_bytes:
                    self.send_response(413)
                    self.end_headers()
                    return

                body = self.rfile.read(length)
                try:
                    self.server.subscriber.handle(body)
                    self.send_response(204)
                    stop_event.set()
                except Exception:
                    self.send_response(400)
                self.end_headers()

            def do_GET(self):  # noqa: N802
                self.send_response(405)
                self.end_headers()

            def do_PUT(self):  # noqa: N802
                self.send_response(405)
                self.end_headers()

            def do_DELETE(self):  # noqa: N802
                self.send_response(405)
                self.end_headers()

            def do_PATCH(self):  # noqa: N802
                self.send_response(405)
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


def _build_endpoint(endpoint: str, topic: str) -> str:
    if "{topic}" in endpoint:
        return endpoint.replace("{topic}", quote(topic))
    if not topic:
        return endpoint
    return endpoint.rstrip("/") + "/" + quote(topic)


def _normalize_response(
    value: Union[int, tuple[int, Union[bytes, str]]],
) -> tuple[int, str]:
    if isinstance(value, tuple):
        status, body = value
    else:
        status, body = value, ""
    if isinstance(body, bytes):
        body = body.decode("utf-8", errors="replace")
    body = body.strip()
    if len(body) > 200:
        body = body[:200]
    return status, body


__all__ = [
    "HttpPublisher",
    "HttpPublisherConfig",
    "HttpPublisherConnectConfig",
    "HttpSubscriber",
    "HttpSubscriberConfig",
    "HttpSubscriberListenConfig",
]
