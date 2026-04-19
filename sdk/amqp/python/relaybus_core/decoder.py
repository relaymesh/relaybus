from __future__ import annotations

import base64
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Optional, Union
import uuid


@dataclass
class Message:
    v: str
    id: str
    topic: str
    ts: datetime
    content_type: str
    payload: bytes
    meta: Dict[str, str]


@dataclass
class OutgoingMessage:
    topic: str
    payload: bytes
    id: Optional[str] = None
    ts: Optional[datetime] = None
    content_type: Optional[str] = None
    meta: Optional[Dict[str, str]] = None


def _parse_ts(value: str) -> datetime:
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    if "." in value:
        main, rest = value.split(".", 1)
        frac = rest
        tz = ""
        for idx, ch in enumerate(rest):
            if ch in "+-":
                frac = rest[:idx]
                tz = rest[idx:]
                break
        if len(frac) > 6:
            frac = frac[:6]
        if frac:
            value = f"{main}.{frac}{tz}"
        else:
            value = f"{main}{tz}"
    ts = datetime.fromisoformat(value)
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts


def _require_str(obj: Dict[str, Any], field: str) -> str:
    value = obj.get(field)
    if not isinstance(value, str) or value == "":
        raise ValueError(f"invalid {field}")
    return value


def decode_envelope(data: Union[bytes, str]) -> Message:
    raw = data.decode("utf-8") if isinstance(data, (bytes, bytearray)) else data
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError("invalid json") from exc

    if not isinstance(parsed, dict):
        raise ValueError("invalid envelope")

    if parsed.get("v") != "v1":
        raise ValueError("invalid v")

    msg_id = _require_str(parsed, "id")
    topic = _require_str(parsed, "topic")
    ts_raw = _require_str(parsed, "ts")
    content_type = _require_str(parsed, "content_type")

    payload_b64 = parsed.get("payload_b64")
    if not isinstance(payload_b64, str):
        raise ValueError("invalid payload_b64")
    try:
        payload = base64.b64decode(payload_b64, validate=True)
    except Exception as exc:
        raise ValueError("invalid payload_b64") from exc

    meta = parsed.get("meta")
    if not isinstance(meta, dict):
        raise ValueError("invalid meta")
    for value in meta.values():
        if not isinstance(value, str):
            raise ValueError("invalid meta")

    ts = _parse_ts(ts_raw)

    return Message(
        v="v1",
        id=msg_id,
        topic=topic,
        ts=ts,
        content_type=content_type,
        payload=payload,
        meta=meta,
    )


def encode_envelope(
    message: OutgoingMessage,
    *,
    now: Optional[Callable[[], datetime]] = None,
    id_generator: Optional[Callable[[], str]] = None,
) -> bytes:
    if message is None:
        raise ValueError("message is required")
    if not message.topic:
        raise ValueError("topic is required")
    if message.payload is None:
        raise ValueError("payload is required")

    now_func = now or (lambda: datetime.now(timezone.utc))
    id_func = id_generator or (lambda: str(uuid.uuid4()))

    msg_id = message.id or id_func()
    if not msg_id:
        raise ValueError("id is required")

    ts = message.ts or now_func()
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)

    content_type = message.content_type or "application/octet-stream"
    meta = message.meta or {}
    if not isinstance(meta, dict):
        raise ValueError("invalid meta")
    for value in meta.values():
        if not isinstance(value, str):
            raise ValueError("invalid meta")

    payload_b64 = base64.b64encode(message.payload).decode()
    env = {
        "v": "v1",
        "id": msg_id,
        "topic": message.topic,
        "ts": ts.isoformat().replace("+00:00", "Z"),
        "content_type": content_type,
        "payload_b64": payload_b64,
        "meta": meta,
    }
    return json.dumps(env).encode("utf-8")


def resolve_topic_or_raise(argument_topic: str, message_topic: Optional[str]) -> str:
    topic = message_topic or argument_topic
    if not topic:
        raise ValueError("topic is required")
    if argument_topic and message_topic and argument_topic != message_topic:
        raise ValueError(f"topic mismatch: {message_topic} vs {argument_topic}")
    return topic


def join_prefixed_topic(prefix: str, topic: str) -> str:
    if not prefix:
        return topic
    if prefix.endswith((".", "_", "-")):
        return f"{prefix}{topic}"
    return f"{prefix}.{topic}"
