from __future__ import annotations

import base64
import json
from datetime import datetime, timezone

import pytest

from relaybus_core import OutgoingMessage, encode_envelope


def test_encode_defaults_with_injected_clock():
    fixed = datetime(2024, 1, 1, tzinfo=timezone.utc)
    data = encode_envelope(
        OutgoingMessage(topic="alpha", payload=b"hi"),
        now=lambda: fixed,
        id_generator=lambda: "id-123",
    )
    parsed = json.loads(data.decode())
    assert parsed["v"] == "v1"
    assert parsed["id"] == "id-123"
    assert parsed["topic"] == "alpha"
    assert parsed["ts"] == fixed.isoformat().replace("+00:00", "Z")
    assert parsed["content_type"] == "application/octet-stream"
    assert parsed["payload_b64"] == base64.b64encode(b"hi").decode()
    assert parsed["meta"] == {}


def test_encode_requires_topic():
    with pytest.raises(ValueError, match="topic is required"):
        encode_envelope(OutgoingMessage(topic="", payload=b"hi"))


def test_encode_requires_payload():
    with pytest.raises(ValueError, match="payload is required"):
        encode_envelope(OutgoingMessage(topic="alpha", payload=None))


def test_encode_rejects_invalid_meta():
    with pytest.raises(ValueError, match="invalid meta"):
        encode_envelope(
            OutgoingMessage(topic="alpha", payload=b"hi", meta={"k": 123})
        )
