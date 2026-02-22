from __future__ import annotations

import base64
import json
from pathlib import Path

import pytest

from datetime import datetime

from relaybus_core import decode_envelope

ROOT = Path(__file__).resolve().parents[4]
SAMPLES = ROOT / "spec" / "corpus" / "samples"
EXPECTED = ROOT / "spec" / "corpus" / "expected"


def test_decode_corpus_samples():
    for sample_path in SAMPLES.glob("*.json"):
        expected_path = EXPECTED / sample_path.name
        decoded = decode_envelope(sample_path.read_bytes())
        expected = json.loads(expected_path.read_text())

        assert decoded.v == expected["v"]
        assert decoded.id == expected["id"]
        assert decoded.topic == expected["topic"]
        assert decoded.ts.isoformat().replace("+00:00", "Z") == normalize_ts(
            expected["ts"]
        )
        assert decoded.content_type == expected["content_type"]
        assert decoded.meta == expected["meta"]
        assert base64.b64encode(decoded.payload).decode() == expected["payload_bytes_b64"]


def test_decode_invalid_json():
    with pytest.raises(ValueError, match="invalid json"):
        decode_envelope("{")


def test_decode_missing_fields():
    with pytest.raises(ValueError, match="invalid id"):
        decode_envelope(json.dumps({"v": "v1"}))


def test_decode_invalid_base64():
    sample = {
        "v": "v1",
        "id": "id",
        "topic": "t",
        "ts": "2024-01-01T00:00:00Z",
        "content_type": "text/plain",
        "payload_b64": "???",
        "meta": {},
    }
    with pytest.raises(ValueError, match="invalid payload_b64"):
        decode_envelope(json.dumps(sample))


def normalize_ts(value: str) -> str:
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
    return datetime.fromisoformat(value).isoformat().replace("+00:00", "Z")
