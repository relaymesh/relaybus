from __future__ import annotations

import base64
import json
from pathlib import Path

from datetime import datetime

from relaybus_amqp import AmqpSubscriber, AmqpSubscriberConfig, Delivery

ROOT = Path(__file__).resolve().parents[4]
SAMPLE = ROOT / "spec" / "corpus" / "samples" / "sample1.json"
EXPECTED = ROOT / "spec" / "corpus" / "expected" / "sample1.json"


def test_subscriber_invokes_handler():
    expected = json.loads(EXPECTED.read_text())
    captured = {}

    def handler(msg):
        captured["msg"] = msg

    subscriber = AmqpSubscriber(AmqpSubscriberConfig(on_message=handler))
    subscriber.handle_delivery(Delivery(body=SAMPLE.read_bytes()))

    msg = captured["msg"]
    assert msg.id == expected["id"]
    assert msg.topic == expected["topic"]
    assert msg.ts.isoformat().replace("+00:00", "Z") == normalize_ts(expected["ts"])
    assert msg.content_type == expected["content_type"]
    assert msg.meta == expected["meta"]
    assert base64.b64encode(msg.payload).decode() == expected["payload_bytes_b64"]


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
