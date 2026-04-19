#!/usr/bin/env python3
from __future__ import annotations

import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]

SYNC_TARGETS = [
    "sdk/amqp/typescript/src/core.ts",
    "sdk/http/typescript/src/core.ts",
    "sdk/nats/typescript/src/core.ts",
    "sdk/kafka/typescript/src/core.ts",
    "sdk/amqp/python/relaybus_core",
    "sdk/http/python/relaybus_core",
    "sdk/nats/python/relaybus_core",
    "sdk/kafka/python/relaybus_core",
]


def run(cmd: list[str]) -> int:
    completed = subprocess.run(cmd, cwd=ROOT)
    return completed.returncode


def main() -> int:
    if run([sys.executable, "scripts/sync_core.py"]) != 0:
        return 1
    return run(["git", "diff", "--exit-code", "--", *SYNC_TARGETS])


if __name__ == "__main__":
    raise SystemExit(main())
