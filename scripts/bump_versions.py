#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]

TS_PACKAGES = [
    ROOT / "sdk" / "core" / "typescript" / "package.json",
    ROOT / "sdk" / "amqp" / "typescript" / "package.json",
    ROOT / "sdk" / "http" / "typescript" / "package.json",
    ROOT / "sdk" / "nats" / "typescript" / "package.json",
    ROOT / "sdk" / "kafka" / "typescript" / "package.json",
]

PY_PACKAGES = [
    ROOT / "sdk" / "core" / "python" / "pyproject.toml",
    ROOT / "sdk" / "amqp" / "python" / "pyproject.toml",
    ROOT / "sdk" / "http" / "python" / "pyproject.toml",
    ROOT / "sdk" / "nats" / "python" / "pyproject.toml",
    ROOT / "sdk" / "kafka" / "python" / "pyproject.toml",
]


def update_file(path: Path, pattern: str, replacement: str, dry_run: bool) -> None:
    if not path.exists():
        raise FileNotFoundError(f"missing file: {path}")
    text = path.read_text()
    updated, count = re.subn(pattern, replacement, text, count=1, flags=re.MULTILINE)
    if count != 1:
        raise ValueError(f"expected 1 match in {path}, found {count}")
    if dry_run:
        return
    path.write_text(updated)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--version", required=True)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    json_pattern = r'"version"\s*:\s*"[^"]+"'
    json_replacement = f'"version": "{args.version}"'
    toml_pattern = r'^version\s*=\s*"[^"]+"'
    toml_replacement = f'version = "{args.version}"'

    for path in TS_PACKAGES:
        update_file(path, json_pattern, json_replacement, args.dry_run)
    for path in PY_PACKAGES:
        update_file(path, toml_pattern, toml_replacement, args.dry_run)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
