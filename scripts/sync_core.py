#!/usr/bin/env python3
from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PACKAGES = ("amqp", "http", "nats", "kafka")


def sync_typescript(dest: Path, src: Path, dry_run: bool) -> None:
    if dry_run:
        print(f"[dry-run] copy {src} -> {dest}")
        return
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(src, dest)


def sync_python(dest_dir: Path, src_dir: Path, dry_run: bool) -> None:
    if dry_run:
        print(f"[dry-run] copy tree {src_dir} -> {dest_dir}")
        return
    if dest_dir.exists():
        shutil.rmtree(dest_dir)
    shutil.copytree(
        src_dir,
        dest_dir,
        ignore=shutil.ignore_patterns("__pycache__", "*.pyc"),
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--package", action="append", dest="packages")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    packages = args.packages or list(PACKAGES)
    unknown = [pkg for pkg in packages if pkg not in PACKAGES]
    if unknown:
        print(f"unknown package(s): {', '.join(unknown)}", file=sys.stderr)
        return 2

    ts_src = ROOT / "sdk" / "core" / "typescript" / "src" / "index.ts"
    py_src = ROOT / "sdk" / "core" / "python" / "relaybus_core"
    if not ts_src.exists():
        print(f"missing typescript core source: {ts_src}", file=sys.stderr)
        return 1
    if not py_src.exists():
        print(f"missing python core source: {py_src}", file=sys.stderr)
        return 1

    for pkg in packages:
        sync_typescript(
            ROOT / "sdk" / pkg / "typescript" / "src" / "core.ts",
            ts_src,
            args.dry_run,
        )
        sync_python(
            ROOT / "sdk" / pkg / "python" / "relaybus_core",
            py_src,
            args.dry_run,
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
