#!/usr/bin/env python3
"""Fail early when company CI is using mutable or missing base-image refs."""
import os
import re
import sys

failed = False
for name in ("RUST_TOOLS_IMAGE", "RUST_RUNTIME_IMAGE", "TEST_POSTGRES_IMAGE", "TEST_KAFKA_IMAGE"):
    value = os.environ.get(name, "")
    if not re.fullmatch(r"[^\s@]+@sha256:[a-f0-9]{64}", value):
        print(f"{name} must be an approved registry image pinned by @sha256.", file=sys.stderr)
        failed = True
if failed:
    print("Configure the project variables using README.md#dependency-supply-chain before running CI.", file=sys.stderr)
raise SystemExit(1 if failed else 0)
