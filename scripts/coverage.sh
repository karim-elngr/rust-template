#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p coverage
# The dependency-fetch step has finished; test/build children need no token.
unset CARGO_REGISTRIES_COMPANY_TOKEN
# Stable line coverage includes unit + HTTP/gRPC + real PostgreSQL/Kafka tests.
# Only generated protobuf output and build scripts are excluded by the tool's
# normal defaults. No handwritten business or adapter source is excluded.
cargo llvm-cov clean --workspace
cargo llvm-cov --workspace --all-targets --frozen --no-report -- --include-ignored --test-threads="${TEST_THREADS:-1}"
cargo llvm-cov report --frozen --cobertura --output-path coverage/cobertura.xml
cargo llvm-cov report --frozen --json --summary-only --output-path coverage/summary.json
cargo llvm-cov report --frozen --html --output-dir coverage
python3 scripts/coverage-summary.py coverage/summary.json "${COVERAGE_MIN_LINES:-70}"
