#!/usr/bin/env python3
"""Report the same line metric used by the CI gate and GitLab widget."""
import json
import math
import sys
from pathlib import Path


def main() -> int:
    if len(sys.argv) != 3:
        raise ValueError("usage: coverage-summary.py report.json minimum-percent")
    minimum = float(sys.argv[2])
    if not math.isfinite(minimum) or not 0 <= minimum <= 100:
        raise ValueError("coverage minimum must be between 0 and 100")
    report = json.loads(Path(sys.argv[1]).read_text())
    totals = [entry["totals"]["lines"] for entry in report["data"]]
    count = sum(entry["count"] for entry in totals)
    covered = sum(entry["covered"] for entry in totals)
    if count <= 0 or not 0 <= covered <= count:
        raise ValueError("coverage report has no valid instrumented lines")
    percentage = 100.0 * covered / count
    print(f"LINE_COVERAGE={percentage:.2f}%", flush=True)
    print(f"Covered {covered}/{count} lines; required minimum {minimum:g}%")
    return 0 if percentage >= minimum else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (KeyError, TypeError, ValueError, OSError) as error:
        print(f"Invalid coverage report: {error}", file=sys.stderr)
        raise SystemExit(2)
