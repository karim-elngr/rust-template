#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
python3 scripts/check-image-refs.py
test "$(rustc --version | cut -d' ' -f2)" = "1.99.1"
test "$(cargo llvm-cov --version | cut -d' ' -f2)" = "0.9.1"
test "$(cargo deny --version | cut -d' ' -f2)" = "0.20.2"
mkdir -p "$CARGO_HOME"
case ${DEPENDENCY_MODE:-mirror} in
  mirror|vendor) ;;
  *) echo 'DEPENDENCY_MODE must be mirror or vendor.' >&2; exit 1 ;;
esac
if [[ ${DEPENDENCY_MODE:-mirror} == vendor ]]; then
  test -d vendor || { echo 'Provide the approved vendor artifact before CI starts.' >&2; exit 1; }
  python3 - <<'PY'
import json
import os
from pathlib import Path
path = Path(os.environ["CARGO_HOME"]) / "config.toml"
path.write_text('[source.crates-io]\nreplace-with = "vendored-sources"\n'
                '[source.vendored-sources]\ndirectory = ' + json.dumps(str(Path("vendor").resolve())) +
                '\n[net]\noffline = true\n')
PY
else
  : "${CARGO_SOURCE_CONFIG:?Set a GitLab file variable containing the approved Cargo mirror configuration}"
  test -f "$CARGO_SOURCE_CONFIG"
  cp "$CARGO_SOURCE_CONFIG" "$CARGO_HOME/config.toml"
fi
chmod 600 "$CARGO_HOME/config.toml"
# Credential-free config is deliberately outside the cached registry/git paths.
python3 - <<'PY'
import os
import tomllib
from pathlib import Path
config = tomllib.loads((Path(os.environ["CARGO_HOME"]) / "config.toml").read_text())
if not config.get("source", {}).get("crates-io", {}).get("replace-with"):
    raise SystemExit("CI requires explicit crates.io source replacement; see README.md#dependency-supply-chain")
PY
cargo fetch --locked
git diff --exit-code -- Cargo.lock
