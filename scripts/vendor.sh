#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
args=()
if [[ -n ${CARGO_SOURCE_CONFIG:-} ]]; then
  args+=(--config "$CARGO_SOURCE_CONFIG")
fi
mkdir -p artifacts
# Respect source replacement: default cargo vendor would bypass the mirror.
cargo "${args[@]}" vendor --locked --respect-source-config --versioned-dirs vendor \
  > artifacts/vendor-config.toml
cp Cargo.lock artifacts/Cargo.lock
tar -czf artifacts/vendor.tar.gz vendor
python3 - <<'PY'
import hashlib
from pathlib import Path
lines = []
for name in ("vendor.tar.gz", "Cargo.lock"):
    path = Path("artifacts") / name
    digest = hashlib.sha256()
    with path.open("rb") as archive:
        for chunk in iter(lambda: archive.read(1024 * 1024), b""):
            digest.update(chunk)
    lines.append(f"{digest.hexdigest()}  {name}")
manifest = "\n".join(lines) + "\n"
(Path("artifacts") / "SHA256SUMS").write_text(manifest)
print(manifest, end="")
PY
# Authenticate the artifact/digests using your approved release channel. A hash
# delivered beside an untrusted archive alone does not prove its provenance.
