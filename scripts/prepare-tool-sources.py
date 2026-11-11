#!/usr/bin/env python3
"""Fetch exact Cargo tool sources and their bundled locked dependencies.

Run in the approved networked intake stage. Cargo metadata/fetch do not compile
the packages or execute their build scripts. Installation happens separately,
without network or registry credentials. No executable release assets are used.
"""
import argparse
import json
import shutil
import subprocess
import tempfile
from pathlib import Path


TOOLS = {"cargo-llvm-cov": "0.9.1", "cargo-deny": "0.20.2"}
CRATES_IO_SOURCE = "registry+https://github.com/rust-lang/crates.io-index"


def prepare(config: Path, output: Path, toolchain: str, offline: bool) -> None:
    config = config.resolve(strict=True)
    output = output.resolve()
    if output.exists():
        raise ValueError(f"output already exists: {output}")
    cargo = ["cargo", f"+{toolchain}", "--config", str(config)]
    version = subprocess.check_output(["rustc", f"+{toolchain}", "-vV"], text=True)
    host = next(line.removeprefix("host: ") for line in version.splitlines() if line.startswith("host: "))
    offline_args = ["--offline"] if offline else []
    with tempfile.TemporaryDirectory(prefix="cargo-tool-intake-") as directory:
        seed = Path(directory)
        (seed / "src").mkdir()
        (seed / "src/lib.rs").touch()
        # This disposable graph only locates source archives; it is never built.
        # Tool compilation resolves against each published package's own lock.
        dependencies = "\n".join(f'{name} = "={version}"' for name, version in TOOLS.items())
        (seed / "Cargo.toml").write_text(
            '[package]\nname = "tool-source-intake"\nversion = "0.0.0"\n'
            'edition = "2024"\npublish = false\n[dependencies]\n' + dependencies + "\n"
        )
        metadata = json.loads(subprocess.check_output(
            cargo + ["metadata", "--format-version=1", "--filter-platform", host] + offline_args,
            cwd=seed, text=True,
        ))
        output.mkdir(parents=True)
        for name, expected_version in TOOLS.items():
            packages = [package for package in metadata["packages"]
                        if package["name"] == name and package["version"] == expected_version
                        and package["source"] == CRATES_IO_SOURCE]
            if len(packages) != 1:
                raise ValueError(f"expected one crates.io source package for {name} {expected_version}")
            source = Path(packages[0]["manifest_path"]).parent
            lock = (source / "Cargo.lock").read_bytes()
            destination = output / name
            shutil.copytree(source, destination)
            # Keep cwd outside the fetched source: source-local Cargo config is
            # not used during the credential-bearing dependency-intake step.
            subprocess.run(
                cargo + ["fetch", "--manifest-path", str(destination / "Cargo.toml"),
                         "--locked", "--target", host] + offline_args,
                cwd=seed, check=True,
            )
            if (destination / "Cargo.lock").read_bytes() != lock:
                raise ValueError(f"bundled lockfile changed for {name}")
            print(f"Prepared {name} {expected_version} with its unchanged bundled lockfile", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cargo-config", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--toolchain", default="1.99.1")
    parser.add_argument("--offline", action="store_true", help="validate using already cached sources")
    args = parser.parse_args()
    prepare(args.cargo_config, args.output, args.toolchain, args.offline)


if __name__ == "__main__":
    main()
