#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
builder=${BUILDER_IMAGE:-rust-template-tools:local}
runtime=${RUNTIME_IMAGE:-rust-template-runtime:local}
prefix=${IMAGE_PREFIX:-rust-commerce}
tag=${IMAGE_TAG:-local}
args=(--build-arg "BUILDER_IMAGE=$builder" --build-arg "RUNTIME_IMAGE=$runtime"
      --build-arg "CARGO_BUILD_JOBS=${CARGO_BUILD_JOBS:-2}")
case ${DEPENDENCY_MODE:-mirror} in
  mirror|vendor) ;;
  *) echo 'DEPENDENCY_MODE must be mirror or vendor.' >&2; exit 1 ;;
esac
if [[ ${DEPENDENCY_MODE:-mirror} == vendor ]]; then
  args+=(--build-arg DEPENDENCY_STAGE=dependencies-vendored)
elif [[ -n ${CARGO_SOURCE_CONFIG:-} ]]; then
  args+=(--secret "id=cargo-config,src=$CARGO_SOURCE_CONFIG")
fi
if [[ -n ${CARGO_REGISTRIES_COMPANY_TOKEN:-} ]]; then
  args+=(--secret id=cargo-token,env=CARGO_REGISTRIES_COMPANY_TOKEN)
fi
export DOCKER_BUILDKIT=1
for target in http-api grpc-api kafka-outbox-poller kafka-consumer cronjob migrate; do
  docker build "${args[@]}" --target "$target" --tag "$prefix/$target:$tag" .
done
