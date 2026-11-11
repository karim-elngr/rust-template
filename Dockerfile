# Use the reviewed Dockerfile frontend bundled with the company BuildKit daemon.
ARG BUILDER_IMAGE=rust-template-tools:local
ARG RUNTIME_IMAGE=rust-template-runtime:local
ARG DEPENDENCY_STAGE=dependencies
FROM ${BUILDER_IMAGE} AS dependencies
WORKDIR /workspace
# Preserve installed tooling on PATH, while keeping fetched crates in a layer.
ENV CARGO_HOME=/opt/cargo RUSTUP_AUTO_INSTALL=0
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY .cargo/config.toml .cargo/config.toml
COPY crates/commerce/Cargo.toml crates/commerce/Cargo.toml
COPY crates/contracts/Cargo.toml crates/contracts/Cargo.toml
COPY crates/platform/Cargo.toml crates/platform/Cargo.toml
COPY crates/services/Cargo.toml crates/services/Cargo.toml
COPY crates/shared-kernel/Cargo.toml crates/shared-kernel/Cargo.toml
COPY crates/cart/Cargo.toml crates/cart/Cargo.toml
COPY crates/orders/Cargo.toml crates/orders/Cargo.toml
COPY crates/payments/Cargo.toml crates/payments/Cargo.toml
COPY crates/checkout/Cargo.toml crates/checkout/Cargo.toml
# Minimal targets allow metadata/fetch without tying this layer to source edits.
RUN for crate in shared-kernel cart orders payments checkout commerce contracts platform services; do \
      mkdir -p "crates/$crate/src"; touch "crates/$crate/src/lib.rs"; \
    done
RUN --mount=type=secret,id=cargo-config \
    --mount=type=secret,id=cargo-token \
    set -eu; \
    config=.cargo/config.toml; \
    if [ -f /run/secrets/cargo-config ]; then config=/run/secrets/cargo-config; fi; \
    if [ -f /run/secrets/cargo-token ]; then export CARGO_REGISTRIES_COMPANY_TOKEN="$(cat /run/secrets/cargo-token)"; fi; \
    cargo --config "$config" fetch --locked

FROM ${BUILDER_IMAGE} AS dependencies-vendored
WORKDIR /workspace
ENV CARGO_HOME=/opt/cargo RUSTUP_AUTO_INSTALL=0
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY crates crates
COPY vendor vendor
RUN mkdir -p .cargo && printf '%s\n' \
      '[source.crates-io]' 'replace-with = "vendored-sources"' \
      '[source.vendored-sources]' 'directory = "/workspace/vendor"' \
      '[net]' 'offline = true' > .cargo/config.toml

FROM ${DEPENDENCY_STAGE} AS builder
ARG TARGETARCH
ARG CARGO_BUILD_JOBS=2
COPY crates crates
COPY migrations migrations
# Source archives are in the preceding image layer, not a disposable cache
# mount. A cached fetch layer therefore still works after cache-mount eviction.
# The target cache is optional: a cold build compiles everything correctly.
RUN --network=none \
    --mount=type=secret,id=cargo-config \
    --mount=type=cache,id=commerce-rust-1.99.1-${TARGETARCH},target=/workspace/target,sharing=locked \
    set -eu; \
    config=.cargo/config.toml; \
    if [ -f /run/secrets/cargo-config ]; then config=/run/secrets/cargo-config; fi; \
    cargo --config "$config" build --workspace --bins --release --frozen --jobs "$CARGO_BUILD_JOBS"; \
    mkdir -p /out; \
    for bin in http-api grpc-api kafka-outbox-poller kafka-consumer cronjob migrate; do \
      cp "target/release/$bin" "/out/$bin"; \
    done

FROM ${RUNTIME_IMAGE} AS runtime
WORKDIR /app
USER 65532:65532

FROM runtime AS http-api
COPY --from=builder --chown=65532:65532 /out/http-api /app/http-api
EXPOSE 8080 9000
ENTRYPOINT ["/app/http-api"]

FROM runtime AS grpc-api
COPY --from=builder --chown=65532:65532 /out/grpc-api /app/grpc-api
EXPOSE 50051 9000
ENTRYPOINT ["/app/grpc-api"]

FROM runtime AS kafka-outbox-poller
COPY --from=builder --chown=65532:65532 /out/kafka-outbox-poller /app/kafka-outbox-poller
EXPOSE 9000
ENTRYPOINT ["/app/kafka-outbox-poller"]

FROM runtime AS kafka-consumer
COPY --from=builder --chown=65532:65532 /out/kafka-consumer /app/kafka-consumer
EXPOSE 9000
ENTRYPOINT ["/app/kafka-consumer"]

FROM runtime AS cronjob
COPY --from=builder --chown=65532:65532 /out/cronjob /app/cronjob
ENTRYPOINT ["/app/cronjob"]

FROM runtime AS migrate
COPY --from=builder --chown=65532:65532 /out/migrate /app/migrate
ENTRYPOINT ["/app/migrate"]
