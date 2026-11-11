# Working in this repository

Read the relevant README section; it is the architecture and operations reference.

## Checks

- `cargo fmt --all`; `make lint`; `make test`; `make build`.
- Database/broker behavior: `make test-integration` (Docker required).
- Before delivery: `make check`; `make coverage` with Docker and approved tools.
- Report checks actually executed, failures and unavailable infrastructure accurately.

## Boundaries and invariants

- `cart`, `orders`, `payments`, `checkout` own their domain state machines; keep
  framework/database types out. `shared-kernel` stays small and domain-neutral.
- `commerce` composes use cases and explicit SQLx units of work. Separate commands,
  queries, projections and per-domain persistence. Transport adapters stay in
  `services`; binaries remain thin. Do not let handlers own business transitions.
- Payment is its own aggregate. Persist provider operations before external I/O;
  replay the same provider key after uncertain outcomes. A timeout is not a decline.
  Never hold a database transaction across a provider or broker call.
- Saga state/effects/outbox and inbox receipts commit atomically. Preserve capture,
  confirmation and void/refund compensation rules, including cancellation races.
- Preserve optimistic cart versions, request conflict detection, outbox lease fencing
  and projection version guards. Kafka offsets follow durable effects; never commit
  past a failed record. At-least-once delivery must remain safe on replay.
- Money uses checked integer minor units; catalog prices are server-owned. Keep
  tokens, credentials and PII out of logs/read models and IDs out of metric labels.
- Use compatible forward migrations and versioned events/protobuf. Never edit an
  applied migration, reuse protobuf field numbers or silently change replay identity.
- Retain bounded shutdown, readiness, telemetry flushing and trace propagation.
- Use approved sources, lockfiles and pinned tools. No executable download scripts,
  floating Git dependencies or credentials in source/images/caches.
- Add tests for changed failure/concurrency behavior; do not weaken tests, coverage
  or dependency policies to obtain a green build. Company identity and real payment
  adapters require their own integration and authorization tests before release.
