SHELL := /bin/bash
.DEFAULT_GOAL := help
TEST_THREADS ?= 1

.PHONY: help fmt lint test test-integration coverage check build tools up down logs cron migrate images vendor audit
help:
	@echo 'fmt lint test test-integration coverage check build tools up down logs cron migrate images vendor audit'
	@echo 'First local container run: make tools && make up. See README.md for prerequisites.'
fmt:
	cargo fmt --all -- --check
lint:
	cargo clippy --workspace --all-targets --locked -- -D warnings
test:
	cargo test --workspace --locked
test-integration:
	cargo test --workspace --locked -- --ignored --test-threads=$(TEST_THREADS)
coverage:
	cargo fetch --locked
	bash scripts/coverage.sh
check: fmt lint test
build:
	cargo build --workspace --bins --locked
tools:
	DOCKER_BUILDKIT=1 docker build -f Dockerfile.tools --target tools -t rust-template-tools:local .
	DOCKER_BUILDKIT=1 docker build -f Dockerfile.tools --target runtime -t rust-template-runtime:local .
up:
	docker compose up --build -d
down:
	docker compose down
logs:
	docker compose logs -f --tail=100
cron:
	docker compose --profile jobs run --rm cronjob
migrate:
	docker compose run --rm migrate
images:
	bash scripts/build-images.sh
vendor:
	bash scripts/vendor.sh
audit:
	cargo deny --locked check advisories bans licenses sources
