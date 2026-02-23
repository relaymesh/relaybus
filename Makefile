SHELL := /bin/bash
.SHELLFLAGS := -euo pipefail -c
MAKEFLAGS += --no-builtin-rules
.DEFAULT_GOAL := help

PYTHON ?= python3
PIP ?= $(PYTHON) -m pip
PIP_E2E_REQUIREMENTS ?= e2e/python/requirements.txt

TS_PM ?= pnpm
TS_INSTALL_CMD ?= $(TS_PM) install
TS_BUILD_CMD ?= $(TS_PM) -r run build
TS_TEST_CMD ?= $(TS_PM) -r test
TS_DEPS_OK_CMD ?= node -e "require.resolve('amqplib');require.resolve('kafkajs');require.resolve('nats');"

E2E_SCRIPT ?= scripts/e2e.sh

.PHONY: help test test-go test-ts test-py e2e e2e-setup ts-install ts-build py-e2e-deps bump

help: ## Show available targets
	@awk 'BEGIN {FS = ":.*##"; printf "Targets:\\n"} /^[a-zA-Z0-9_-]+:.*##/ {printf "  %-18s %s\\n", $$1, $$2}' $(MAKEFILE_LIST)

test: test-go test-ts test-py ## Run unit tests across Go, TS, and Python

test-go: ## Run Go unit tests
	go test ./...

test-ts: ## Run TypeScript unit tests
	$(TS_TEST_CMD)

test-py: ## Run Python unit tests
	pytest

e2e: e2e-setup ## Run end-to-end tests (requires docker-compose)
	bash $(E2E_SCRIPT)

e2e-setup: ts-build py-e2e-deps ## Build TypeScript packages + install Python e2e deps

ts-install: ## Install TypeScript workspace dependencies (skips when already present)
	@if [ -d node_modules ] && $(TS_DEPS_OK_CMD) >/dev/null 2>&1; then \
		echo "node_modules present with e2e deps, skipping install"; \
	else \
		$(TS_INSTALL_CMD); \
	fi

ts-build: ts-install ## Build TypeScript workspace packages
	$(TS_BUILD_CMD)

py-e2e-deps: ## Install Python e2e dependencies
	$(PIP) install -r $(PIP_E2E_REQUIREMENTS)

bump: ## Bump TS/Python package versions (VERSION=0.x.y)
	@if [ -z "$(VERSION)" ]; then \
		echo "VERSION is required (e.g., VERSION=0.2.0)"; \
		exit 1; \
	fi
	$(PYTHON) scripts/bump_versions.py --version "$(VERSION)" $(if $(DRY_RUN),--dry-run,)
