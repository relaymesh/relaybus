PYTHON ?= python3
TS_INSTALL_CMD ?= pnpm install
TS_BUILD_CMD ?= pnpm -r run build
TS_DEPS_OK_CMD ?= node -e "require.resolve('amqplib');require.resolve('kafkajs');require.resolve('nats');"

.PHONY: test test-go test-ts test-py e2e e2e-setup e2e-ts-build e2e-py-setup

test: test-go test-ts test-py

test-go:
	go test ./...

test-ts:
	npm test

test-py:
	pytest

e2e: e2e-setup
	bash scripts/e2e.sh

e2e-setup: e2e-ts-build e2e-py-setup

e2e-ts-build:
	@if [ -d node_modules ] && $(TS_DEPS_OK_CMD) >/dev/null 2>&1; then \
		echo "node_modules present with e2e deps, skipping install"; \
	else \
		$(TS_INSTALL_CMD); \
	fi
	$(TS_BUILD_CMD)

e2e-py-setup:
	$(PYTHON) -m pip install -r e2e/python/requirements.txt
