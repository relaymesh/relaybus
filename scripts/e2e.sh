#!/usr/bin/env bash
set -euo pipefail

PYTHON_BIN="${PYTHON_BIN:-python3}"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHONPATH_ADD="${ROOT_DIR}/sdk/core/python:${ROOT_DIR}/sdk/amqp/python:${ROOT_DIR}/sdk/http/python:${ROOT_DIR}/sdk/nats/python:${ROOT_DIR}/sdk/kafka/python"
export PYTHONPATH="${PYTHONPATH_ADD}${PYTHONPATH:+:${PYTHONPATH}}"

run_pair() {
  local name="$1"
  local sub_cmd="$2"
  local pub_cmd="$3"

  echo "==> ${name}"

  bash -c "$sub_cmd" &
  local sub_pid=$!

  sleep 2

  set +e
  bash -c "$pub_cmd"
  local pub_status=$?

  if [ $pub_status -ne 0 ]; then
    kill "$sub_pid" 2>/dev/null || true
    wait "$sub_pid" 2>/dev/null || true
    echo "FAILED ${name} (publisher exit ${pub_status})"
    exit $pub_status
  fi

  wait "$sub_pid"
  local sub_status=$?
  set -e

  if [ $sub_status -ne 0 ]; then
    echo "FAILED ${name} (subscriber exit ${sub_status})"
    exit $sub_status
  fi
}

run_pair "go-amqp" \
  "go run ./e2e/go/amqp -mode=sub -topic relaybus.e2e.go.amqp" \
  "go run ./e2e/go/amqp -mode=pub -topic relaybus.e2e.go.amqp"

run_pair "go-nats" \
  "go run ./e2e/go/nats -mode=sub -topic e2e.go.nats" \
  "go run ./e2e/go/nats -mode=pub -topic e2e.go.nats"

run_pair "go-kafka" \
  "go run ./e2e/go/kafka -mode=sub -topic relaybus.e2e.go.kafka" \
  "go run ./e2e/go/kafka -mode=pub -topic relaybus.e2e.go.kafka"

run_pair "go-http" \
  "go run ./e2e/go/http -mode=sub -addr :8088" \
  "go run ./e2e/go/http -mode=pub -endpoint http://localhost:8088/{topic} -topic relaybus.e2e.go.http"

run_pair "ts-amqp" \
  "TOPIC=relaybus.e2e.ts.amqp node e2e/typescript/amqp.js sub" \
  "TOPIC=relaybus.e2e.ts.amqp node e2e/typescript/amqp.js pub"

run_pair "ts-nats" \
  "TOPIC=e2e.ts.nats NATS_PREFIX=relaybus node e2e/typescript/nats.js sub" \
  "TOPIC=e2e.ts.nats NATS_PREFIX=relaybus node e2e/typescript/nats.js pub"

run_pair "ts-kafka" \
  "TOPIC=relaybus.e2e.ts.kafka node e2e/typescript/kafka.js sub" \
  "TOPIC=relaybus.e2e.ts.kafka node e2e/typescript/kafka.js pub"

run_pair "ts-http" \
  "TOPIC=relaybus.e2e.ts.http HTTP_PORT=8089 node e2e/typescript/http.js sub" \
  "TOPIC=relaybus.e2e.ts.http HTTP_ENDPOINT=http://localhost:8089/{topic} node e2e/typescript/http.js pub"

run_pair "py-amqp" \
  "TOPIC=relaybus.e2e.py.amqp ${PYTHON_BIN} e2e/python/amqp.py sub" \
  "TOPIC=relaybus.e2e.py.amqp ${PYTHON_BIN} e2e/python/amqp.py pub"

run_pair "py-nats" \
  "TOPIC=e2e.py.nats NATS_PREFIX=relaybus ${PYTHON_BIN} e2e/python/nats_e2e.py sub" \
  "TOPIC=e2e.py.nats NATS_PREFIX=relaybus ${PYTHON_BIN} e2e/python/nats_e2e.py pub"

run_pair "py-kafka" \
  "TOPIC=relaybus.e2e.py.kafka ${PYTHON_BIN} e2e/python/kafka_e2e.py sub" \
  "TOPIC=relaybus.e2e.py.kafka ${PYTHON_BIN} e2e/python/kafka_e2e.py pub"

run_pair "py-http" \
  "TOPIC=relaybus.e2e.py.http HTTP_PORT=8090 ${PYTHON_BIN} e2e/python/http_e2e.py sub" \
  "TOPIC=relaybus.e2e.py.http HTTP_ENDPOINT=http://localhost:8090/{topic} ${PYTHON_BIN} e2e/python/http_e2e.py pub"
