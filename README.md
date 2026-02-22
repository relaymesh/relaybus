# relaybus

Relaybus is an open-source monorepo for SDKs that publish and consume opaque message envelopes across brokers. This repo is library-first: no CLI and no examples in this iteration.

## Scope

- Go: publish + subscribe SDK with adapters for HTTP, AMQP, NATS, Kafka, and memory (publish-only for memory).
- TypeScript: publish + subscribe utilities for HTTP, AMQP, NATS, and Kafka.
- Python: publish + subscribe utilities for HTTP, AMQP, NATS, and Kafka.
- Envelope v1 contract and corpus samples under `spec/`.

## Repository layout

```
/sdk
  /core
    /go
    /typescript
    /python
  /amqp
    /go
    /typescript
    /python
  /nats
    /go
    /typescript
    /python
  /kafka
    /go
    /typescript
    /python
  /http
    /go
    /typescript
    /python
/spec
  envelope_v1.jsonschema
  corpus/
    samples/
    expected/
```

## Package names

TypeScript (npm):
- `@relaybus/relaybus-core`
- `@relaybus/relaybus-amqp`
- `@relaybus/relaybus-nats`
- `@relaybus/relaybus-kafka`
- `@relaybus/relaybus-http`

Python (PyPI):
- `relaybus-core`
- `relaybus-amqp`
- `relaybus-nats`
- `relaybus-kafka`
- `relaybus-http`

## Testing

- Go: `go test ./...`
- TypeScript: `npm test`
- Python: `pytest`

## End-to-end

E2E runs against local brokers from `docker-compose.yaml`.

```
docker compose up -d
make e2e
```

To stop services:

```
docker compose down -v
```
