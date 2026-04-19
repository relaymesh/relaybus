# relaybus

Relaybus is a broker-agnostic messaging SDK with a stable envelope contract across Go, TypeScript, and Python.

It is designed for teams that want transport flexibility (AMQP/NATS/Kafka/HTTP) without changing message semantics between languages.

## Why relaybus

- Opaque payloads: relaybus never interprets your bytes; you control serialization.
- Stable envelope: all transports use the same versioned JSON envelope.
- Cross-language parity: shared corpus tests validate behavior across SDKs.
- Library-first: small transport packages, no required runtime service from relaybus itself.

## Project status

- Current schema: `v1`
- Current transports: AMQP, NATS, Kafka, HTTP across Go/TypeScript/Python
- Go-only extra: in-memory publisher adapter (`memory` destination)

## Supported adapters

| Adapter | Go | TypeScript | Python |
| --- | --- | --- | --- |
| AMQP | publish + subscribe | publish + subscribe | publish + subscribe |
| NATS | publish + subscribe | publish + subscribe | publish + subscribe |
| Kafka | publish + subscribe | publish + subscribe | publish + subscribe |
| HTTP | publish + subscribe | publish + subscribe | publish + subscribe |
| Memory | publish only | - | - |

## Envelope contract (v1)

Every message is encoded as JSON with a base64 payload field.

Required fields:
- `v`
- `id`
- `topic`
- `ts`
- `content_type`
- `payload_b64`
- `meta`

Defaults:
- `content_type` defaults to `application/octet-stream`
- `meta` is always present (empty object allowed)

Source of truth:
- JSON Schema: `spec/envelope_v1.jsonschema`
- Cross-language corpus: `spec/corpus/samples`, `spec/corpus/expected`

## Install

TypeScript (npm):

```bash
npm install @relaymesh/relaybus-core @relaymesh/relaybus-amqp @relaymesh/relaybus-nats @relaymesh/relaybus-kafka @relaymesh/relaybus-http
```

Python (PyPI):

```bash
pip install relaybus-core relaybus-amqp relaybus-nats relaybus-kafka relaybus-http
```

Go:

```bash
go get github.com/relaymesh/relaybus/sdk/core/go
```

## Quick example (Go publisher -> TypeScript subscriber over AMQP)

```go
package main

import (
	"context"
	"log"
	"time"

	amqpadapter "github.com/relaymesh/relaybus/sdk/amqp/go"
	"github.com/relaymesh/relaybus/sdk/core/go"
)

func main() {
	pub, err := core.NewPublisher(core.Config{
		Destination: "amqp",
		AMQP: amqpadapter.Config{
			URL:                "amqp://guest:guest@localhost:5672/",
			RoutingKeyTemplate: "{topic}",
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer pub.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = pub.Publish(ctx, "relaybus.demo", core.Message{
		Topic:   "relaybus.demo",
		Payload: []byte("hello from go"),
	})
	if err != nil {
		log.Fatal(err)
	}
}
```

```ts
import { AmqpSubscriber } from "@relaymesh/relaybus-amqp";

async function main() {
  const sub = await AmqpSubscriber.connect({
    url: "amqp://guest:guest@localhost:5672/",
    onMessage: (msg) => {
      console.log(msg.topic, msg.payload.toString());
    }
  });

  await sub.start("relaybus.demo");
  await sub.close();
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
```

## Package docs

TypeScript:
- `sdk/core/typescript/README.md`
- `sdk/amqp/typescript/README.md`
- `sdk/nats/typescript/README.md`
- `sdk/kafka/typescript/README.md`
- `sdk/http/typescript/README.md`

Python:
- `sdk/core/python/README.md`
- `sdk/amqp/python/README.md`
- `sdk/nats/python/README.md`
- `sdk/kafka/python/README.md`
- `sdk/http/python/README.md`

Go:
- `sdk/core/go`
- `sdk/amqp/go`
- `sdk/nats/go`
- `sdk/kafka/go`
- `sdk/http/go`

## Development

This repository is a multi-language monorepo.

Top-level layout:
- `sdk/core/{go,typescript,python}`: shared envelope/core behavior
- `sdk/{amqp,http,nats,kafka}/{go,typescript,python}`: transport adapters
- `spec/`: schema + corpus fixtures used by tests
- `scripts/`: repo tooling (`sync_core.py`, `bump_versions.py`, `e2e.sh`)

Install JS workspace deps:

```bash
pnpm install
```

Run all unit tests:

```bash
make test
```

Run e2e locally:

```bash
docker compose -f docker-compose.yaml up -d
make e2e
docker compose -f docker-compose.yaml down -v
```

Sync vendored core code into adapters (required after core TS/Python changes):

```bash
python3 scripts/sync_core.py
```

## CI and releases

- CI workflow: `.github/workflows/ci.yml`
- Release workflow: `.github/workflows/release.yml` (tag push `v*`)

Version bump helper:

```bash
make bump VERSION=0.x.y
```
