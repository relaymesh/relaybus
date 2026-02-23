# relaybus

Relaybus is a broker-agnostic messaging SDK built around a stable, testable envelope format. It keeps payloads opaque, focuses on predictable behavior, and makes it easy to mix languages within the same event stream.

## Design principles

- Opaque payloads: the SDK never interprets your bytes, so you own serialization.
- Stable envelope: versioned JSON with a required base64 payload field.
- Deterministic testing: shared corpus samples validate every SDK the same way.
- Library-first: no CLI, no examples folder, and no broker dependencies in unit tests.

## Supported adapters (iteration 1)

| Adapter | Go | TypeScript | Python |
| --- | --- | --- | --- |
| AMQP | publish + subscribe | publish + subscribe | publish + subscribe |
| NATS | publish + subscribe | publish + subscribe | publish + subscribe |
| Kafka | publish + subscribe | publish + subscribe | publish + subscribe |
| HTTP | publish + subscribe | publish + subscribe | publish + subscribe |
| Memory | publish only | - | - |

## Envelope v1 (summary)

Every message is encoded as JSON with a base64 payload:

- Required fields: `v`, `id`, `topic`, `ts`, `content_type`, `payload_b64`, `meta`
- `content_type` defaults to `application/octet-stream`
- `meta` is always present (empty object allowed)

The canonical schema and corpus live under `spec/`.

## Cross-language example (Go publisher → TypeScript subscriber via AMQP)

Go publisher:

```go
package main

import (
	"context"
	"log"
	"time"

	amqpadapter "github.com/relaybus/relaybus/sdk/amqp/go"
	"github.com/relaybus/relaybus/sdk/core/go"
)

func main() {
	pub, err := core.NewPublisher(core.Config{
		Destination: "amqp",
		AMQP: amqpadapter.Config{
			URL:                "amqp://guest:guest@localhost:5672/",
			Exchange:           "",
			RoutingKeyTemplate: "{topic}",
		},
	})
	if err != nil {
		log.Fatalf("publisher: %v", err)
	}
	defer pub.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := pub.Publish(ctx, "relaybus.demo", core.Message{
		Topic:    "relaybus.demo",
		Payload:  []byte("hello from go"),
		Metadata: map[string]string{"lang": "go"},
	}); err != nil {
		log.Fatalf("publish: %v", err)
	}
}
```

TypeScript subscriber:

```ts
import { AmqpSubscriber } from "@relaybus/relaybus-amqp";

async function main() {
  const sub = await AmqpSubscriber.connect({
    url: "amqp://guest:guest@localhost:5672/",
    onMessage: (msg) => {
      console.log(`received id=${msg.id} topic=${msg.topic} payload=${msg.payload.toString()}`);
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

## Packages

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

## Install

npm (public registry):

```
npm install @relaybus/relaybus-core @relaybus/relaybus-amqp @relaybus/relaybus-nats @relaybus/relaybus-kafka @relaybus/relaybus-http
```

pip (PyPI):

```
pip install relaybus-core relaybus-amqp relaybus-nats relaybus-kafka relaybus-http
```

## Testing and e2e

Unit tests are language-native (`go test`, `npm test`, `pytest`). End-to-end runs use the local `docker-compose.yaml` harness; see `docs/e2e.md` for details.
