# relaybus

Relaybus is an open-source monorepo for SDKs that publish and consume opaque message envelopes across brokers. This repo is library-first: no CLI and no examples in this iteration.

## What’s included

- Go: publish + subscribe SDK with adapters for HTTP, AMQP, NATS, Kafka, and memory (publish-only for memory).
- TypeScript: publish + subscribe utilities for HTTP, AMQP, NATS, and Kafka.
- Python: publish + subscribe utilities for HTTP, AMQP, NATS, and Kafka.
- A stable envelope contract under `spec/` plus corpus samples used by all tests.

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
