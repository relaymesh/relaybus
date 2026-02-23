# relaybus-nats (Go)

NATS publisher and subscriber adapters for Relaybus.

## Install

This module currently declares `module relaybus` in `go.mod`. If you consume it outside this repo, add a replace in your own `go.mod` or adjust the module path in your fork.

## Example

```go
package main

import (
	"context"
	"log"
	"time"

	natsadapter "relaybus/sdk/nats/go"
	"relaybus/sdk/core/go/message"
)

func main() {
	pub, err := natsadapter.NewPublisher(natsadapter.Config{
		URL:           "nats://localhost:4222",
		SubjectPrefix: "relaybus",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer pub.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_ = pub.Publish(ctx, "alpha", message.Message{
		Topic:   "alpha",
		Payload: []byte("hello"),
	})
}
```
