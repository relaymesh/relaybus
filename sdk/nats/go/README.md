# relaybus-nats (Go)

NATS publisher and subscriber adapters for Relaybus.

## Install

This module declares `module github.com/relaymesh/relaybus` in `go.mod`.

## Example

```go
package main

import (
	"context"
	"log"
	"time"

	natsadapter "github.com/relaymesh/relaybus/sdk/nats/go"
	"github.com/relaymesh/relaybus/sdk/core/go/message"
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
