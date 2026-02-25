# relaybus-amqp (Go)

AMQP publisher and subscriber adapters for Relaybus.

## Install

This module declares `module github.com/relaymesh/relaybus` in `go.mod`.

## Example

```go
package main

import (
	"context"
	"log"
	"time"

	amqpadapter "github.com/relaymesh/relaybus/sdk/amqp/go"
	"github.com/relaymesh/relaybus/sdk/core/go/message"
)

func main() {
	pub, err := amqpadapter.NewPublisher(amqpadapter.Config{
		URL:          "amqp://guest:guest@localhost:5672/",
		Exchange:     "relaybus.events",
		ExchangeType: "topic",
		Queue:        "relaybus.demo",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer pub.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_ = pub.Publish(ctx, "relaybus.demo", message.Message{
		Topic:   "relaybus.demo",
		Payload: []byte("hello"),
	})
}
```
