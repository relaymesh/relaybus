# relaybus-kafka (Go)

Kafka publisher and subscriber adapters for Relaybus.

## Install

This module declares `module github.com/relaymesh/relaybus` in `go.mod`.

## Example

```go
package main

import (
	"context"
	"log"
	"time"

	kafkaadapter "github.com/relaymesh/relaybus/sdk/kafka/go"
	"github.com/relaymesh/relaybus/sdk/core/go/message"
)

func main() {
	pub, err := kafkaadapter.NewPublisher(kafkaadapter.Config{
		Broker: "localhost:29092",
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
