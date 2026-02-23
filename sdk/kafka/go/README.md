# relaybus-kafka (Go)

Kafka publisher and subscriber adapters for Relaybus.

## Install

This module currently declares `module relaybus` in `go.mod`. If you consume it outside this repo, add a replace in your own `go.mod` or adjust the module path in your fork.

## Example

```go
package main

import (
	"context"
	"log"
	"time"

	kafkaadapter "relaybus/sdk/kafka/go"
	"relaybus/sdk/core/go/message"
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
