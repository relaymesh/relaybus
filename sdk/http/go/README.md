# relaybus-http (Go)

HTTP publisher and subscriber adapters for Relaybus.

## Install

This module declares `module github.com/relaymesh/relaybus` in `go.mod`.

## Example

```go
package main

import (
	"context"
	"log"
	"time"

	httpadapter "github.com/relaymesh/relaybus/sdk/http/go"
	"github.com/relaymesh/relaybus/sdk/core/go/message"
)

func main() {
	pub, err := httpadapter.NewPublisher(httpadapter.Config{
		Endpoint: "http://localhost:8088/{topic}",
	})
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_ = pub.Publish(ctx, "relaybus.demo", message.Message{
		Topic:   "relaybus.demo",
		Payload: []byte("hello"),
	})
}
```
