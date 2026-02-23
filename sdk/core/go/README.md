# relaybus-core (Go)

Core envelope utilities and the publisher factory for Relaybus.

## Install

This module declares `module github.com/relaymesh/relaybus` in `go.mod`.

## Example

```go
package main

import (
	"fmt"

	"github.com/relaymesh/relaybus/sdk/core/go"
)

func main() {
	encoded, _ := core.EncodeEnvelope(core.Message{
		Topic:   "alpha",
		Payload: []byte("hello"),
	})

	decoded, _ := core.DecodeEnvelope(encoded)
	fmt.Println(decoded.Topic, string(decoded.Payload))
}
```
