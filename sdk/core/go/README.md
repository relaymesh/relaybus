# relaybus-core (Go)

Core envelope utilities and the publisher factory for Relaybus.

## Install

This module currently declares `module relaybus` in `go.mod`. If you consume it outside this repo, add a replace in your own `go.mod` or adjust the module path in your fork.

## Example

```go
package main

import (
	"fmt"

	"relaybus/sdk/core/go"
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
