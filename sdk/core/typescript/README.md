# relaybus-core (TypeScript)

Core envelope encode/decode utilities for Relaybus.

## Install

```
npm install @relaymesh/relaybus-core
```

## Example

```ts
import { decodeEnvelope, encodeEnvelope } from "@relaymesh/relaybus-core";

const encoded = encodeEnvelope({
  topic: "alpha",
  payload: Buffer.from("hello"),
  meta: { source: "ts" }
});

const decoded = decodeEnvelope(encoded);
console.log(decoded.topic, decoded.payload.toString());
```
