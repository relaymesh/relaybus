# relaybus-http (TypeScript)

HTTP publisher and subscriber utilities for Relaybus.

## Install

```
npm install @relaymesh/relaybus-http
```

## Example

```ts
import { HttpPublisher, HttpSubscriber } from "@relaymesh/relaybus-http";

async function main() {
  const subscriber = new HttpSubscriber({
    onMessage: (msg) => console.log(msg.topic, msg.payload.toString())
  });
  await subscriber.listen({ port: 8088 });

  const publisher = HttpPublisher.connect({
    endpoint: "http://localhost:8088/{topic}"
  });
  await publisher.publish("relaybus.demo", {
    topic: "relaybus.demo",
    payload: Buffer.from("hello")
  });
}

main().catch(console.error);
```
