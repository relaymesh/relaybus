# relaybus-nats (TypeScript)

NATS publisher and subscriber utilities for Relaybus.

## Install

```
npm install @relaymesh/relaybus-nats
```

## Example

```ts
import { NatsPublisher, NatsSubscriber } from "@relaymesh/relaybus-nats";

async function main() {
  const publisher = await NatsPublisher.connect({ url: "nats://localhost:4222" });
  await publisher.publish("alpha", {
    topic: "alpha",
    payload: Buffer.from("hello")
  });

  const subscriber = await NatsSubscriber.connect({
    url: "nats://localhost:4222",
    subjectPrefix: "relaybus",
    onMessage: (msg) => console.log(msg.topic, msg.payload.toString())
  });
  await subscriber.start("alpha");
  await subscriber.close();
}

main().catch(console.error);
```
