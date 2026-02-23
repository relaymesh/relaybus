# relaybus-amqp (TypeScript)

AMQP publisher and subscriber utilities for Relaybus.

## Install

```
npm install @relaymesh/relaybus-amqp
```

## Example

```ts
import { AmqpPublisher, AmqpSubscriber } from "@relaymesh/relaybus-amqp";

async function main() {
  const publisher = await AmqpPublisher.connect({
    url: "amqp://guest:guest@localhost:5672/"
  });
  await publisher.publish("relaybus.demo", {
    topic: "relaybus.demo",
    payload: Buffer.from("hello")
  });
  await publisher.close();

  const subscriber = await AmqpSubscriber.connect({
    url: "amqp://guest:guest@localhost:5672/",
    onMessage: (msg) => console.log(msg.topic, msg.payload.toString())
  });
  await subscriber.start("relaybus.demo");
  await subscriber.close();
}

main().catch(console.error);
```
