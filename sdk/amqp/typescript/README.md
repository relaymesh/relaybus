# relaybus-amqp (TypeScript)

AMQP publisher and subscriber utilities for Relaybus.

## Install

```
npm install @relaymesh/relaybus-amqp
```

## Example

Publisher will assert the exchange/queue on first publish (defaults: `exchangeType: "topic"`, `queue: topic`).

```ts
import { AmqpPublisher, AmqpSubscriber } from "@relaymesh/relaybus-amqp";

async function main() {
  const exchange = "relaybus.events";
  const queue = "relaybus.demo";

  const publisher = await AmqpPublisher.connect({
    url: "amqp://guest:guest@localhost:5672/",
    exchange,
    exchangeType: "topic",
    queue
  });
  await publisher.publish("relaybus.demo", {
    topic: "relaybus.demo",
    payload: Buffer.from("hello")
  });
  await publisher.close();

  const subscriber = await AmqpSubscriber.connect({
    url: "amqp://guest:guest@localhost:5672/",
    exchange,
    queue,
    onMessage: (msg) => console.log(msg.topic, msg.payload.toString())
  });
  await subscriber.start("relaybus.demo");
  await subscriber.close();
}

main().catch(console.error);
```
