# relaybus-kafka (TypeScript)

Kafka publisher and subscriber utilities for Relaybus.

## Install

```
npm install @relaymesh/relaybus-kafka
```

## Example

Publisher will attempt to create the topic on first publish (requires broker permissions).

```ts
import { KafkaPublisher, KafkaSubscriber } from "@relaymesh/relaybus-kafka";

async function main() {
  const publisher = await KafkaPublisher.connect({
    brokers: ["localhost:29092"]
  });
  await publisher.publish("relaybus.demo", {
    topic: "relaybus.demo",
    payload: Buffer.from("hello")
  });
  await publisher.close();

  const subscriber = await KafkaSubscriber.connect({
    brokers: ["localhost:29092"],
    groupId: "relaybus",
    onMessage: (msg) => console.log(msg.topic, msg.payload.toString())
  });
  await subscriber.start("relaybus.demo");
}

main().catch(console.error);
```
