const { KafkaPublisher, KafkaSubscriber } = require("../../sdk/kafka/typescript/dist/index.js");

async function main() {
  const mode = process.argv[2] || "sub";
  const broker = process.env.KAFKA_BROKER || "localhost:29092";
  const topic = process.env.TOPIC || "relaybus.alpha";

  if (mode === "sub") {
    const subscriber = await KafkaSubscriber.connect({
      brokers: [broker],
      onMessage: (msg) => {
        console.log(`received id=${msg.id} topic=${msg.topic} payload=${msg.payload.toString()}`);
      },
      groupId: "relaybus-e2e",
      clientId: "relaybus-e2e",
      maxMessages: 1
    });

    await withTimeout(subscriber.start(topic), 30000);
    await subscriber.close();
    return;
  }

  const publisher = await KafkaPublisher.connect({
    brokers: [broker],
    clientId: "relaybus-e2e",
    topicPrefix: ""
  });

  await publisher.publish(topic, {
    topic,
    payload: Buffer.from("hello from typescript"),
    id: `ts-${Date.now()}`,
    meta: { lang: "ts" }
  });

  await publisher.close();
  console.log("published");
}

function withTimeout(promise, ms) {
  return Promise.race([
    promise,
    new Promise((_, reject) =>
      setTimeout(() => reject(new Error("timeout waiting for message")), ms)
    )
  ]);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
