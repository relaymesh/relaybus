const { NatsPublisher, NatsSubscriber } = require("../../sdk/nats/typescript/dist/index.js");

async function main() {
  const mode = process.argv[2] || "sub";
  const url = process.env.NATS_URL || "nats://localhost:4222";
  const prefix = process.env.NATS_PREFIX || "relaybus";
  const topic = process.env.TOPIC || "alpha";

  if (mode === "sub") {
    const subscriber = await NatsSubscriber.connect({
      url,
      onMessage: (msg) => {
        console.log(`received id=${msg.id} topic=${msg.topic} payload=${msg.payload.toString()}`);
      },
      subjectPrefix: prefix,
      maxMessages: 1
    });

    await withTimeout(subscriber.start(topic), 30000);
    await subscriber.close();
    return;
  }

  const publisher = await NatsPublisher.connect({ url, subjectPrefix: prefix });

  await publisher.publish(topic, {
    topic,
    payload: Buffer.from("hello from typescript"),
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
