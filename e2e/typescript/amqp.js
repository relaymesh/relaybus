const { AmqpPublisher, AmqpSubscriber } = require("../../sdk/amqp/typescript/dist/index.js");

async function main() {
  const mode = process.argv[2] || "sub";
  const url = process.env.AMQP_URL || "amqp://guest:guest@localhost:5672/";
  const topic = process.env.TOPIC || "relaybus.alpha";
  const exchange = process.env.AMQP_EXCHANGE || "relaybus.events";
  const exchangeType = process.env.AMQP_EXCHANGE_TYPE || "topic";
  const queue = process.env.AMQP_QUEUE || `${topic}.queue`;

  if (mode === "sub") {
    const subscriber = await AmqpSubscriber.connect({
      url,
      exchange,
      exchangeType,
      queue,
      onMessage: (msg) => {
        console.log(`received id=${msg.id} topic=${msg.topic} payload=${msg.payload.toString()}`);
      }
    });

    await withTimeout(subscriber.start(topic), 30000);
    await subscriber.close();
    return;
  }

  const publisher = await AmqpPublisher.connect({ url, exchange, exchangeType, queue });

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
